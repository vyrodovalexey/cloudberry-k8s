package builder

import (
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// pxfNetworkPolicyTCP / pxfNetworkPolicyUDP are the protocols used by the PXF
// NetworkPolicy ingress rules. They are package vars (not consts) because
// corev1.Protocol must be addressed (&proto) in the NetworkPolicyPort.
var (
	pxfNetworkPolicyTCP = corev1.ProtocolTCP
	pxfNetworkPolicyUDP = corev1.ProtocolUDP
)

const (
	// interconnectPortMin / interconnectPortMax bound the ephemeral port range
	// the Cloudberry Motion/Interconnect layer uses BETWEEN cluster pods.
	// Every QD/QE process binds a kernel-assigned non-privileged port for
	// Motion (Redistribute/Broadcast/Gather) data exchange, so the whole
	// non-privileged range must be admitted for intra-cluster traffic.
	interconnectPortMin int32 = 1025
	interconnectPortMax int32 = 65535

	// intraClusterSSHPort is the intra-cluster SSH port the Greenplum/
	// Cloudberry MPP toolchain depends on: gpbackup/gprestore dispatch
	// per-segment work (mkdir, gpbackup_s3_plugin setup/agents) from the
	// coordinator to every segment over SSH, and gpexpand/gprecoverseg/
	// gpinitstandby use the same channel. It MUST be admitted from this
	// cluster's pods, or live backups hang at the first coordinator→segment
	// ssh (observed: gpbackup stuck in "mkdir -p .../backups/<ts>" with the
	// SYN silently dropped by the SE.5 policy). Scoped to same-cluster pods
	// only — never opened wide. The ROOTLESS sshd variant (port 2022, see
	// gpexpand_builder.clusterSSHPort) needs no extra rule: 2022 already
	// falls inside the admitted interconnect ephemeral range.
	intraClusterSSHPort int32 = 22
)

// BuildPXFClusterNetworkPolicy builds the SE.5 NetworkPolicy that confines the
// PXF service port (5888) on the segment-primary AND segment-mirror pods:
// cross-pod ingress is permitted ONLY for the legitimate cluster ports
// (PostgreSQL + the postgres/node exporters) plus — scoped to pods of the SAME
// cluster — the Cloudberry interconnect ephemeral port range. The PXF port is
// deliberately OMITTED from every allowed-ingress set (the TCP interconnect
// range is split around it). Because the operator's data-loading path always
// reaches PXF over localhost inside the same pod — and intra-pod (loopback)
// traffic is never subject to a NetworkPolicy — loads keep working while no
// other pod can reach :5888. Mirror pods are covered because PXF now runs on
// mirrors too (D9), so :5888 must stay confined after a failover.
// Returns nil when PXF is not enabled (gated on pxfSidecarEnabled), so a default
// cluster produces no policy.
func (b *DefaultBuilder) BuildPXFClusterNetworkPolicy(
	cluster *cbv1alpha1.CloudberryCluster,
) *networkingv1.NetworkPolicy {
	if !pxfSidecarEnabled(cluster) {
		return nil
	}

	labels := util.CommonLabels(cluster.Name, util.ComponentSegmentPrimary)
	// Select BOTH the segment-primary and segment-mirror pods via their standard
	// component label (an In-set match) so the policy applies to every pod that
	// hosts a PXF sidecar — the primary and, since D9, the mirror as well.
	selector := metav1.LabelSelector{
		MatchLabels: map[string]string{util.LabelCluster: cluster.Name},
		MatchExpressions: []metav1.LabelSelectorRequirement{
			{
				Key:      util.LabelComponent,
				Operator: metav1.LabelSelectorOpIn,
				Values: []string{
					util.ComponentSegmentPrimary,
					util.ComponentSegmentMirror,
				},
			},
		},
	}

	return &networkingv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.PxfNetworkPolicyName(cluster.Name),
			Namespace: cluster.Namespace,
			Labels:    labels,
			OwnerReferences: []metav1.OwnerReference{
				ownerRef(cluster),
			},
		},
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: selector,
			PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress},
			// Two ingress rules (OR-ed by the NetworkPolicy semantics). The PXF
			// port (5888) is intentionally absent from BOTH: with at least one
			// Ingress rule present the NetworkPolicy denies all OTHER ingress
			// (including cross-pod :5888) by default, while same-pod localhost
			// traffic the loader uses is never policy-controlled.
			Ingress: []networkingv1.NetworkPolicyIngressRule{
				// Rule 1 — the pre-existing SE.5 allowlist: exact service ports,
				// source-unrestricted (external clients reach postgres; the
				// monitoring stack outside the cluster scrapes the exporters).
				{Ports: pxfAllowedIngressPorts(cluster)},
				// Rule 2 — the interconnect ephemeral range, admitted ONLY from
				// pods of this cluster (coordinator, standby, segment primaries
				// and mirrors all carry the cluster label) in the same
				// namespace. Never open to sources outside the cluster pods.
				interconnectIngressRule(cluster),
			},
		},
	}
}

// pxfAllowedIngressPorts returns the legitimate cross-pod ingress service ports
// for the segment-primary and segment-mirror pods under the SE.5 policy: the
// PostgreSQL/segment port and the postgres + node exporter ports. The PXF port
// (5888) is deliberately excluded.
func pxfAllowedIngressPorts(
	cluster *cbv1alpha1.CloudberryCluster,
) []networkingv1.NetworkPolicyPort {
	pgPort := resolvePort(cluster)
	ports := []int32{pgPort, pgExporterPort, nodeExporterPort}
	out := make([]networkingv1.NetworkPolicyPort, 0, len(ports))
	for _, p := range ports {
		port := intstr.FromInt32(p)
		out = append(out, networkingv1.NetworkPolicyPort{
			Protocol: &pxfNetworkPolicyTCP,
			Port:     &port,
		})
	}
	return out
}

// interconnectIngressRule returns the ingress rule admitting the Cloudberry
// interconnect ephemeral port range from this cluster's pods only.
//
// WHY the range is required: Cloudberry's Motion/Interconnect layer opens
// dynamically-bound (ephemeral) listeners on every segment/coordinator backend
// to redistribute data between segments during distributed queries (JOIN,
// GROUP BY, distributed INSERT ... Motion nodes). With only the fixed service
// ports admitted, segment-to-segment Motion setup is silently dropped and any
// distributed query hangs until gp_interconnect_setup_timeout expires.
//
// Both TCP and UDP are admitted: the default interconnect type (udpifc) runs
// over UDP, while gp_interconnect_type=tcp (and the TCP-based PROXY mode) runs
// over TCP.
//
// Scoping: the From peer restricts sources to pods carrying this cluster's
// label in the SAME namespace (no NamespaceSelector ⇒ policy namespace only),
// covering coordinator, standby, segment-primary and segment-mirror pods while
// keeping the wide port range closed to everything else.
func interconnectIngressRule(
	cluster *cbv1alpha1.CloudberryCluster,
) networkingv1.NetworkPolicyIngressRule {
	return networkingv1.NetworkPolicyIngressRule{
		From: []networkingv1.NetworkPolicyPeer{
			{
				PodSelector: &metav1.LabelSelector{
					MatchLabels: map[string]string{util.LabelCluster: cluster.Name},
				},
			},
		},
		Ports: interconnectIngressPorts(pxfPort(cluster.Spec.DataLoading.Pxf)),
	}
}

// interconnectIngressPorts returns the intra-cluster port set: the
// interconnect ephemeral ranges plus the MPP-toolchain SSH port.
//
// SE.5 invariant: the TCP range is SPLIT around the resolved PXF port so the
// full ephemeral range never re-opens cross-pod TCP :5888 (the policy's whole
// purpose). No interconnect traffic is lost by the split: PXF itself holds the
// TCP listener on that port inside every selected pod, so the kernel can never
// assign it to a Motion listener there. UDP deliberately stays a single full
// range — PXF has no UDP listener, and carving a hole the udpifc interconnect
// could randomly bind would reintroduce rare query hangs.
//
// TCP 22 (clusterSSHPort) is additionally admitted from the SAME From peer
// (this cluster's pods): gpbackup/gprestore/gpexpand dispatch per-segment
// steps from the coordinator over SSH, which the ephemeral ranges do not
// cover (22 < 1025). Without it every live backup hangs on the first
// coordinator→segment ssh.
func interconnectIngressPorts(pxfServicePort int32) []networkingv1.NetworkPolicyPort {
	tcpRanges := portRangesExcluding(interconnectPortMin, interconnectPortMax, pxfServicePort)
	out := make([]networkingv1.NetworkPolicyPort, 0, len(tcpRanges)+2)
	out = append(out, rangePolicyPort(&pxfNetworkPolicyTCP, intraClusterSSHPort, intraClusterSSHPort))
	for _, r := range tcpRanges {
		out = append(out, rangePolicyPort(&pxfNetworkPolicyTCP, r[0], r[1]))
	}
	out = append(out, rangePolicyPort(&pxfNetworkPolicyUDP, interconnectPortMin, interconnectPortMax))
	return out
}

// rangePolicyPort builds a NetworkPolicyPort covering [start, end] for the
// given protocol (pass one of the package-level protocol vars so the pointer
// stays valid). A single-port range (end == start) omits EndPort.
func rangePolicyPort(proto *corev1.Protocol, start, end int32) networkingv1.NetworkPolicyPort {
	startPort := intstr.FromInt32(start)
	port := networkingv1.NetworkPolicyPort{
		Protocol: proto,
		Port:     &startPort,
	}
	if end > start {
		port.EndPort = util.Ptr(end)
	}
	return port
}

// portRangesExcluding returns the closed ranges covering [lo, hi] with the
// excluded port carved out. When excluded lies outside [lo, hi] the single
// full range is returned unchanged.
func portRangesExcluding(lo, hi, excluded int32) [][2]int32 {
	if excluded < lo || excluded > hi {
		return [][2]int32{{lo, hi}}
	}
	ranges := make([][2]int32, 0, 2)
	if excluded > lo {
		ranges = append(ranges, [2]int32{lo, excluded - 1})
	}
	if excluded < hi {
		ranges = append(ranges, [2]int32{excluded + 1, hi})
	}
	return ranges
}
