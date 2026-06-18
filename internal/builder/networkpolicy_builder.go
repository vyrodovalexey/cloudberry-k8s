package builder

import (
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// pxfNetworkPolicyTCP is the protocol used for every PXF NetworkPolicy ingress
// rule. It is a package var (not a const) because corev1.Protocol must be
// addressed (&proto) in the NetworkPolicyPort.
var pxfNetworkPolicyTCP = corev1.ProtocolTCP

// BuildPXFClusterNetworkPolicy builds the SE.5 NetworkPolicy that confines the
// PXF service port (5888) on the segment-primary pods: cross-pod ingress is
// permitted ONLY for the legitimate cluster ports (PostgreSQL + the
// postgres/node exporters), and the PXF port is deliberately OMITTED from the
// allowed-ingress set. Because the operator's data-loading path always reaches
// PXF over localhost inside the same pod — and intra-pod (loopback) traffic is
// never subject to a NetworkPolicy — loads keep working while no other pod can
// reach :5888. Returns nil when PXF is not enabled (gated on pxfSidecarEnabled),
// so a default cluster produces no policy.
func (b *DefaultBuilder) BuildPXFClusterNetworkPolicy(
	cluster *cbv1alpha1.CloudberryCluster,
) *networkingv1.NetworkPolicy {
	if !pxfSidecarEnabled(cluster) {
		return nil
	}

	labels := util.CommonLabels(cluster.Name, util.ComponentSegmentPrimary)
	// Select the segment-primary pods via their standard component label so the
	// policy applies exactly to the pods that host the PXF sidecar.
	selector := map[string]string{
		util.LabelCluster:   cluster.Name,
		util.LabelComponent: util.ComponentSegmentPrimary,
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
			PodSelector: metav1.LabelSelector{MatchLabels: selector},
			PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress},
			// Exactly one ingress rule listing the legitimate ports. The PXF port
			// (5888) is intentionally absent: with at least one Ingress rule
			// present the NetworkPolicy denies all OTHER ingress (including
			// cross-pod :5888) by default, while same-pod localhost traffic the
			// loader uses is never policy-controlled.
			Ingress: []networkingv1.NetworkPolicyIngressRule{
				{Ports: pxfAllowedIngressPorts(cluster)},
			},
		},
	}
}

// pxfAllowedIngressPorts returns the legitimate cross-pod ingress ports for the
// segment-primary pods under the SE.5 policy: the PostgreSQL/segment port and the
// postgres + node exporter ports. The PXF port (5888) is deliberately excluded.
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
