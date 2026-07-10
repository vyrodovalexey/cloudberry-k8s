package builder

// Tests for the interconnect ingress amendment of the SE.5 PXF cluster
// NetworkPolicy (Cycle-2 production fix): the Cloudberry Motion/Interconnect
// layer uses ephemeral TCP/UDP ports between cluster pods, so the policy must
// admit the 1025–65535 range for intra-cluster traffic — WITHOUT re-opening
// the PXF port cross-pod (the TCP range is split around it) and WITHOUT
// exposing the range to sources outside this cluster's pods (From-scoped).

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// policyPortRange is a flattened (protocol, start, end) view of a
// NetworkPolicyPort for readable assertions.
type policyPortRange struct {
	proto corev1.Protocol
	start int32
	end   int32
}

// flattenPolicyPorts converts a rule's ports into policyPortRange tuples
// (an exact port becomes start == end).
func flattenPolicyPorts(t *testing.T, ports []networkingv1.NetworkPolicyPort) []policyPortRange {
	t.Helper()
	out := make([]policyPortRange, 0, len(ports))
	for _, p := range ports {
		require.NotNil(t, p.Protocol, "every policy port must pin a protocol")
		require.NotNil(t, p.Port, "every policy port must set a start port")
		r := policyPortRange{proto: *p.Protocol, start: p.Port.IntVal, end: p.Port.IntVal}
		if p.EndPort != nil {
			r.end = *p.EndPort
		}
		out = append(out, r)
	}
	return out
}

// TestPXFNetworkPolicy_ServicePortsRule_ScopingUnchanged proves rule 0 is the
// pre-existing SE.5 allowlist, byte-for-byte in behavior: the exact TCP
// service ports (postgres + both exporters), no ranges, and — critically — an
// EMPTY From (source-unrestricted), so external clients and the monitoring
// stack keep reaching those ports exactly as before the interconnect fix.
func TestPXFNetworkPolicy_ServicePortsRule_ScopingUnchanged(t *testing.T) {
	b := NewBuilder()
	np := b.BuildPXFClusterNetworkPolicy(newPXFTestCluster())
	require.NotNil(t, np)
	require.Len(t, np.Spec.Ingress, 2)

	rule := np.Spec.Ingress[0]
	assert.Empty(t, rule.From,
		"service-port rule must stay source-unrestricted (scoping unchanged)")

	got := flattenPolicyPorts(t, rule.Ports)
	want := []policyPortRange{
		{proto: corev1.ProtocolTCP, start: 5432, end: 5432},
		{proto: corev1.ProtocolTCP, start: pgExporterPort, end: pgExporterPort},
		{proto: corev1.ProtocolTCP, start: nodeExporterPort, end: nodeExporterPort},
	}
	assert.ElementsMatch(t, want, got,
		"existing exact service ports must be retained unchanged")
}

// TestPXFNetworkPolicy_InterconnectRule_RangesAndScope proves the
// intra-cluster rule admits the ephemeral range over BOTH protocols — TCP
// split around the PXF port (SE.5) and UDP as one full range (default udpifc
// interconnect) — PLUS the MPP-toolchain SSH port 22 (gpbackup/gprestore/
// gpexpand dispatch coordinator→segment work over ssh; without it live
// backups hang at the first ssh), and ONLY from pods of the same cluster: one
// From peer, a PodSelector pinned to the cluster label (matching coordinator,
// standby, segment primaries and mirrors), no NamespaceSelector (same
// namespace only) and no IPBlock.
func TestPXFNetworkPolicy_InterconnectRule_RangesAndScope(t *testing.T) {
	b := NewBuilder()
	cluster := newPXFTestCluster()
	np := b.BuildPXFClusterNetworkPolicy(cluster)
	require.NotNil(t, np)
	require.Len(t, np.Spec.Ingress, 2)

	rule := np.Spec.Ingress[1]

	// Port set: intra-cluster SSH (22), TCP split around 5888, UDP full.
	got := flattenPolicyPorts(t, rule.Ports)
	want := []policyPortRange{
		{proto: corev1.ProtocolTCP, start: intraClusterSSHPort, end: intraClusterSSHPort},
		{proto: corev1.ProtocolTCP, start: 1025, end: 5887},
		{proto: corev1.ProtocolTCP, start: 5889, end: 65535},
		{proto: corev1.ProtocolUDP, start: 1025, end: 65535},
	}
	assert.ElementsMatch(t, want, got,
		"intra-cluster rule must admit SSH(22) + TCP (split around PXF) + UDP ephemeral ranges")

	// Source scoping: intra-cluster pods only.
	require.Len(t, rule.From, 1, "interconnect rule must be From-scoped")
	peer := rule.From[0]
	require.NotNil(t, peer.PodSelector, "peer must select pods by label")
	assert.Equal(t,
		map[string]string{util.LabelCluster: cluster.Name},
		peer.PodSelector.MatchLabels,
		"peer must admit exactly this cluster's pods (all components carry the label)")
	assert.Empty(t, peer.PodSelector.MatchExpressions)
	assert.Nil(t, peer.NamespaceSelector,
		"no NamespaceSelector: the range must stay closed to other namespaces")
	assert.Nil(t, peer.IPBlock,
		"no IPBlock: the range must stay closed to non-pod sources")
}

// TestPXFNetworkPolicy_CustomPXFPort_SplitFollowsResolvedPort proves the SE.5
// TCP split tracks the RESOLVED PXF port, not the default: with pxf.port=6000
// the carved-out port moves and the default 5888 becomes plain interconnect
// range again.
func TestPXFNetworkPolicy_CustomPXFPort_SplitFollowsResolvedPort(t *testing.T) {
	b := NewBuilder()
	cluster := newPXFTestCluster()
	cluster.Spec.DataLoading.Pxf.Port = 6000

	np := b.BuildPXFClusterNetworkPolicy(cluster)
	require.NotNil(t, np)
	require.Len(t, np.Spec.Ingress, 2)

	got := flattenPolicyPorts(t, np.Spec.Ingress[1].Ports)
	want := []policyPortRange{
		{proto: corev1.ProtocolTCP, start: intraClusterSSHPort, end: intraClusterSSHPort},
		{proto: corev1.ProtocolTCP, start: 1025, end: 5999},
		{proto: corev1.ProtocolTCP, start: 6001, end: 65535},
		{proto: corev1.ProtocolUDP, start: 1025, end: 65535},
	}
	assert.ElementsMatch(t, want, got)

	assert.False(t, tcpPortCoveredByPolicy(np, 6000),
		"the resolved PXF port must not be covered by any TCP entry")
	assert.True(t, tcpPortCoveredByPolicy(np, 5888),
		"5888 is ordinary interconnect range when PXF listens elsewhere")
}

// TestPXFNetworkPolicy_DefaultPXFPortFallback proves an unset pxf.port falls
// back to the default 5888 for the split (pxfPort resolution is honored).
func TestPXFNetworkPolicy_DefaultPXFPortFallback(t *testing.T) {
	b := NewBuilder()
	cluster := newPXFTestCluster()
	cluster.Spec.DataLoading.Pxf.Port = 0 // unset → defaultPxfPort

	np := b.BuildPXFClusterNetworkPolicy(cluster)
	require.NotNil(t, np)
	assert.False(t, tcpPortCoveredByPolicy(np, defaultPxfPort),
		"the default PXF port must be carved out when pxf.port is unset")
}

// TestPortRangesExcluding covers the range-splitting helper edge cases.
func TestPortRangesExcluding(t *testing.T) {
	tests := []struct {
		name     string
		lo, hi   int32
		excluded int32
		want     [][2]int32
	}{
		{
			name: "mid split", lo: 1025, hi: 65535, excluded: 5888,
			want: [][2]int32{{1025, 5887}, {5889, 65535}},
		},
		{
			name: "excluded below range", lo: 1025, hi: 65535, excluded: 80,
			want: [][2]int32{{1025, 65535}},
		},
		{
			name: "excluded above range", lo: 1025, hi: 65534, excluded: 65535,
			want: [][2]int32{{1025, 65534}},
		},
		{
			name: "excluded at lower bound", lo: 1025, hi: 65535, excluded: 1025,
			want: [][2]int32{{1026, 65535}},
		},
		{
			name: "excluded at upper bound", lo: 1025, hi: 65535, excluded: 65535,
			want: [][2]int32{{1025, 65534}},
		},
		{
			name: "adjacent to lower bound yields single-port head", lo: 1025, hi: 65535, excluded: 1026,
			want: [][2]int32{{1025, 1025}, {1027, 65535}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, portRangesExcluding(tc.lo, tc.hi, tc.excluded))
		})
	}
}

// TestRangePolicyPort_SinglePortOmitsEndPort proves a degenerate one-port
// range renders as an exact port (no EndPort), matching the k8s API's
// preferred shape.
func TestRangePolicyPort_SinglePortOmitsEndPort(t *testing.T) {
	p := rangePolicyPort(&pxfNetworkPolicyTCP, 1025, 1025)
	require.NotNil(t, p.Port)
	assert.Equal(t, int32(1025), p.Port.IntVal)
	assert.Nil(t, p.EndPort)

	ranged := rangePolicyPort(&pxfNetworkPolicyUDP, 1025, 65535)
	require.NotNil(t, ranged.EndPort)
	assert.Equal(t, int32(65535), *ranged.EndPort)
	assert.Equal(t, corev1.ProtocolUDP, *ranged.Protocol)
}
