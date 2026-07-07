package builder

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// segHLSearchDomain returns the expected segment headless-service DNS search
// domain for the given cluster (the domain that makes bare segment pod names
// resolve, fixing gpexpand's internal short-hostname rsync/ssh).
func segHLSearchDomain(cluster *cbv1alpha1.CloudberryCluster) string {
	return fmt.Sprintf(
		"%s.%s.svc.cluster.local",
		util.SegmentServiceName(cluster.Name), cluster.Namespace,
	)
}

// assertDNSSearchesPresent asserts the pod template carries the segment,
// coordinator and standby headless-service search domains and keeps the DNS
// policy at its default (unset => ClusterFirst) so the searches augment cluster
// DNS rather than replacing it.
func assertDNSSearchesPresent(t *testing.T, cluster *cbv1alpha1.CloudberryCluster, sts *appsv1.StatefulSet) {
	t.Helper()
	require.NotNil(t, sts)
	require.NotNil(t, sts.Spec.Template.Spec.DNSConfig, "pod spec must carry a dnsConfig")

	searches := sts.Spec.Template.Spec.DNSConfig.Searches
	ns := cluster.Namespace
	assert.Contains(t, searches, segHLSearchDomain(cluster),
		"segment headless search domain must be present so bare segment pod names resolve")
	assert.Contains(t, searches,
		fmt.Sprintf("%s.%s.svc.cluster.local", util.CoordinatorServiceName(cluster.Name), ns),
		"coordinator headless search domain must be present")
	assert.Contains(t, searches,
		fmt.Sprintf("%s.%s.svc.cluster.local", util.StandbyServiceName(cluster.Name), ns),
		"standby headless search domain must be present")

	// dnsPolicy default: leave empty so searches AUGMENT cluster DNS.
	assert.Equal(t, corev1.DNSPolicy(""), sts.Spec.Template.Spec.DNSPolicy,
		"dnsPolicy must remain default (ClusterFirst) so searches do not replace cluster DNS")
}

func TestBuildCoordinatorStatefulSet_DNSSearches(t *testing.T) {
	b := NewBuilder()
	cluster := newTestCluster()

	sts, err := b.BuildCoordinatorStatefulSet(cluster)
	require.NoError(t, err)
	assertDNSSearchesPresent(t, cluster, sts)
}

func TestBuildStandbyStatefulSet_DNSSearches(t *testing.T) {
	b := NewBuilder()
	cluster := newTestCluster()
	cluster.Spec.Standby = &cbv1alpha1.StandbySpec{Enabled: true}

	sts, err := b.BuildStandbyStatefulSet(cluster)
	require.NoError(t, err)
	assertDNSSearchesPresent(t, cluster, sts)
}

func TestBuildSegmentPrimaryStatefulSet_DNSSearches(t *testing.T) {
	b := NewBuilder()
	cluster := newTestCluster()

	sts, err := b.BuildSegmentPrimaryStatefulSet(cluster)
	require.NoError(t, err)
	assertDNSSearchesPresent(t, cluster, sts)
}

func TestBuildSegmentMirrorStatefulSet_DNSSearches(t *testing.T) {
	b := NewBuilder()
	cluster := newTestCluster()
	cluster.Spec.Segments.Mirroring = &cbv1alpha1.MirroringSpec{Enabled: true}

	sts, err := b.BuildSegmentMirrorStatefulSet(cluster)
	require.NoError(t, err)
	require.NotNil(t, sts)
	// Primaries and mirrors share the SegmentServiceName headless service, so the
	// one segment search domain covers both StatefulSets.
	assertDNSSearchesPresent(t, cluster, sts)
}

// TestAddClusterDNSSearchDomains_Deduplicates verifies the helper does not
// append a search domain that is already present (idempotent, stable template).
func TestAddClusterDNSSearchDomains_Deduplicates(t *testing.T) {
	cluster := newTestCluster()
	spec := &corev1.PodSpec{
		DNSConfig: &corev1.PodDNSConfig{
			Searches: []string{segHLSearchDomain(cluster)},
		},
	}

	addClusterDNSSearchDomains(cluster, spec)

	count := 0
	for _, s := range spec.DNSConfig.Searches {
		if s == segHLSearchDomain(cluster) {
			count++
		}
	}
	assert.Equal(t, 1, count, "segment search domain must not be duplicated")
}
