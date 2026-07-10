// Package builder provides functions to construct Kubernetes resources from CloudberryCluster specs.
package builder

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
)

// defaultAntiAffinityTopologyKey is the node topology key used by every
// cross-role anti-affinity term when the cluster does not override it. It
// matches the value the segment affinity has always shipped with, so a nil
// spec.affinity yields byte-identical output.
const defaultAntiAffinityTopologyKey = "kubernetes.io/hostname"

// preferredAntiAffinityWeight is the weight applied to every preferred
// (best-effort) cross-role anti-affinity term. It matches the value the segment
// affinity has always shipped with.
const preferredAntiAffinityWeight int32 = 100

// resolvedAffinity is the fully materialized view of a ClusterAffinitySpec. It
// is the SINGLE source of truth for whether each cross-role term is emitted and
// with what placement/topology. The defaulter only fills Type/TopologyKey; mode
// materialization lives here so there is never a double resolution.
type resolvedAffinity struct {
	// segmentMirror toggles the segment primary<->mirror cross-component term.
	// The segment term is ALWAYS emitted (default cluster behavior), so this is
	// true unless a nil spec keeps the historical default. See resolveAffinity.
	segmentMirror bool
	// coordinatorBackup toggles the backup-Job<->coordinator term.
	coordinatorBackup bool
	// coordinatorStandby toggles the standby<->coordinator term.
	coordinatorStandby bool
	// required selects required (hard) placement for the coordinator terms.
	// The segment primary<->mirror term ignores this and is always preferred.
	required bool
	// topologyKey is the node topology key for every term.
	topologyKey string
}

// resolveAffinity materializes a ClusterAffinitySpec into a resolvedAffinity. It
// is nil-safe: a nil spec returns the historical default (segment primary<->mirror
// preferred term only, hostname topology, no coordinator terms), preserving the
// byte-identical baseline.
//
// Resolution order:
//  1. Mode materializes preset toggles (full => all three; segment-mirror => segment only).
//  2. Explicit *bool overrides apply on top of the preset (including explicit false).
//  3. Type => required flag (coordinator terms only); TopologyKey defaults to hostname.
func resolveAffinity(spec *cbv1alpha1.ClusterAffinitySpec) resolvedAffinity {
	// Historical default: segment primary<->mirror preferred term is always on;
	// coordinator terms are off. A nil spec must reproduce this exactly.
	res := resolvedAffinity{
		segmentMirror:      true,
		coordinatorBackup:  false,
		coordinatorStandby: false,
		required:           false,
		topologyKey:        defaultAntiAffinityTopologyKey,
	}
	if spec == nil {
		return res
	}

	// Step 1: mode preset.
	switch spec.Mode {
	case cbv1alpha1.ClusterAntiAffinityModeFull:
		res.segmentMirror = true
		res.coordinatorBackup = true
		res.coordinatorStandby = true
	case cbv1alpha1.ClusterAntiAffinityModeSegmentMirror:
		res.segmentMirror = true
	default:
		// Empty/unknown mode leaves the historical default (segment on only).
		// Enum validation is enforced by the CRD schema + webhook.
	}

	// Step 2: explicit per-field overrides (including explicit false).
	if spec.SegmentMirrorAntiAffinity != nil {
		res.segmentMirror = *spec.SegmentMirrorAntiAffinity
	}
	if spec.CoordinatorBackupAntiAffinity != nil {
		res.coordinatorBackup = *spec.CoordinatorBackupAntiAffinity
	}
	if spec.CoordinatorStandbyAntiAffinity != nil {
		res.coordinatorStandby = *spec.CoordinatorStandbyAntiAffinity
	}

	// Step 3: type + topology.
	res.required = spec.Type == cbv1alpha1.AntiAffinityRequired
	if spec.TopologyKey != "" {
		res.topologyKey = spec.TopologyKey
	}
	return res
}

// crossRoleAntiAffinityTerm builds a pod anti-affinity term selecting the pods
// of targetComponent within the same cluster at topologyKey. It references only
// the two identity labels (cluster + component) so it matches regardless of
// managed-by drift.
func crossRoleAntiAffinityTerm(
	cluster *cbv1alpha1.CloudberryCluster,
	targetComponent, topologyKey string,
) corev1.PodAffinityTerm {
	return corev1.PodAffinityTerm{
		LabelSelector: &metav1.LabelSelector{
			MatchLabels: map[string]string{
				util.LabelCluster:   cluster.Name,
				util.LabelComponent: targetComponent,
			},
		},
		TopologyKey: topologyKey,
	}
}

// buildStandbyAffinity returns the anti-affinity applied to the standby pod
// template. When the resolved coordinatorStandby toggle is on it emits a term
// selecting the coordinator pod (required or preferred per the resolved Type);
// otherwise it returns nil so the standby pod keeps NO affinity (byte-identical
// to the historical default). The coordinator STS itself is never touched.
func buildStandbyAffinity(cluster *cbv1alpha1.CloudberryCluster) *corev1.Affinity {
	resolved := resolveAffinity(cluster.Spec.Affinity)
	if !resolved.coordinatorStandby {
		return nil
	}
	term := crossRoleAntiAffinityTerm(cluster, util.ComponentCoordinator, resolved.topologyKey)
	return mergeAntiAffinityTerm(nil, resolved.required, term)
}

// buildBackupAntiAffinity merges a coordinator anti-affinity term onto the given
// backup Job pod affinity when the resolved coordinatorBackup toggle is on. It is
// additive: any affinity already present on the pod spec is preserved. When the
// toggle is off it returns the input unchanged (nil stays nil), keeping the
// default backup Job pod byte-identical.
func buildBackupAntiAffinity(
	cluster *cbv1alpha1.CloudberryCluster,
	affinity *corev1.Affinity,
) *corev1.Affinity {
	resolved := resolveAffinity(cluster.Spec.Affinity)
	if !resolved.coordinatorBackup {
		return affinity
	}
	term := crossRoleAntiAffinityTerm(cluster, util.ComponentCoordinator, resolved.topologyKey)
	return mergeAntiAffinityTerm(affinity, resolved.required, term)
}

// mergeAntiAffinityTerm additively merges a single pod anti-affinity term into
// an existing (possibly nil) affinity, returning the (possibly newly allocated)
// affinity. It:
//   - allocates Affinity/PodAntiAffinity only when nil (never allocates empty
//     NodeAffinity/PodAffinity siblings),
//   - appends to Required... when required, otherwise Preferred... at weight 100,
//   - NEVER overwrites existing NodeAffinity/PodAffinity or pre-existing
//     PodAntiAffinity terms,
//   - appends deterministically so byte-identical render tests stay stable.
func mergeAntiAffinityTerm(
	affinity *corev1.Affinity,
	required bool,
	term corev1.PodAffinityTerm,
) *corev1.Affinity {
	if affinity == nil {
		affinity = &corev1.Affinity{}
	}
	if affinity.PodAntiAffinity == nil {
		affinity.PodAntiAffinity = &corev1.PodAntiAffinity{}
	}
	paa := affinity.PodAntiAffinity

	if required {
		paa.RequiredDuringSchedulingIgnoredDuringExecution = append(
			paa.RequiredDuringSchedulingIgnoredDuringExecution, term,
		)
		return affinity
	}

	paa.PreferredDuringSchedulingIgnoredDuringExecution = append(
		paa.PreferredDuringSchedulingIgnoredDuringExecution,
		corev1.WeightedPodAffinityTerm{
			Weight:          preferredAntiAffinityWeight,
			PodAffinityTerm: term,
		},
	)
	return affinity
}
