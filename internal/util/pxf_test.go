package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// pxfPod builds a segment-primary pod whose "pxf" container readiness is the
// given value. When hasPXF is false the pod carries NO "pxf" container status
// (only a non-pxf "segment" container) so it counts toward total but never
// toward readyCount — the honest "unobservable" case.
func pxfPod(name string, ready, hasPXF bool) corev1.Pod {
	statuses := []corev1.ContainerStatus{{Name: "segment", Ready: true}}
	if hasPXF {
		statuses = append(statuses, corev1.ContainerStatus{Name: PXFContainerName, Ready: ready})
	}
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Status:     corev1.PodStatus{ContainerStatuses: statuses},
	}
}

// TestSegmentPrimaryPXFSelector covers the SHARED label selector (105-S1-B7,
// Q3): the controller and the API handler must aggregate over EXACTLY the same
// segment-primary pod set, so the selector is the cluster + segment-primary
// component labels and nothing else.
func TestSegmentPrimaryPXFSelector(t *testing.T) {
	labels := SegmentPrimaryPXFSelector("my-cluster")

	assert.Equal(t, "my-cluster", labels[LabelCluster])
	assert.Equal(t, ComponentSegmentPrimary, labels[LabelComponent])
	// Exactly the two component labels — no extra keys that could narrow the set.
	assert.Len(t, labels, 2)
}

// TestPXFReadyCount covers the SHARED readiness aggregation over a PodList with
// mixed "pxf" container readiness (105-S1-B1..B4 inputs, 105-S1-B7 parity).
// A pod whose "pxf" container status is MISSING counts toward total but NOT
// toward readyCount (honest: it is not observably up).
func TestPXFReadyCount(t *testing.T) {
	tests := []struct {
		name      string
		pods      []corev1.Pod
		wantReady int
		wantTotal int
	}{
		{
			name:      "nil pod list → 0,0 (105-S1-B4)",
			pods:      nil,
			wantReady: 0,
			wantTotal: 0,
		},
		{
			name:      "no pods → 0,0 (105-S1-B4)",
			pods:      []corev1.Pod{},
			wantReady: 0,
			wantTotal: 0,
		},
		{
			name:      "all pxf containers ready → ready==total (105-S1-B1)",
			pods:      []corev1.Pod{pxfPod("a", true, true), pxfPod("b", true, true)},
			wantReady: 2,
			wantTotal: 2,
		},
		{
			name:      "mixed ready/not-ready → partial (105-S1-B2)",
			pods:      []corev1.Pod{pxfPod("a", true, true), pxfPod("b", false, true)},
			wantReady: 1,
			wantTotal: 2,
		},
		{
			name:      "all pxf containers not-ready → 0 ready (105-S1-B3)",
			pods:      []corev1.Pod{pxfPod("a", false, true), pxfPod("b", false, true)},
			wantReady: 0,
			wantTotal: 2,
		},
		{
			name:      "missing pxf container counts toward total not ready (105-S1-B4)",
			pods:      []corev1.Pod{pxfPod("a", true, true), pxfPod("b", false, false)},
			wantReady: 1,
			wantTotal: 2,
		},
		{
			name:      "all pods lack pxf container → ready 0, total>0 (105-S1-B4)",
			pods:      []corev1.Pod{pxfPod("a", false, false), pxfPod("b", false, false)},
			wantReady: 0,
			wantTotal: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var podList *corev1.PodList
			if tt.pods != nil {
				podList = &corev1.PodList{Items: tt.pods}
			}
			ready, total := PXFReadyCount(podList)
			assert.Equal(t, tt.wantReady, ready, "readyCount")
			assert.Equal(t, tt.wantTotal, total, "total")
		})
	}
}

// TestPXFReadyCount_NilPodList explicitly asserts the nil-input guard returns
// the honest zero aggregation (no panic).
func TestPXFReadyCount_NilPodList(t *testing.T) {
	ready, total := PXFReadyCount(nil)
	assert.Equal(t, 0, ready)
	assert.Equal(t, 0, total)
}

// TestPXFReadyByHost covers the M.1 per-host disaggregation (109-M1-U): the
// returned map has ONE entry per OBSERVED segment-primary pod NAME with the
// value being that pod's real "pxf" container readiness. A pod missing the
// "pxf" container status is reported as false (honest: not observably up), never
// absent, and a nil/empty PodList yields an empty (non-nil) map. The keys are
// derived purely from the observed pods, so an unobserved host never appears.
func TestPXFReadyByHost(t *testing.T) {
	tests := []struct {
		name string
		pods []corev1.Pod
		want map[string]bool
	}{
		{
			name: "nil pod list → empty map (109-M1-U)",
			pods: nil,
			want: map[string]bool{},
		},
		{
			name: "empty pod list → empty map (109-M1-U)",
			pods: []corev1.Pod{},
			want: map[string]bool{},
		},
		{
			name: "all pxf containers ready → all true (109-M1-U)",
			pods: []corev1.Pod{pxfPod("seg-0", true, true), pxfPod("seg-1", true, true)},
			want: map[string]bool{"seg-0": true, "seg-1": true},
		},
		{
			name: "mixed ready/not-ready → per-host bool (109-M1-U)",
			pods: []corev1.Pod{pxfPod("seg-0", true, true), pxfPod("seg-1", false, true)},
			want: map[string]bool{"seg-0": true, "seg-1": false},
		},
		{
			// A pod with NO "pxf" container status → false (counts as a host, but
			// honestly not observably up). It is present in the map, never absent.
			name: "missing pxf container → host present with false (109-M1-U)",
			pods: []corev1.Pod{pxfPod("seg-0", true, true), pxfPod("seg-1", false, false)},
			want: map[string]bool{"seg-0": true, "seg-1": false},
		},
		{
			name: "all pods lack pxf container → all false (109-M1-U)",
			pods: []corev1.Pod{pxfPod("seg-0", false, false), pxfPod("seg-1", false, false)},
			want: map[string]bool{"seg-0": false, "seg-1": false},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var podList *corev1.PodList
			if tt.pods != nil {
				podList = &corev1.PodList{Items: tt.pods}
			}
			got := PXFReadyByHost(podList)
			assert.NotNil(t, got, "result must be a non-nil map even for nil input")
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestPXFReadyByHost_NilPodList explicitly pins the nil-input guard: an empty,
// non-nil map (no panic, no fabricated host).
func TestPXFReadyByHost_NilPodList(t *testing.T) {
	got := PXFReadyByHost(nil)
	assert.NotNil(t, got)
	assert.Empty(t, got)
}

// TestPXFReadyByHost_ConsistentWithReadyCount ties PXFReadyByHost to the
// aggregate PXFReadyCount: the number of true entries in the per-host map MUST
// equal readyCount and the map size MUST equal total — they are the same honest
// readiness signal, just disaggregated.
func TestPXFReadyByHost_ConsistentWithReadyCount(t *testing.T) {
	pods := []corev1.Pod{
		pxfPod("seg-0", true, true),
		pxfPod("seg-1", false, true),
		pxfPod("seg-2", false, false), // missing pxf → false
	}
	podList := &corev1.PodList{Items: pods}

	byHost := PXFReadyByHost(podList)
	readyCount, total := PXFReadyCount(podList)

	trueCount := 0
	for _, ready := range byHost {
		if ready {
			trueCount++
		}
	}
	assert.Equal(t, readyCount, trueCount, "true entries must equal readyCount")
	assert.Equal(t, total, len(byHost), "map size must equal total observed pods")
}

// TestPXFStatusFromReadiness covers the FULL, locked status mapping table from
// (readyCount,total) to the honest status string (105-S1-B1..B4). The
// unobservable case (total==0) maps to "" — NEVER a synthesized health value.
func TestPXFStatusFromReadiness(t *testing.T) {
	tests := []struct {
		name  string
		ready int
		total int
		want  string
	}{
		// 105-S1-B4: unobservable → ABSENT (""), never forced to a health value.
		{"total 0 → absent (105-S1-B4)", 0, 0, ""},
		{"ready 0 total 0 → absent (105-S1-B4)", 0, 0, ""},
		{"negative total → absent (defensive)", 1, -1, ""},
		// 105-S1-B1: all ready → Running.
		{"ready==total==1 → Running (105-S1-B1)", 1, 1, PXFStatusRunning},
		{"ready==total==3 → Running (105-S1-B1)", 3, 3, PXFStatusRunning},
		// 105-S1-B2: partial → Error.
		{"0<ready<total → Error (105-S1-B2)", 1, 2, PXFStatusError},
		{"2 of 3 ready → Error (105-S1-B2)", 2, 3, PXFStatusError},
		// 105-S1-B3: none ready, total>0 → Stopped.
		{"ready 0 total 1 → Stopped (105-S1-B3)", 0, 1, PXFStatusStopped},
		{"ready 0 total 3 → Stopped (105-S1-B3)", 0, 3, PXFStatusStopped},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := PXFStatusFromReadiness(tt.ready, tt.total)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestPXFStatusFromReadiness_Constants pins the exact string values so the
// honest enum can never silently drift (105-S1-B1..B3).
func TestPXFStatusFromReadiness_Constants(t *testing.T) {
	assert.Equal(t, "Running", PXFStatusRunning)
	assert.Equal(t, "Stopped", PXFStatusStopped)
	assert.Equal(t, "Error", PXFStatusError)
	assert.Equal(t, "pxf", PXFContainerName)
}

// TestDiffPXFServerNames covers the PURE server-name diff helper that backs the
// HONEST PXFServersChanged event message (Scenario 106 SL.7/SL.8 + MX honesty).
// It parses "<server>__<file>.xml" keys into server-name sets and reports the
// added/removed/updated server names — all SORTED and deterministic. Keys
// without the "__" separator (top-level files like the connectors listing) are
// ignored, never mis-attributed to a server.
func TestDiffPXFServerNames(t *testing.T) {
	tests := []struct {
		name        string
		existing    map[string]string
		desired     map[string]string
		wantAdded   []string
		wantRemoved []string
		wantUpdated []string
	}{
		{
			// 106-SL7-B*: same server set, one server's rendered file value
			// changed (e.g. patched fs.s3a.endpoint in s3-site.xml) → updated only.
			name: "update only: one server's file value changed (SL.7)",
			existing: map[string]string{
				"minio-warehouse__s3-site.xml": "<endpoint>OLD</endpoint>",
				"hdfs__core-site.xml":          "<fs>same</fs>",
			},
			desired: map[string]string{
				"minio-warehouse__s3-site.xml": "<endpoint>NEW</endpoint>",
				"hdfs__core-site.xml":          "<fs>same</fs>",
			},
			wantAdded:   nil,
			wantRemoved: nil,
			wantUpdated: []string{"minio-warehouse"},
		},
		{
			// 106-SL8-B*: a server removed → removed set, its keys gone.
			name: "server removed (SL.8)",
			existing: map[string]string{
				"s3-a__s3-site.xml":   "<a/>",
				"s3-b__s3-site.xml":   "<b/>",
				"hdfs__core-site.xml": "<h/>",
			},
			desired: map[string]string{
				"s3-a__s3-site.xml":   "<a/>",
				"hdfs__core-site.xml": "<h/>",
			},
			wantAdded:   nil,
			wantRemoved: []string{"s3-b"},
			wantUpdated: nil,
		},
		{
			// server added → added set.
			name: "server added",
			existing: map[string]string{
				"s3-a__s3-site.xml": "<a/>",
			},
			desired: map[string]string{
				"s3-a__s3-site.xml": "<a/>",
				"s3-b__s3-site.xml": "<b/>",
			},
			wantAdded:   []string{"s3-b"},
			wantRemoved: nil,
			wantUpdated: nil,
		},
		{
			// 106-MX-B2: identical maps → all empty (no-op honesty).
			name: "no change: identical maps → all empty (MX honesty)",
			existing: map[string]string{
				"s3-a__s3-site.xml":   "<a/>",
				"hdfs__core-site.xml": "<h/>",
			},
			desired: map[string]string{
				"s3-a__s3-site.xml":   "<a/>",
				"hdfs__core-site.xml": "<h/>",
			},
			wantAdded:   nil,
			wantRemoved: nil,
			wantUpdated: nil,
		},
		{
			// 106-MX-B*: mixed add + remove + update in one diff → correct sorted
			// sets. zeta/alpha/mike chosen so sort order is non-trivial.
			name: "mixed add+remove+update in one diff (sorted)",
			existing: map[string]string{
				"alpha__s3-site.xml":  "<a>old</a>",
				"mike__core-site.xml": "<m/>",
				"zeta__s3-site.xml":   "<z/>",
			},
			desired: map[string]string{
				"alpha__s3-site.xml":  "<a>new</a>", // updated
				"mike__core-site.xml": "<m/>",       // unchanged
				"bravo__s3-site.xml":  "<b/>",       // added
				"delta__s3-site.xml":  "<d/>",       // added
			},
			wantAdded:   []string{"bravo", "delta"},
			wantRemoved: []string{"zeta"},
			wantUpdated: []string{"alpha"},
		},
		{
			// A server whose KEY SET changed (a file added/removed) is "updated".
			name: "server key set changed → updated",
			existing: map[string]string{
				"hdfs__core-site.xml": "<c/>",
			},
			desired: map[string]string{
				"hdfs__core-site.xml": "<c/>",
				"hdfs__hdfs-site.xml": "<h/>",
			},
			wantAdded:   nil,
			wantRemoved: nil,
			wantUpdated: []string{"hdfs"},
		},
		{
			name:        "both nil → all empty",
			existing:    nil,
			desired:     nil,
			wantAdded:   nil,
			wantRemoved: nil,
			wantUpdated: nil,
		},
		{
			name:     "nil existing, populated desired → all added",
			existing: nil,
			desired: map[string]string{
				"s3-a__s3-site.xml": "<a/>",
			},
			wantAdded:   []string{"s3-a"},
			wantRemoved: nil,
			wantUpdated: nil,
		},
		{
			name: "populated existing, nil desired → all removed",
			existing: map[string]string{
				"s3-a__s3-site.xml": "<a/>",
			},
			desired:     nil,
			wantAdded:   nil,
			wantRemoved: []string{"s3-a"},
			wantUpdated: nil,
		},
		{
			name:        "empty maps → all empty",
			existing:    map[string]string{},
			desired:     map[string]string{},
			wantAdded:   nil,
			wantRemoved: nil,
			wantUpdated: nil,
		},
		{
			// Keys without the "__" separator (top-level connectors listing) are
			// NOT server-scoped and must be ignored, even when they differ.
			name: "top-level keys without __ are ignored, not mis-attributed",
			existing: map[string]string{
				"pxf-connectors.txt": "old listing",
				"s3-a__s3-site.xml":  "<a/>",
			},
			desired: map[string]string{
				"pxf-connectors.txt": "new listing", // changed but NOT a server
				"s3-a__s3-site.xml":  "<a/>",        // unchanged server
			},
			wantAdded:   nil,
			wantRemoved: nil,
			wantUpdated: nil,
		},
		{
			// A leading "__" (idx == 0) yields an empty server name and must be
			// skipped (the idx<=0 guard), never produce a "" server.
			name: "leading separator key is skipped (idx<=0 guard)",
			existing: map[string]string{
				"__weird.xml":       "x",
				"s3-a__s3-site.xml": "<a/>",
			},
			desired: map[string]string{
				"__weird.xml":       "y",
				"s3-a__s3-site.xml": "<a/>",
			},
			wantAdded:   nil,
			wantRemoved: nil,
			wantUpdated: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			added, removed, updated := DiffPXFServerNames(tt.existing, tt.desired)
			assert.Equal(t, tt.wantAdded, added, "added")
			assert.Equal(t, tt.wantRemoved, removed, "removed")
			assert.Equal(t, tt.wantUpdated, updated, "updated")
		})
	}
}

// TestDiffPXFServerNames_DoesNotMutateInputs guards the PURE-function contract:
// the diff never mutates the maps it is handed.
func TestDiffPXFServerNames_DoesNotMutateInputs(t *testing.T) {
	existing := map[string]string{"s3-a__s3-site.xml": "<a/>"}
	desired := map[string]string{"s3-b__s3-site.xml": "<b/>"}

	DiffPXFServerNames(existing, desired)

	assert.Equal(t, map[string]string{"s3-a__s3-site.xml": "<a/>"}, existing)
	assert.Equal(t, map[string]string{"s3-b__s3-site.xml": "<b/>"}, desired)
}

// TestDiffPXFServerNames_Deterministic asserts the SORTED contract holds
// regardless of map iteration order: many added/removed servers come back in a
// stable sorted order so the derived event message never flaps.
func TestDiffPXFServerNames_Deterministic(t *testing.T) {
	existing := map[string]string{
		"z9__s3-site.xml": "<z9/>",
		"m5__s3-site.xml": "<m5/>",
		"a1__s3-site.xml": "<a1/>",
	}
	desired := map[string]string{
		"q7__s3-site.xml": "<q7/>",
		"b2__s3-site.xml": "<b2/>",
		"f3__s3-site.xml": "<f3/>",
	}
	for i := 0; i < 10; i++ {
		added, removed, updated := DiffPXFServerNames(existing, desired)
		assert.Equal(t, []string{"b2", "f3", "q7"}, added)
		assert.Equal(t, []string{"a1", "m5", "z9"}, removed)
		assert.Empty(t, updated)
	}
}

// TestFormatPXFServersChangedMessage covers the bounded, deterministic event
// message shape "PXF servers changed: added=[..] removed=[..] updated=[..]".
// Empty lists render as "[]"; populated lists are joined comma-separated in the
// caller-provided (sorted) order (Scenario 106 event granularity).
func TestFormatPXFServersChangedMessage(t *testing.T) {
	tests := []struct {
		name    string
		added   []string
		removed []string
		updated []string
		want    string
	}{
		{
			name: "all empty lists render as []",
			want: "PXF servers changed: added=[] removed=[] updated=[]",
		},
		{
			name:    "update only (SL.7)",
			updated: []string{"minio-warehouse"},
			want:    "PXF servers changed: added=[] removed=[] updated=[minio-warehouse]",
		},
		{
			name:    "removed only (SL.8)",
			removed: []string{"s3-b"},
			want:    "PXF servers changed: added=[] removed=[s3-b] updated=[]",
		},
		{
			name:  "added only",
			added: []string{"s3-c"},
			want:  "PXF servers changed: added=[s3-c] removed=[] updated=[]",
		},
		{
			name:    "mixed with multi-element sorted lists",
			added:   []string{"bravo", "delta"},
			removed: []string{"zeta"},
			updated: []string{"alpha"},
			want:    "PXF servers changed: added=[bravo,delta] removed=[zeta] updated=[alpha]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := FormatPXFServersChangedMessage(tt.added, tt.removed, tt.updated)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestDiffAndFormatPXFServers_EndToEnd ties the diff and the message formatter
// together exactly as the controller/api callers compose them, proving the
// honest message reflects a real (existing→desired) Data diff (106-MX-B1).
func TestDiffAndFormatPXFServers_EndToEnd(t *testing.T) {
	existing := map[string]string{
		"keep__s3-site.xml":  "<k/>",
		"drop__s3-site.xml":  "<d/>",
		"patch__s3-site.xml": "<p>old</p>",
	}
	desired := map[string]string{
		"keep__s3-site.xml":  "<k/>",
		"patch__s3-site.xml": "<p>new</p>",
		"add__s3-site.xml":   "<a/>",
	}

	added, removed, updated := DiffPXFServerNames(existing, desired)
	msg := FormatPXFServersChangedMessage(added, removed, updated)

	assert.Equal(t,
		"PXF servers changed: added=[add] removed=[drop] updated=[patch]", msg)
}

// TestPXFReadiness_EndToEndMapping ties PXFReadyCount and
// PXFStatusFromReadiness together over real PodLists, mirroring exactly how the
// API handler and the controller consume the SHARED helper (105-S1-B7).
func TestPXFReadiness_EndToEndMapping(t *testing.T) {
	tests := []struct {
		name string
		pods []corev1.Pod
		want string
	}{
		{
			name: "all ready → Running (105-S1-B1)",
			pods: []corev1.Pod{pxfPod("a", true, true), pxfPod("b", true, true)},
			want: PXFStatusRunning,
		},
		{
			name: "one down → Error (105-S1-B2)",
			pods: []corev1.Pod{pxfPod("a", true, true), pxfPod("b", false, true)},
			want: PXFStatusError,
		},
		{
			name: "all down → Stopped (105-S1-B3)",
			pods: []corev1.Pod{pxfPod("a", false, true), pxfPod("b", false, true)},
			want: PXFStatusStopped,
		},
		{
			name: "no pods → absent (105-S1-B4)",
			pods: []corev1.Pod{},
			want: "",
		},
		{
			name: "pods without pxf container → absent health (Stopped) honest (105-S1-B4)",
			pods: []corev1.Pod{pxfPod("a", false, false)},
			want: PXFStatusStopped,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			podList := &corev1.PodList{Items: tt.pods}
			ready, total := PXFReadyCount(podList)
			assert.Equal(t, tt.want, PXFStatusFromReadiness(ready, total))
		})
	}
}
