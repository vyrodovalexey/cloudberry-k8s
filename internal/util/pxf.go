package util

import (
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
)

// pxfServerKeySeparator is the separator in a rendered PXF servers ConfigMap key
// of the form "<server>__<file>.xml". It is the SINGLE source of truth shared
// with the builder's keyFor() so the diff helper splits keys exactly as the
// renderer joined them.
const pxfServerKeySeparator = "__"

// DiffPXFServerNames is a PURE diff of two rendered PXF servers ConfigMap Data
// maps by server NAME. It parses each "<server>__<file>.xml" key into its
// server-name component (keys without the "__" separator are top-level, NOT
// server-scoped, and are ignored for naming) and reports:
//
//   - added:   server names present in desired but not in existing.
//   - removed: server names present in existing but not in desired.
//   - updated: server names present in BOTH whose rendered per-file values
//     differ (any value for one of the server's keys changed, or its key set
//     changed).
//
// All three slices are sorted and deterministic so the derived event message is
// stable. The function never mutates its inputs and is safe with nil maps.
func DiffPXFServerNames(existingData, desiredData map[string]string) (added, removed, updated []string) {
	existingServers := groupPXFKeysByServer(existingData)
	desiredServers := groupPXFKeysByServer(desiredData)

	for name := range desiredServers {
		if _, ok := existingServers[name]; !ok {
			added = append(added, name)
		}
	}
	for name, existingFiles := range existingServers {
		desiredFiles, ok := desiredServers[name]
		if !ok {
			removed = append(removed, name)
			continue
		}
		if !pxfServerFilesEqual(existingFiles, desiredFiles) {
			updated = append(updated, name)
		}
	}

	sort.Strings(added)
	sort.Strings(removed)
	sort.Strings(updated)
	return added, removed, updated
}

// groupPXFKeysByServer groups a rendered ConfigMap Data map into a
// per-server-name map of file-key → value. Keys without the "<server>__" prefix
// (top-level files) are skipped because they are not server-scoped.
func groupPXFKeysByServer(data map[string]string) map[string]map[string]string {
	servers := make(map[string]map[string]string)
	for key, value := range data {
		idx := strings.Index(key, pxfServerKeySeparator)
		if idx <= 0 {
			continue
		}
		name := key[:idx]
		if servers[name] == nil {
			servers[name] = make(map[string]string)
		}
		servers[name][key] = value
	}
	return servers
}

// pxfServerFilesEqual reports whether two server-scoped file-key → value maps
// are identical (same key set and same values).
func pxfServerFilesEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for key, valA := range a {
		valB, ok := b[key]
		if !ok || valA != valB {
			return false
		}
	}
	return true
}

// FormatPXFServersChangedMessage renders a concise, bounded, deterministic event
// message describing a PXF servers ConfigMap Data change. The output has the
// shape "PXF servers changed: added=[..] removed=[..] updated=[..]".
func FormatPXFServersChangedMessage(added, removed, updated []string) string {
	return "PXF servers changed: " +
		"added=[" + strings.Join(added, ",") + "] " +
		"removed=[" + strings.Join(removed, ",") + "] " +
		"updated=[" + strings.Join(updated, ",") + "]"
}

// PXFContainerName is the container name of the PXF sidecar injected into the
// segment-primary pods. It is the single source of truth shared by the API
// handler (handlePXFStatus) and the controller (reconcilePxf) so they agree on
// which container's readiness is the PXF runtime signal.
const PXFContainerName = "pxf"

// PXF runtime status strings reported in
// status.dataLoading.pxf.status. They are HONEST, observation-derived values;
// an UNOBSERVABLE state is the empty string (absent), never one of these.
const (
	// PXFStatusRunning means every observed segment-primary "pxf" container is
	// ready (readyCount == total, total > 0).
	PXFStatusRunning = "Running"
	// PXFStatusStopped means segment-primary pods were observed but none of their
	// "pxf" containers are ready (readyCount == 0, total > 0).
	PXFStatusStopped = "Stopped"
	// PXFStatusError means some but not all observed "pxf" containers are ready
	// (0 < readyCount < total) — a segment's PXF is down (degraded).
	PXFStatusError = "Error"
)

// SegmentPrimaryPXFSelector returns the label selector used to list the
// segment-primary pods that host the PXF sidecar. It is the SINGLE source of
// truth shared by the API PXF-status handler and the controller's PXF reconcile
// so both aggregate readiness over exactly the same pod set.
func SegmentPrimaryPXFSelector(cluster string) map[string]string {
	return map[string]string{
		LabelCluster:   cluster,
		LabelComponent: ComponentSegmentPrimary,
	}
}

// PXFReadyCount aggregates the real readiness of the PXF sidecar across the
// given segment-primary pods. It returns the number of pods whose "pxf"
// container is observably Ready (readyCount) and the total number of pods
// considered (total = len(podList.Items)). The signal is derived ONLY from each
// pod's Status.ContainerStatuses — there is no live health probe, exec or
// cross-pod HTTP. A pod with no "pxf" container status is counted toward total
// but NOT toward readyCount (honest: it is not observably up). This is the
// shared aggregation behind both pxfSidecarStatuses (API) and the controller's
// PXF status, so the two never disagree.
func PXFReadyCount(podList *corev1.PodList) (readyCount, total int) {
	if podList == nil {
		return 0, 0
	}
	total = len(podList.Items)
	for i := range podList.Items {
		pod := &podList.Items[i]
		if pxfContainerReady(pod) {
			readyCount++
		}
	}
	return readyCount, total
}

// PXFReadyByHost is the PER-HOST disaggregation of the PXFReadyCount aggregation:
// it returns a map keyed by each observed segment-primary pod NAME with the value
// being that pod's real "pxf" container readiness (true=observably Ready). It is
// the SAME honest signal as PXFReadyCount — derived ONLY from each pod's
// Status.ContainerStatuses (no live probe, exec, or cross-pod HTTP), reusing the
// shared pxfContainerReady helper / PXFContainerName — but split per host so the
// caller can publish a per-segment_host pxf_service_up gauge. A pod with no "pxf"
// container status is reported as false (honest: not observably up), never absent.
// A nil podList yields an empty map.
func PXFReadyByHost(podList *corev1.PodList) map[string]bool {
	out := make(map[string]bool)
	if podList == nil {
		return out
	}
	for i := range podList.Items {
		pod := &podList.Items[i]
		out[pod.Name] = pxfContainerReady(pod)
	}
	return out
}

// pxfContainerReady reports whether the pod's "pxf" container is observably
// Ready, read straight from Status.ContainerStatuses. Absence of the container
// status is reported as not ready.
func pxfContainerReady(pod *corev1.Pod) bool {
	for j := range pod.Status.ContainerStatuses {
		cs := &pod.Status.ContainerStatuses[j]
		if cs.Name == PXFContainerName {
			return cs.Ready
		}
	}
	return false
}

// PXFStatusFromReadiness is the PURE mapping from a (readyCount, total)
// readiness aggregation to the honest PXF status string. It is the single
// source of truth for the locked status mapping:
//
//   - total == 0 (no pods / no pxf containers observed) → "" (UNOBSERVABLE,
//     ABSENT: the caller MUST NOT set status.dataLoading.pxf.status).
//   - readyCount == total (all ready, total > 0) → "Running".
//   - 0 < readyCount < total → "Error" (a segment's PXF is down; degraded).
//   - readyCount == 0 (total > 0) → "Stopped".
//
// Returning "" for the unobservable case keeps the status HONEST: an absent
// status never claims health that was not observed.
func PXFStatusFromReadiness(readyCount, total int) string {
	if total <= 0 {
		return ""
	}
	switch {
	case readyCount >= total:
		return PXFStatusRunning
	case readyCount == 0:
		return PXFStatusStopped
	default:
		return PXFStatusError
	}
}
