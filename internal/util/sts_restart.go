package util

import (
	"context"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/telemetry"
)

// stsRestartTracerName is the tracer name for the leaf-package STS restart
// primitive span. internal/util is a leaf package and internal/telemetry
// imports no internal package, so this adds no import cycle.
const stsRestartTracerName = "util"

// PatchStatefulSetRestartTrigger triggers a rolling update of the named
// StatefulSet by stamping the pod-template annotation AnnotationRestartTrigger
// with the current UTC timestamp (RFC3339Nano). Changing the pod-template
// annotation causes the StatefulSet controller to roll its pods.
//
// This is the shared restart primitive used both by the AdminReconciler
// (rolling restart) and by the REST API PXF restart/sync handlers. It lives in
// internal/util — a leaf package imported by both internal/api and
// internal/controller — to avoid an import cycle between those two packages.
//
// The function performs a Get followed by an Update; callers that race on the
// same StatefulSet may observe a conflict error from the API server and should
// retry as appropriate.
func PatchStatefulSetRestartTrigger(ctx context.Context, c client.Client, namespace, name string) (err error) {
	// Span around the Get+Update so the "why did these pods roll" K8s I/O is
	// traced at the primitive level (nests under the caller's request/controller
	// span). No-op when telemetry is disabled.
	ctx, span := telemetry.StartSpan(ctx, stsRestartTracerName, "util.patchStatefulSetRestartTrigger")
	defer func() {
		telemetry.SetSpanError(span, err)
		span.End()
	}()

	sts := &appsv1.StatefulSet{}
	if err = c.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, sts); err != nil {
		return fmt.Errorf("getting statefulset %s/%s: %w", namespace, name, err)
	}

	if sts.Spec.Template.Annotations == nil {
		sts.Spec.Template.Annotations = make(map[string]string)
	}
	sts.Spec.Template.Annotations[AnnotationRestartTrigger] = time.Now().UTC().Format(time.RFC3339Nano)

	if err = c.Update(ctx, sts); err != nil {
		return fmt.Errorf("updating statefulset %s/%s: %w", namespace, name, err)
	}

	return nil
}
