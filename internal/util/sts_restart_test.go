package util

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/codes"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/cloudberry-contrib/cloudberry-k8s/internal/telemetry"
)

func stsRestartScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, appsv1.AddToScheme(scheme))
	return scheme
}

func TestPatchStatefulSetRestartTrigger_SetsAnnotation(t *testing.T) {
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-segment-primary", Namespace: "ns"},
	}
	c := fake.NewClientBuilder().WithScheme(stsRestartScheme(t)).WithObjects(sts).Build()

	require.NoError(t, PatchStatefulSetRestartTrigger(
		context.Background(), c, "ns", "demo-segment-primary"))

	got := &appsv1.StatefulSet{}
	require.NoError(t, c.Get(context.Background(),
		types.NamespacedName{Name: "demo-segment-primary", Namespace: "ns"}, got))
	assert.NotEmpty(t, got.Spec.Template.Annotations[AnnotationRestartTrigger])
}

func TestPatchStatefulSetRestartTrigger_PreservesExistingAnnotations(t *testing.T) {
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-segment-primary", Namespace: "ns"},
	}
	sts.Spec.Template.Annotations = map[string]string{"keep": "me"}
	c := fake.NewClientBuilder().WithScheme(stsRestartScheme(t)).WithObjects(sts).Build()

	require.NoError(t, PatchStatefulSetRestartTrigger(
		context.Background(), c, "ns", "demo-segment-primary"))

	got := &appsv1.StatefulSet{}
	require.NoError(t, c.Get(context.Background(),
		types.NamespacedName{Name: "demo-segment-primary", Namespace: "ns"}, got))
	assert.Equal(t, "me", got.Spec.Template.Annotations["keep"])
	assert.NotEmpty(t, got.Spec.Template.Annotations[AnnotationRestartTrigger])
}

func TestPatchStatefulSetRestartTrigger_NotFound(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(stsRestartScheme(t)).Build()
	err := PatchStatefulSetRestartTrigger(context.Background(), c, "ns", "missing")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "getting statefulset")
}

// TestPatchStatefulSetRestartTrigger_EmitsSpan verifies the C-4 span (T9): a
// successful Get+Update emits a span named "util.patchStatefulSetRestartTrigger"
// (via telemetry.StartSpan) that ends WITHOUT an error status.
func TestPatchStatefulSetRestartTrigger_EmitsSpan(t *testing.T) {
	sr, restore := telemetry.InstallSpanRecorder()
	defer restore()

	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-segment-primary", Namespace: "ns"},
	}
	c := fake.NewClientBuilder().WithScheme(stsRestartScheme(t)).WithObjects(sts).Build()

	require.NoError(t, PatchStatefulSetRestartTrigger(
		context.Background(), c, "ns", "demo-segment-primary"))

	var found bool
	for _, s := range sr.Ended() {
		if s.Name() == "util.patchStatefulSetRestartTrigger" {
			found = true
			assert.NotEqual(t, codes.Error, s.Status().Code,
				"the span must NOT be errored on the success path")
		}
	}
	assert.True(t, found, "util.patchStatefulSetRestartTrigger span must exist")
}

// TestPatchStatefulSetRestartTrigger_SpanErrorOnNotFound verifies the C-4 span
// error path (T9): when the STS Get fails (missing STS) the span is ended with
// codes.Error (SetSpanError records the propagated error onto the span).
func TestPatchStatefulSetRestartTrigger_SpanErrorOnNotFound(t *testing.T) {
	sr, restore := telemetry.InstallSpanRecorder()
	defer restore()

	c := fake.NewClientBuilder().WithScheme(stsRestartScheme(t)).Build()
	err := PatchStatefulSetRestartTrigger(context.Background(), c, "ns", "missing")
	require.Error(t, err)

	var found bool
	for _, s := range sr.Ended() {
		if s.Name() == "util.patchStatefulSetRestartTrigger" {
			found = true
			assert.Equal(t, codes.Error, s.Status().Code,
				"the span must be codes.Error when the Get fails")
		}
	}
	assert.True(t, found, "util.patchStatefulSetRestartTrigger span must exist")
}

// TestPatchStatefulSetRestartTrigger_SpanErrorOnUpdate verifies the span records
// an error status when the Update fails (the second error return of the function)
// — covering the Update-error branch's SetSpanError path.
func TestPatchStatefulSetRestartTrigger_SpanErrorOnUpdate(t *testing.T) {
	sr, restore := telemetry.InstallSpanRecorder()
	defer restore()

	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-segment-primary", Namespace: "ns"},
	}
	updateBoom := errors.New("update boom")
	c := fake.NewClientBuilder().WithScheme(stsRestartScheme(t)).WithObjects(sts).
		WithInterceptorFuncs(interceptor.Funcs{
			Update: func(_ context.Context, _ client.WithWatch, _ client.Object,
				_ ...client.UpdateOption) error {
				return updateBoom
			},
		}).Build()

	err := PatchStatefulSetRestartTrigger(context.Background(), c, "ns", "demo-segment-primary")
	require.Error(t, err)
	assert.ErrorIs(t, err, updateBoom)

	var found bool
	for _, s := range sr.Ended() {
		if s.Name() == "util.patchStatefulSetRestartTrigger" {
			found = true
			assert.Equal(t, codes.Error, s.Status().Code,
				"the span must be codes.Error when the Update fails")
		}
	}
	assert.True(t, found, "util.patchStatefulSetRestartTrigger span must exist")
}
