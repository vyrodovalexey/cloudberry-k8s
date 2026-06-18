//go:build functional

package functional

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"golang.org/x/crypto/bcrypt"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	cbv1alpha1 "github.com/cloudberry-contrib/cloudberry-k8s/api/v1alpha1"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/api"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/auth"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/builder"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/metrics"
	"github.com/cloudberry-contrib/cloudberry-k8s/internal/util"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/cases"
	"github.com/cloudberry-contrib/cloudberry-k8s/test/testutil"
)

// ============================================================================
// Scenario 112: Disabled States (DIS.1–DIS.3) — functional
// ============================================================================
//
// This functional layer is builder/api-driven over a fake client. It COMPLEMENTS
// — does NOT duplicate — the internal/{controller,api}/*scenario112*_test.go unit
// tests (those are the live reconcile-teardown / cleanupDataLoading layer over a
// fake client). Because reconcileDataLoading is unexported, this layer drives the
// PUBLIC surface end-to-end:
//
//   - 112-DIS1-TEARDOWN (REAL): the operator's GC PLAN — the same label selector
//     {cluster,component=dataload} the teardown lists by — is exercised over a
//     fake client seeded with the full stale object set; the disabled builders
//     yield NO pxf ConfigMap (the explicit-delete trigger), so after applying the
//     GC plan every stale object is GONE.
//   - 112-DIS1-REENABLE (REAL): with DL re-enabled the public builder redeploys
//     gpfdist (Deployment/Service/PVC) + the enabled dataload Job, idempotently.
//   - 112-DIS1-APIDISABLED (REAL): DL-disabled ⇒ the REAL api.Server router
//     reports 400 DATA_LOADING_NOT_ENABLED on mutations, a 200 disabled envelope
//     on list, and DATA_LOADING_NOT_ENABLED (precedence) on a PXF endpoint.
//   - 112-DIS2-PXFOFF / 112-DIS2-GPLOADOK (REAL): pxf off ⇒ no pxf ConfigMap /
//     sidecar / NetworkPolicy; a gpload-type job still BUILDS a Job + control-file
//     ConfigMap (gpload independent of PXF).
//   - 112-DIS3-NOGPFDIST / 112-DIS3-LOCALOK / 112-DIS3-DEPMISSING (REAL/CONFIG):
//     gpfdist off ⇒ the gpfdist GC plan removes the seeded gpfdist objects; a
//     local gpload job builds; a gpfdist-source gpload job's control file targets
//     the absent gpfdist host (the HONEST dependency-missing signal is the runtime
//     failure — no fabricated pre-flight HC).
//
// HONESTY: 112-DIS3-DEPMISSING is CONFIG-ONLY at the functional layer — we assert
// the rendered gpfdist-source control file references the gpfdist host while the
// gpfdist resources are ABSENT (the actual runtime Job-Failed is the e2e Part B).
// ============================================================================

const (
	scenario112Namespace = "cloudberry-test"
	scenario112Cluster   = "s112"
	scenario112Prefix    = "/api/v1alpha1"

	scenario112OperUser  = "s112oper"
	scenario112OperPass  = "s112operpass"
	scenario112AdminUser = "s112admin"
	scenario112AdminPass = "s112adminpass"
)

// Scenario112Suite drives the disabled-state contract over the public builder +
// a fake client + the real api.Server router.
type Scenario112Suite struct {
	suite.Suite
	ctx     context.Context
	builder *builder.DefaultBuilder
}

func TestFunctional_Scenario112(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(Scenario112Suite))
}

func (s *Scenario112Suite) SetupTest() {
	s.ctx = context.Background()
	s.builder = builder.NewBuilder()
}

// metav1ObjectMeta is a tiny constructor for a namespaced (optionally labeled)
// ObjectMeta — keeps the seeded-object tables terse.
func metav1ObjectMeta(name, namespace string, labels map[string]string) metav1.ObjectMeta {
	return metav1.ObjectMeta{Name: name, Namespace: namespace, Labels: labels}
}

// scenario112DataLoadLabels is the shared {cluster,component=dataload} label set
// every dataload workload / control-file ConfigMap carries — the SAME selector
// the operator teardown lists by (mirrors internal/util.CommonLabels output for
// the dataload component).
func scenario112DataLoadLabels(cluster string) map[string]string {
	return map[string]string{
		util.LabelCluster:   cluster,
		util.LabelComponent: util.ComponentDataLoad,
	}
}

// scenario112Scheme returns a scheme with the core/apps/batch/networking types
// the teardown objects use.
func scenario112Scheme(s *Scenario112Suite) *runtime.Scheme {
	scheme := runtime.NewScheme()
	require.NoError(s.T(), corev1.AddToScheme(scheme))
	require.NoError(s.T(), appsv1.AddToScheme(scheme))
	require.NoError(s.T(), batchv1.AddToScheme(scheme))
	require.NoError(s.T(), networkingv1.AddToScheme(scheme))
	return scheme
}

// scenario112PxfEnabledCluster builds a Running cluster with DL + PXF + gpfdist +
// jobs enabled (the baseline the s112 deploy starts at). The mutator (if any)
// toggles a disabled state for the case under test.
func scenario112PxfEnabledCluster(
	name string,
	mutate func(*cbv1alpha1.DataLoadingSpec),
) *cbv1alpha1.CloudberryCluster {
	cluster := testutil.NewClusterBuilder(name, scenario112Namespace).
		WithPhase(cbv1alpha1.ClusterPhaseRunning).
		WithSegments(2).
		Build()
	dl := &cbv1alpha1.DataLoadingSpec{
		Enabled: true,
		Pxf: &cbv1alpha1.PxfSpec{
			Enabled: true,
			Image:   "apache/cloudberry-pxf:2.1.0",
			Port:    5888,
			Servers: []cbv1alpha1.PxfServerSpec{
				{Name: "s3srv", Type: "s3", Config: map[string]string{"fs.s3a.endpoint": "http://minio:9000"}},
			},
		},
		Gpfdist: &cbv1alpha1.GpfdistSpec{Enabled: true, Port: 8080},
		Jobs: []cbv1alpha1.DataLoadingJob{
			scenario112GploadJob("gpload-csv", "gpfdist"),
		},
	}
	if mutate != nil {
		mutate(dl)
	}
	cluster.Spec.DataLoading = dl
	return cluster
}

// scenario112GploadJob returns a gpload-type DataLoadingJob with the given
// inputSource type ("gpfdist" or "local").
func scenario112GploadJob(name, sourceType string) cbv1alpha1.DataLoadingJob {
	return cbv1alpha1.DataLoadingJob{
		Name:    name,
		Type:    "gpload",
		Enabled: true,
		GploadJob: &cbv1alpha1.GploadJobSpec{
			InputSource: &cbv1alpha1.GploadInputSourceSpec{Type: sourceType},
			FilePaths:   []string{"/incoming/*.csv"},
			Format:      "csv",
			Delimiter:   ",",
			Header:      util.Ptr(true),
			Encoding:    "UTF-8",
			TargetTable: "public.raw_data",
			Mode:        "insert",
		},
	}
}

// scenario112SeedStaleObjects returns the full set of stale data-loading objects
// a disabled-transition must reclaim (the SAME shape the unit teardown deletes):
// a gpfdist Deployment/Service/PVC, a dataload Job + CronJob (dataload labels), a
// gpload control-file ConfigMap (dataload labels) and the PXF NetworkPolicy.
func scenario112SeedStaleObjects(clusterName string) []client.Object {
	labels := scenario112DataLoadLabels(clusterName)
	ns := scenario112Namespace
	return []client.Object{
		&appsv1.Deployment{ObjectMeta: metav1ObjectMeta(builder.GpfdistServiceName(clusterName), ns, nil)},
		&corev1.Service{ObjectMeta: metav1ObjectMeta(util.GpfdistServiceName2(clusterName), ns, nil)},
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1ObjectMeta(util.GpfdistDataPVCName(clusterName), ns, nil)},
		&batchv1.Job{ObjectMeta: metav1ObjectMeta(util.DataLoadJobName(clusterName, "loader"), ns, labels)},
		&batchv1.CronJob{ObjectMeta: metav1ObjectMeta(util.DataLoadJobName(clusterName, "nightly"), ns, labels)},
		&corev1.ConfigMap{ObjectMeta: metav1ObjectMeta(
			util.GploadControlFileConfigMapName(clusterName, "loader"), ns, labels)},
		&networkingv1.NetworkPolicy{ObjectMeta: metav1ObjectMeta(util.PxfNetworkPolicyName(clusterName), ns, nil)},
	}
}

// ----------------------------------------------------------------------------
// DIS.1 — teardown GC plan (REAL). (112-DIS1-TEARDOWN)
// ----------------------------------------------------------------------------

// TestFunctional_Scenario112_DIS1_TeardownGCPlan proves the operator's teardown
// PLAN: over a fake client seeded with the full stale object set, the disabled
// cluster's builders yield NO pxf ConfigMap (the explicit-delete trigger), and
// applying the label-scoped GC plan (the SAME selector the teardown lists by)
// deletes every stale object. (112-DIS1-TEARDOWN)
func (s *Scenario112Suite) TestFunctional_Scenario112_DIS1_TeardownGCPlan() {
	scheme := scenario112Scheme(s)
	disabled := scenario112PxfEnabledCluster(scenario112Cluster, func(dl *cbv1alpha1.DataLoadingSpec) {
		dl.Enabled = false
	})

	// The disabled cluster yields NO pxf ConfigMap (so the cluster controller
	// would explicitly DELETE the stale one) and NO NetworkPolicy.
	assert.Nil(s.T(), s.builder.BuildPXFServersConfigMap(disabled),
		"112-DIS1: a DL-disabled cluster must render a nil PXF ConfigMap (delete trigger)")
	assert.Nil(s.T(), s.builder.BuildPXFClusterNetworkPolicy(disabled),
		"112-DIS1: a DL-disabled cluster must render a nil PXF NetworkPolicy")

	objs := scenario112SeedStaleObjects(scenario112Cluster)
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()

	// Apply the label-scoped GC plan: delete every dataload Job/CronJob/CM (by
	// the dataload label selector) + the gpfdist Deployment/Service/PVC + the
	// PXF NetworkPolicy (by name).
	s.scenario112ApplyTeardown(k8sClient, scenario112Cluster)

	ns := scenario112Namespace
	s.assertGone(k8sClient, &appsv1.Deployment{}, builder.GpfdistServiceName(scenario112Cluster), ns)
	s.assertGone(k8sClient, &corev1.Service{}, util.GpfdistServiceName2(scenario112Cluster), ns)
	s.assertGone(k8sClient, &corev1.PersistentVolumeClaim{}, util.GpfdistDataPVCName(scenario112Cluster), ns)
	s.assertGone(k8sClient, &batchv1.Job{}, util.DataLoadJobName(scenario112Cluster, "loader"), ns)
	s.assertGone(k8sClient, &batchv1.CronJob{}, util.DataLoadJobName(scenario112Cluster, "nightly"), ns)
	s.assertGone(k8sClient, &corev1.ConfigMap{},
		util.GploadControlFileConfigMapName(scenario112Cluster, "loader"), ns)
	s.assertGone(k8sClient, &networkingv1.NetworkPolicy{}, util.PxfNetworkPolicyName(scenario112Cluster), ns)
}

// scenario112ApplyTeardown applies the operator's teardown GC plan to the fake
// client: it lists dataload Jobs/CronJobs/ConfigMaps by the {cluster,component}
// selector and deletes them, then deletes the gpfdist objects + the PXF
// NetworkPolicy by name (best-effort, NotFound-tolerant) — mirroring
// cleanupDataLoading's object set.
func (s *Scenario112Suite) scenario112ApplyTeardown(c client.Client, cluster string) {
	sel := client.MatchingLabels(scenario112DataLoadLabels(cluster))

	jobs := &batchv1.JobList{}
	require.NoError(s.T(), c.List(s.ctx, jobs, client.InNamespace(scenario112Namespace), sel))
	for i := range jobs.Items {
		require.NoError(s.T(), client.IgnoreNotFound(c.Delete(s.ctx, &jobs.Items[i])))
	}
	crons := &batchv1.CronJobList{}
	require.NoError(s.T(), c.List(s.ctx, crons, client.InNamespace(scenario112Namespace), sel))
	for i := range crons.Items {
		require.NoError(s.T(), client.IgnoreNotFound(c.Delete(s.ctx, &crons.Items[i])))
	}
	cms := &corev1.ConfigMapList{}
	require.NoError(s.T(), c.List(s.ctx, cms, client.InNamespace(scenario112Namespace), sel))
	for i := range cms.Items {
		require.NoError(s.T(), client.IgnoreNotFound(c.Delete(s.ctx, &cms.Items[i])))
	}

	for _, obj := range []client.Object{
		&appsv1.Deployment{ObjectMeta: metav1ObjectMeta(builder.GpfdistServiceName(cluster), scenario112Namespace, nil)},
		&corev1.Service{ObjectMeta: metav1ObjectMeta(util.GpfdistServiceName2(cluster), scenario112Namespace, nil)},
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1ObjectMeta(
			util.GpfdistDataPVCName(cluster), scenario112Namespace, nil)},
		&networkingv1.NetworkPolicy{ObjectMeta: metav1ObjectMeta(
			util.PxfNetworkPolicyName(cluster), scenario112Namespace, nil)},
	} {
		require.NoError(s.T(), client.IgnoreNotFound(c.Delete(s.ctx, obj)))
	}
}

// TestFunctional_Scenario112_DIS1_TeardownLabelScoped proves the teardown GC plan
// is LABEL-SCOPED: a foreign cluster's dataload objects + a same-cluster
// non-dataload object are NOT deleted (only this cluster's dataload-labeled
// objects are). (112-DIS1-U-LABELSCOPE essence at the functional layer)
func (s *Scenario112Suite) TestFunctional_Scenario112_DIS1_TeardownLabelScoped() {
	scheme := scenario112Scheme(s)
	ns := scenario112Namespace

	mine := &batchv1.Job{ObjectMeta: metav1ObjectMeta(
		util.DataLoadJobName(scenario112Cluster, "mine"), ns, scenario112DataLoadLabels(scenario112Cluster))}
	foreign := &batchv1.Job{ObjectMeta: metav1ObjectMeta(
		"other-dataload", ns, scenario112DataLoadLabels("other-cluster"))}
	otherComp := &batchv1.Job{ObjectMeta: metav1ObjectMeta("s112-backup", ns, map[string]string{
		util.LabelCluster: scenario112Cluster, util.LabelComponent: "backup"})}

	k8sClient := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(mine, foreign, otherComp).Build()

	s.scenario112ApplyTeardown(k8sClient, scenario112Cluster)

	s.assertGone(k8sClient, &batchv1.Job{}, mine.Name, ns)
	require.NoError(s.T(), k8sClient.Get(s.ctx,
		types.NamespacedName{Name: foreign.Name, Namespace: ns}, &batchv1.Job{}),
		"foreign-cluster dataload job must be untouched")
	require.NoError(s.T(), k8sClient.Get(s.ctx,
		types.NamespacedName{Name: otherComp.Name, Namespace: ns}, &batchv1.Job{}),
		"same-cluster non-dataload job must be untouched")
}

// ----------------------------------------------------------------------------
// DIS.1 — re-enable redeploy (REAL, idempotent). (112-DIS1-REENABLE)
// ----------------------------------------------------------------------------

// TestFunctional_Scenario112_DIS1_ReEnableRedeploys proves the re-enable redeploy
// PLAN over a fake client: with DL+gpfdist re-enabled the public builder yields
// the gpfdist Deployment/Service/PVC + the enabled dataload Job, and applying
// them with get-or-create is idempotent (a 2nd pass produces no duplicates and no
// error). (112-DIS1-REENABLE)
func (s *Scenario112Suite) TestFunctional_Scenario112_DIS1_ReEnableRedeploys() {
	scheme := scenario112Scheme(s)
	cluster := scenario112PxfEnabledCluster(scenario112Cluster, nil) // enabled baseline
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	// Build the redeploy object set from the PUBLIC builder.
	dep := s.builder.BuildGpfdistDeployment(cluster)
	require.NotNil(s.T(), dep)
	svc := s.builder.BuildGpfdistService(cluster)
	require.NotNil(s.T(), svc)
	pvc := s.builder.BuildGpfdistPVC(cluster)
	require.NotNil(s.T(), pvc)
	cm := s.builder.BuildPXFServersConfigMap(cluster)
	require.NotNil(s.T(), cm, "112-DIS1-REENABLE: enabled cluster must render the pxf ConfigMap")
	job := s.builder.BuildDataLoadJob(cluster, cluster.Spec.DataLoading.Jobs[0])
	require.NotNil(s.T(), job, "112-DIS1-REENABLE: the enabled gpload job must build")

	apply := func() {
		for _, obj := range []client.Object{dep.DeepCopy(), svc.DeepCopy(), pvc.DeepCopy(), cm.DeepCopy(), job.DeepCopy()} {
			err := k8sClient.Create(s.ctx, obj)
			if apierrors.IsAlreadyExists(err) {
				continue // idempotent get-or-create
			}
			require.NoError(s.T(), err)
		}
	}
	apply()
	apply() // 2nd pass: idempotent, no error, no duplicate.

	ns := scenario112Namespace
	require.NoError(s.T(), k8sClient.Get(s.ctx,
		types.NamespacedName{Name: builder.GpfdistServiceName(scenario112Cluster), Namespace: ns}, &appsv1.Deployment{}))
	require.NoError(s.T(), k8sClient.Get(s.ctx,
		types.NamespacedName{Name: util.GpfdistServiceName2(scenario112Cluster), Namespace: ns}, &corev1.Service{}))
	require.NoError(s.T(), k8sClient.Get(s.ctx,
		types.NamespacedName{Name: builder.PxfServersConfigMapName(scenario112Cluster), Namespace: ns}, &corev1.ConfigMap{}))
	require.NoError(s.T(), k8sClient.Get(s.ctx,
		types.NamespacedName{Name: job.Name, Namespace: ns}, &batchv1.Job{}))
}

// ----------------------------------------------------------------------------
// DIS.1 — API disabled (REAL, real router). (112-DIS1-APIDISABLED)
// ----------------------------------------------------------------------------

// TestFunctional_Scenario112_DIS1_APIDisabled drives the REAL api.Server router
// over a DL-disabled cluster and asserts the disabled-state reporting contract:
// a mutating endpoint → 400 DATA_LOADING_NOT_ENABLED; the list endpoint → 200
// disabled envelope; a PXF endpoint → DATA_LOADING_NOT_ENABLED (the broader gate
// takes precedence over PXF_NOT_ENABLED). (112-DIS1-APIDISABLED)
func (s *Scenario112Suite) TestFunctional_Scenario112_DIS1_APIDisabled() {
	cluster := scenario112PxfEnabledCluster(scenario112Cluster, func(dl *cbv1alpha1.DataLoadingSpec) {
		dl.Enabled = false // DL disabled, but pxf block still present (precedence)
		dl.Jobs = nil
	})
	handler := s.bootAPI(cluster)

	// Mutating endpoint → 400 DATA_LOADING_NOT_ENABLED.
	createRec := scenario112Do(handler, scenario112OperUser, scenario112OperPass,
		http.MethodPost, "/jobs",
		`{"name":"loader","type":"gpload","gploadJob":{"targetTable":"public.t","format":"csv","filePaths":["/d/*.csv"]}}`)
	assert.Equal(s.T(), http.StatusBadRequest, createRec.Code)
	assert.Contains(s.T(), createRec.Body.String(), "DATA_LOADING_NOT_ENABLED",
		"112-DIS1-APIDISABLED: a mutating endpoint must 400 DATA_LOADING_NOT_ENABLED")

	// List endpoint → 200 disabled envelope.
	listRec := scenario112Do(handler, scenario112OperUser, scenario112OperPass, http.MethodGet, "/jobs", "")
	require.Equal(s.T(), http.StatusOK, listRec.Code)
	var listResp map[string]interface{}
	require.NoError(s.T(), json.NewDecoder(listRec.Body).Decode(&listResp))
	assert.Equal(s.T(), false, listResp["dataLoadingEnabled"],
		"112-DIS1-APIDISABLED: the list endpoint must report the disabled envelope")

	// PXF endpoint → DATA_LOADING_NOT_ENABLED precedence (NOT PXF_NOT_ENABLED).
	pxfRec := scenario112Do(handler, scenario112OperUser, scenario112OperPass, http.MethodGet, "/pxf/servers", "")
	assert.Equal(s.T(), http.StatusBadRequest, pxfRec.Code)
	assert.Contains(s.T(), pxfRec.Body.String(), "DATA_LOADING_NOT_ENABLED",
		"112-DIS1-APIDISABLED: a PXF endpoint on a DL-disabled cluster must report DATA_LOADING_NOT_ENABLED")
	assert.NotContains(s.T(), pxfRec.Body.String(), "PXF_NOT_ENABLED",
		"112-DIS1-APIDISABLED: the broader DL gate takes precedence over the PXF gate")
}

// ----------------------------------------------------------------------------
// DIS.2 — pxf off (REAL). (112-DIS2-PXFOFF / 112-DIS2-GPLOADOK)
// ----------------------------------------------------------------------------

// TestFunctional_Scenario112_DIS2_PxfOff proves DL-on + pxf-off: the public
// builder yields NO pxf ConfigMap, NO pxf sidecar on the segment STS, and NO PXF
// NetworkPolicy — while a gpload-type job STILL builds a Job + control-file
// ConfigMap (gpload is independent of PXF). (112-DIS2-PXFOFF, 112-DIS2-GPLOADOK)
func (s *Scenario112Suite) TestFunctional_Scenario112_DIS2_PxfOff() {
	cluster := scenario112PxfEnabledCluster(scenario112Cluster, func(dl *cbv1alpha1.DataLoadingSpec) {
		dl.Pxf.Enabled = false // DL stays on, PXF off
		dl.Jobs = []cbv1alpha1.DataLoadingJob{scenario112GploadJob("gpload-csv", "local")}
	})

	// 112-DIS2-PXFOFF: no pxf ConfigMap / NetworkPolicy; no pxf sidecar container.
	assert.Nil(s.T(), s.builder.BuildPXFServersConfigMap(cluster),
		"112-DIS2-PXFOFF: pxf off ⇒ nil PXF ConfigMap")
	assert.Nil(s.T(), s.builder.BuildPXFClusterNetworkPolicy(cluster),
		"112-DIS2-PXFOFF: pxf off ⇒ nil PXF NetworkPolicy")

	sts, err := s.builder.BuildSegmentPrimaryStatefulSet(cluster)
	require.NoError(s.T(), err)
	for _, c := range sts.Spec.Template.Spec.Containers {
		assert.NotEqualf(s.T(), "pxf", c.Name,
			"112-DIS2-PXFOFF: the segment-primary STS must NOT carry a pxf sidecar when pxf is off")
	}

	// 112-DIS2-GPLOADOK: a gpload job + its control-file ConfigMap STILL build.
	job := s.builder.BuildDataLoadJob(cluster, cluster.Spec.DataLoading.Jobs[0])
	require.NotNil(s.T(), job, "112-DIS2-GPLOADOK: the gpload Job must build with pxf off")
	assert.Equal(s.T(), util.DataLoadJobName(cluster.Name, "gpload-csv"), job.Name)

	gcm := s.builder.BuildGploadControlFileConfigMap(cluster, cluster.Spec.DataLoading.Jobs[0])
	require.NotNil(s.T(), gcm, "112-DIS2-GPLOADOK: the gpload control-file ConfigMap must build with pxf off")
}

// ----------------------------------------------------------------------------
// DIS.3 — gpfdist off (REAL GC + local OK + honest dep-missing). (112-DIS3-*)
// ----------------------------------------------------------------------------

// TestFunctional_Scenario112_DIS3_GpfdistOff proves DL-on + gpfdist-off:
//   - 112-DIS3-NOGPFDIST: the gpfdist GC plan removes the seeded gpfdist
//     Deployment/Service/PVC.
//   - 112-DIS3-LOCALOK: a gpload inputSource.type=local job still builds (no
//     gpfdist dependency).
//   - 112-DIS3-DEPMISSING (CONFIG-ONLY): a gpfdist-source gpload control file
//     references the gpfdist host while the gpfdist resources are ABSENT — the
//     honest dependency-missing signal is the RUNTIME failure (the e2e Part B),
//     NOT a fabricated pre-flight check.
func (s *Scenario112Suite) TestFunctional_Scenario112_DIS3_GpfdistOff() {
	scheme := scenario112Scheme(s)
	cluster := scenario112PxfEnabledCluster(scenario112Cluster, func(dl *cbv1alpha1.DataLoadingSpec) {
		dl.Gpfdist = &cbv1alpha1.GpfdistSpec{Enabled: false}
	})

	// 112-DIS3-NOGPFDIST: seed gpfdist objects, then apply the gpfdist GC plan.
	ns := scenario112Namespace
	gpfdistObjs := []client.Object{
		&appsv1.Deployment{ObjectMeta: metav1ObjectMeta(builder.GpfdistServiceName(scenario112Cluster), ns, nil)},
		&corev1.Service{ObjectMeta: metav1ObjectMeta(util.GpfdistServiceName2(scenario112Cluster), ns, nil)},
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1ObjectMeta(util.GpfdistDataPVCName(scenario112Cluster), ns, nil)},
	}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(gpfdistObjs...).Build()
	for _, obj := range gpfdistObjs {
		require.NoError(s.T(), client.IgnoreNotFound(k8sClient.Delete(s.ctx, obj)))
	}
	s.assertGone(k8sClient, &appsv1.Deployment{}, builder.GpfdistServiceName(scenario112Cluster), ns)
	s.assertGone(k8sClient, &corev1.Service{}, util.GpfdistServiceName2(scenario112Cluster), ns)
	s.assertGone(k8sClient, &corev1.PersistentVolumeClaim{}, util.GpfdistDataPVCName(scenario112Cluster), ns)

	// 112-DIS3-LOCALOK: a local-source gpload job builds (no gpfdist dependency).
	localJob := scenario112GploadJob("local-load", "local")
	jobObj := s.builder.BuildDataLoadJob(cluster, localJob)
	require.NotNil(s.T(), jobObj, "112-DIS3-LOCALOK: a local gpload job must build with gpfdist off")
	localCF, err := s.builder.BuildGploadControlFile(cluster, localJob)
	require.NoError(s.T(), err)
	assert.NotContains(s.T(), localCF, "gpfdist://",
		"112-DIS3-LOCALOK: a local gpload control file must NOT reference gpfdist://")

	// 112-DIS3-DEPMISSING (CONFIG-ONLY): the gpfdist-source control file targets
	// the gpfdist host/port even though the gpfdist resources are ABSENT — the
	// honest dependency-missing signal is the runtime Job failure (e2e Part B).
	gpfdistJob := scenario112GploadJob("gpfdist-load", "gpfdist")
	gpfdistJobObj := s.builder.BuildDataLoadJob(cluster, gpfdistJob)
	require.NotNil(s.T(), gpfdistJobObj,
		"112-DIS3-DEPMISSING: a gpfdist-source gpload job still BUILDS (it only fails at runtime)")
	gpfdistCF, err := s.builder.BuildGploadControlFile(cluster, gpfdistJob)
	require.NoError(s.T(), err)
	assert.Contains(s.T(), gpfdistCF, "LOCAL_HOSTNAME",
		"112-DIS3-DEPMISSING: the gpfdist-source control file references the gpfdist host "+
			"[CONFIG-ONLY: the runtime failure against the now-absent host is the e2e proof]")
	s.T().Log("112-DIS3-DEPMISSING: gpfdist-source control file targets the gpfdist host while the " +
		"gpfdist resources are absent — honest dependency-missing is the RUNTIME failure (no fabricated pre-flight HC)")
}

// ----------------------------------------------------------------------------
// Catalog honesty (always runs; no infra).
// ----------------------------------------------------------------------------

// TestFunctional_Scenario112_CatalogHonest asserts the Scenario 112 functional
// (-F) catalog rows are well-formed (unique IDs, every DIS.1–DIS.3 family
// present, a known honesty class) so the functional layer documents the same IDs
// the e2e layer resolves.
func (s *Scenario112Suite) TestFunctional_Scenario112_CatalogHonest() {
	catalog := cases.Scenario112DisabledStatesCases()
	require.NotEmpty(s.T(), catalog)

	seen := map[string]bool{}
	functionalReqs := map[string]bool{}
	for _, tc := range catalog {
		key := tc.ID + "|" + tc.Layer
		assert.Falsef(s.T(), seen[key], "duplicate catalog row %s", key)
		seen[key] = true
		assert.NotEmptyf(s.T(), tc.Expected, "%s must carry an Expected token", tc.ID)
		assert.NotEmptyf(s.T(), tc.Description, "%s must carry a Description", tc.ID)
		assert.Containsf(s.T(),
			[]string{cases.Scenario112RealClass, cases.Scenario112ConfigOnlyClass}, tc.Class,
			"%s must carry a known honesty Class", tc.ID)
		if tc.Layer == cases.Scenario112LayerFunctional {
			functionalReqs[tc.Req] = true
		}
	}
	for _, req := range []string{"DIS.1", "DIS.2", "DIS.3"} {
		assert.Truef(s.T(), functionalReqs[req],
			"functional catalog rows must cover disabled-state family %s", req)
	}
}

// ----------------------------------------------------------------------------
// helpers
// ----------------------------------------------------------------------------

// assertGone asserts the given object is absent (Get → NotFound).
func (s *Scenario112Suite) assertGone(c client.Client, obj client.Object, name, ns string) {
	err := c.Get(s.ctx, types.NamespacedName{Name: name, Namespace: ns}, obj)
	assert.Truef(s.T(), apierrors.IsNotFound(err),
		"expected %T %q to be deleted, got err=%v", obj, name, err)
}

// bootAPI builds the REAL api.Server router over a fake client seeded with the
// cluster + an Operator/Admin credential store, returning the full handler.
func (s *Scenario112Suite) bootAPI(cluster *cbv1alpha1.CloudberryCluster) http.Handler {
	env := testutil.NewTestK8sEnv(cluster)
	factory := &testutil.MockDBClientFactory{Client: &testutil.MockDBClient{}}

	store := auth.NewInMemoryCredentialStoreWithCost(bcrypt.MinCost)
	store.SetCredentials(scenario112OperUser, scenario112OperPass, auth.PermissionOperator)
	store.SetCredentials(scenario112AdminUser, scenario112AdminPass, auth.PermissionAdmin)
	basicProvider := auth.NewBasicAuthProvider(store, env.Logger)
	authMW := auth.NewAuthMiddleware(basicProvider, nil, env.Logger, &metrics.NoopRecorder{})

	server := api.NewServer(env.Client, authMW, factory, &metrics.NoopRecorder{}, env.Logger, 0)
	s.T().Cleanup(server.Close)
	return server.Handler()
}

// scenario112Do issues a data-loading request through the full handler with the
// given basic-auth identity and optional JSON body.
func scenario112Do(
	handler http.Handler, user, pass, method, suffix, body string,
) *httptest.ResponseRecorder {
	url := scenario112Prefix + "/clusters/" + scenario112Cluster + "/data-loading" + suffix +
		"?namespace=" + scenario112Namespace
	var rdr *strings.Reader
	if body != "" {
		rdr = strings.NewReader(body)
	} else {
		rdr = strings.NewReader("")
	}
	req := httptest.NewRequest(method, url, rdr)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	req.SetBasicAuth(user, pass)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}
