package controllers

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	kubefake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	nacosv1 "nacos.io/nacos-operator/api/v1alpha1"
	myErrors "nacos.io/nacos-operator/pkg/errors"
	k8sservice "nacos.io/nacos-operator/pkg/service/k8s"
	"nacos.io/nacos-operator/pkg/service/operator"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	configv1 "sigs.k8s.io/controller-runtime/pkg/config/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

type healthCountingClient struct {
	client.Client
	updates   int
	getErr    error
	statusErr error
}

type healthStatusWriter struct {
	client.StatusWriter
	owner *healthCountingClient
}

func (c *healthCountingClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object) error {
	if c.getErr != nil {
		return c.getErr
	}
	return c.Client.Get(ctx, key, obj)
}

func (c *healthCountingClient) Status() client.StatusWriter {
	return &healthStatusWriter{StatusWriter: c.Client.Status(), owner: c}
}

func (w *healthStatusWriter) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	w.owner.updates++
	if w.owner.statusErr != nil {
		return w.owner.statusErr
	}
	return w.StatusWriter.Update(ctx, obj, opts...)
}

type healthRoundTripper func(*http.Request) (*http.Response, error)

func (f healthRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func healthFixture(t *testing.T, phase nacosv1.Phase, ready bool) (*NacosReconciler, *healthCountingClient, *kubefake.Clientset, ctrl.Request, *int) {
	t.Helper()
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{nacosv1.AddToScheme, corev1.AddToScheme, appsv1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	replicas := int32(4)
	cr := &nacosv1.Nacos{
		ObjectMeta: metav1.ObjectMeta{Name: "nacos", Namespace: "base", UID: types.UID("nacos-id"), CreationTimestamp: metav1.NewTime(time.Unix(1, 0))},
		Spec:       nacosv1.NacosSpec{Type: "cluster", Replicas: &replicas, Image: "nacos/nacos-server:v2.4.3", Database: nacosv1.Database{TypeDatabase: "embedded"}},
		Status:     nacosv1.NacosStatus{Phase: phase},
	}
	cl := &healthCountingClient{Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(cr).Build()}
	objects := []runtime.Object{}
	for i := 0; i < 4; i++ {
		readyStatus := corev1.ConditionFalse
		if ready {
			readyStatus = corev1.ConditionTrue
		}
		objects = append(objects, &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("nacos-%d", i), Namespace: "base", Labels: map[string]string{"app": "nacos", "middleware": "nacos", "component": "nacos"}},
			Spec:       corev1.PodSpec{NodeName: fmt.Sprintf("worker-%d", i)},
			Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: fmt.Sprintf("10.0.0.%d", i+1), Conditions: []corev1.PodCondition{
				{Type: corev1.PodConditionType("PodReadyToStartContainers"), Status: corev1.ConditionTrue},
				{Type: corev1.PodInitialized, Status: corev1.ConditionTrue},
				{Type: corev1.PodReady, Status: readyStatus},
				{Type: corev1.ContainersReady, Status: readyStatus},
				{Type: corev1.PodScheduled, Status: corev1.ConditionTrue},
			}},
		})
	}
	kube := kubefake.NewSimpleClientset(objects...)
	services := k8sservice.NewK8sService(kube, logr.Discard())
	op := &operator.OperatorClient{
		KindClient:   operator.NewKindClient(logr.Discard(), services, scheme),
		CheckClient:  operator.NewCheckClient(logr.Discard(), services),
		HealClient:   operator.NewHealClient(logr.Discard(), services),
		StatusClient: operator.NewStatusClient(logr.Discard(), services, cl),
	}
	calls := new(int)
	originalTransport := http.DefaultTransport
	http.DefaultTransport = healthRoundTripper(func(req *http.Request) (*http.Response, error) {
		*calls++
		if req.URL.Path != "/nacos/v1/core/cluster/nodes" {
			t.Errorf("unexpected health path: %s", req.URL.Path)
		}
		nodes := []map[string]interface{}{}
		for i := 0; i < 4; i++ {
			nodes = append(nodes, map[string]interface{}{
				"ip": fmt.Sprintf("10.0.0.%d", i+1), "port": 8848, "state": "UP",
				"extendInfo": map[string]interface{}{
					"version":      "2.4.3",
					"raftMetaData": map[string]interface{}{"metaDataMap": map[string]interface{}{"naming_persistent_service": map[string]interface{}{"leader": "nacos-0.nacos-headless.base.svc.cluster.local:7848"}}},
				},
			})
		}
		body, err := json.Marshal(map[string]interface{}{"code": 200, "data": nodes})
		if err != nil {
			return nil, err
		}
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(string(body))), Request: req}, nil
	})
	t.Cleanup(func() { http.DefaultTransport = originalTransport })
	return &NacosReconciler{Client: cl, Scheme: scheme, Log: logr.Discard(), OperaterClient: op}, cl, kube,
		ctrl.Request{NamespacedName: types.NamespacedName{Name: "nacos", Namespace: "base"}}, calls
}

func healthStoredCR(t *testing.T, cl client.Client, req ctrl.Request) *nacosv1.Nacos {
	t.Helper()
	cr := &nacosv1.Nacos{}
	if err := cl.Get(context.Background(), req.NamespacedName, cr); err != nil {
		t.Fatal(err)
	}
	return cr
}

func TestHealthReconcileRecoversAndChecksEveryThirtySeconds(t *testing.T) {
	r, cl, kube, req, calls := healthFixture(t, nacosv1.PhaseFailed, true)
	result, err := r.Reconcile(context.Background(), req)
	if err != nil || result.RequeueAfter != 30*time.Second {
		t.Fatalf("healthy result=%+v err=%v", result, err)
	}
	cr := healthStoredCR(t, cl, req)
	if cr.Status.Phase != nacosv1.PhaseRunning || cr.Status.Version != "2.4.3" || len(cr.Status.Conditions) != 4 || len(cr.Status.Event) != 1 || !cr.Status.Event[0].Status {
		t.Fatalf("Failed did not recover fully: %+v", cr.Status)
	}
	if _, err := kube.AppsV1().StatefulSets("base").Get(context.Background(), "nacos", metav1.GetOptions{}); err != nil {
		t.Fatal(err)
	}
	result, err = r.Reconcile(context.Background(), req)
	if err != nil || result.RequeueAfter != 30*time.Second || cl.updates != 1 || *calls != 8 {
		t.Fatalf("stable healthy recheck result=%+v err=%v updates=%d HTTP calls=%d", result, err, cl.updates, *calls)
	}
}

func TestHealthReconcileRepeatedFailureDoesNotWriteAgain(t *testing.T) {
	r, cl, _, req, calls := healthFixture(t, nacosv1.PhaseRunning, false)
	for i := 0; i < 2; i++ {
		result, err := r.Reconcile(context.Background(), req)
		if err != nil || result.RequeueAfter != 5*time.Second {
			t.Fatalf("failed result=%+v err=%v", result, err)
		}
	}
	cr := healthStoredCR(t, cl, req)
	if cl.updates != 1 || cr.Status.Phase != nacosv1.PhaseFailed || *calls != 0 || len(cr.Status.Event) != 1 || cr.Status.Event[0].Status {
		t.Fatalf("repeated failure updates=%d HTTP calls=%d status=%+v", cl.updates, *calls, cr.Status)
	}
}

func TestHealthReconcileOrdinaryAPIErrorsRemainVisibleAndRetry(t *testing.T) {
	r, cl, kube, req, _ := healthFixture(t, nacosv1.PhaseRunning, true)
	kube.PrependReactor("get", "statefulsets", func(action k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "statefulsets"}, "nacos", fmt.Errorf("denied by RBAC"))
	})
	result, err := r.Reconcile(context.Background(), req)
	if err != nil || result.RequeueAfter != 5*time.Second {
		t.Fatalf("API failure result=%+v err=%v", result, err)
	}
	cr := healthStoredCR(t, cl, req)
	if cr.Status.Phase != nacosv1.PhaseFailed || len(cr.Status.Event) != 1 || cr.Status.Event[0].Code != myErrors.CODE_ERR_UNKNOW || !strings.Contains(cr.Status.Event[0].Message, "denied by RBAC") {
		t.Fatalf("ordinary API error was lost: %+v", cr.Status)
	}
}

func TestHealthReconcileInitialPhaseAndStatusWriteErrorsRetry(t *testing.T) {
	for _, failWrite := range []bool{false, true} {
		t.Run(fmt.Sprintf("statusWriteFailure=%v", failWrite), func(t *testing.T) {
			r, cl, _, req, _ := healthFixture(t, nacosv1.PhaseNone, false)
			if failWrite {
				cl.statusErr = apierrors.NewConflict(schema.GroupResource{Resource: "nacos"}, "nacos", fmt.Errorf("conflict"))
			}
			result, err := r.Reconcile(context.Background(), req)
			if err != nil || result.RequeueAfter != 5*time.Second {
				t.Fatalf("initializing result=%+v err=%v", result, err)
			}
			cr := healthStoredCR(t, cl, req)
			if !failWrite && cr.Status.Phase != nacosv1.PhaseCreating {
				t.Fatalf("initial status lost: %+v", cr.Status)
			}
		})
	}
}

func TestHealthReconcileReadErrorAndNotFound(t *testing.T) {
	r, cl, _, req, _ := healthFixture(t, nacosv1.PhaseRunning, true)
	cl.getErr = apierrors.NewForbidden(schema.GroupResource{Resource: "nacos"}, "nacos", fmt.Errorf("forbidden"))
	result, err := r.Reconcile(context.Background(), req)
	if err != nil || result.RequeueAfter != 5*time.Second {
		t.Fatalf("read error result=%+v err=%v", result, err)
	}
	cl.getErr = nil
	req.Name = "deleted"
	result, err = r.Reconcile(context.Background(), req)
	if err != nil || result.Requeue || result.RequeueAfter != 0 {
		t.Fatalf("NotFound requeued: result=%+v err=%v", result, err)
	}
}

type healthWatch struct {
	object     client.Object
	predicates []predicate.Predicate
}

type healthManager struct {
	manager.Manager
	scheme  *runtime.Scheme
	watches []healthWatch
}

func (m *healthManager) GetScheme() *runtime.Scheme { return m.scheme }
func (m *healthManager) GetLogger() logr.Logger     { return logr.Discard() }
func (m *healthManager) GetControllerOptions() configv1.ControllerConfigurationSpec {
	return configv1.ControllerConfigurationSpec{}
}
func (m *healthManager) Add(manager.Runnable) error { return nil }
func (m *healthManager) SetFields(value interface{}) error {
	if src, ok := value.(*source.Kind); ok {
		m.watches = append(m.watches, healthWatch{object: src.Type})
	}
	if pr, ok := value.(predicate.Predicate); ok {
		m.watches[len(m.watches)-1].predicates = append(m.watches[len(m.watches)-1].predicates, pr)
	}
	return nil
}

func TestHealthWatchFiltersOnlyNacosStatusUpdates(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := nacosv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	mgr := &healthManager{scheme: scheme}
	r := &NacosReconciler{Log: logr.Discard()}
	if err := r.SetupWithManager(mgr); err != nil {
		t.Fatal(err)
	}
	if len(mgr.watches) != 2 {
		t.Fatalf("got %d watches, want CR and StatefulSet", len(mgr.watches))
	}
	for _, watch := range mgr.watches {
		switch watch.object.(type) {
		case *nacosv1.Nacos:
			if len(watch.predicates) != 1 {
				t.Fatalf("Nacos predicates=%d want 1", len(watch.predicates))
			}
			old := &nacosv1.Nacos{ObjectMeta: metav1.ObjectMeta{Generation: 4}}
			current := old.DeepCopy()
			current.Status.Phase = nacosv1.PhaseRunning
			if watch.predicates[0].Update(event.UpdateEvent{ObjectOld: old, ObjectNew: current}) {
				t.Error("CR status-only update passed")
			}
			current.Generation++
			if !watch.predicates[0].Update(event.UpdateEvent{ObjectOld: old, ObjectNew: current}) {
				t.Error("CR spec update blocked")
			}
		case *appsv1.StatefulSet:
			old := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Generation: 4}}
			current := old.DeepCopy()
			current.Status.ReadyReplicas = 4
			for _, pr := range watch.predicates {
				if !pr.Update(event.UpdateEvent{ObjectOld: old, ObjectNew: current}) {
					t.Error("StatefulSet status update blocked")
				}
			}
		default:
			t.Errorf("unexpected watched object %T", watch.object)
		}
	}
}
