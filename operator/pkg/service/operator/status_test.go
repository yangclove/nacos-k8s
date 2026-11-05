package operator

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	nacosv1 "nacos.io/nacos-operator/api/v1alpha1"
	myErrors "nacos.io/nacos-operator/pkg/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type statusCountingClient struct {
	client.Client
	updates   int
	updateErr error
}

type statusCountingWriter struct {
	client.StatusWriter
	owner *statusCountingClient
}

func (c *statusCountingClient) Status() client.StatusWriter {
	return &statusCountingWriter{StatusWriter: c.Client.Status(), owner: c}
}

func (w *statusCountingWriter) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	w.owner.updates++
	if w.owner.updateErr != nil {
		return w.owner.updateErr
	}
	return w.StatusWriter.Update(ctx, obj, opts...)
}

func statusFixture(t *testing.T) (*StatusClient, *statusCountingClient, *nacosv1.Nacos) {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := nacosv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	cr := &nacosv1.Nacos{ObjectMeta: metav1.ObjectMeta{Name: "nacos", Namespace: "base"}}
	cl := &statusCountingClient{Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(cr).Build()}
	stored := &nacosv1.Nacos{}
	if err := cl.Get(context.Background(), types.NamespacedName{Name: cr.Name, Namespace: cr.Namespace}, stored); err != nil {
		t.Fatal(err)
	}
	return NewStatusClient(logr.Discard(), nil, cl), cl, stored
}

func TestStatusRepeatedFailureDoesNotWriteOrChangeTime(t *testing.T) {
	sc, cl, cr := statusFixture(t)
	failure := &myErrors.Err{Code: myErrors.CODE_CLUSTER_FAILE, Msg: "node unavailable"}
	sc.UpdateExceptionStatus(cr, failure, cr.Status.DeepCopy())
	first := cr.Status.DeepCopy()
	time.Sleep(time.Millisecond)
	sc.UpdateExceptionStatus(cr, failure, cr.Status.DeepCopy())
	if cl.updates != 1 {
		t.Errorf("duplicate failure wrote status %d times, want 1", cl.updates)
	}
	if !first.Event[0].LastTransitionTime.Equal(&cr.Status.Event[0].LastTransitionTime) {
		t.Error("duplicate failure changed timestamp")
	}
}

func TestStatusFirstRunningEventIsSuccessful(t *testing.T) {
	sc, _, cr := statusFixture(t)
	sc.UpdateStatusRunning(cr, cr.Status.DeepCopy())
	if cr.Status.Phase != nacosv1.PhaseRunning || len(cr.Status.Event) != 1 || !cr.Status.Event[0].Status {
		t.Fatalf("first Running event not successful: %+v", cr.Status)
	}
}

func TestStatusCompleteSnapshotPreservesConditionsAndVersionChanges(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*nacosv1.Nacos)
	}{
		{"conditions", func(cr *nacosv1.Nacos) { cr.Status.Conditions[0].Instance = "10.0.0.9" }},
		{"version", func(cr *nacosv1.Nacos) { cr.Status.Version = "2.4.4" }},
		{"event", func(cr *nacosv1.Nacos) { cr.Status.Event[0].Message = "changed" }},
		{"phase", func(cr *nacosv1.Nacos) { cr.Status.Phase = nacosv1.PhaseFailed }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sc, cl, cr := statusFixture(t)
			cr.Status.Conditions = []nacosv1.Condition{{PodName: "nacos-0", Instance: "10.0.0.1"}}
			cr.Status.Version = "2.4.3"
			sc.UpdateStatusRunning(cr, cr.Status.DeepCopy())
			before := cr.Status.DeepCopy()
			tc.mutate(cr)
			sc.UpdateStatus(cr, before)
			if cl.updates != 2 {
				t.Fatalf("changed %s did not write complete status: %d updates", tc.name, cl.updates)
			}
			stored := &nacosv1.Nacos{}
			if err := cl.Get(context.Background(), client.ObjectKeyFromObject(cr), stored); err != nil {
				t.Fatal(err)
			}
			// metav1.Time is serialized at second precision by the API; compare wire values.
			actual, err := json.Marshal(stored.Status)
			if err != nil {
				t.Fatal(err)
			}
			expected, err := json.Marshal(cr.Status)
			if err != nil {
				t.Fatal(err)
			}
			if string(actual) != string(expected) {
				t.Errorf("stored status differs: got %s want %s", actual, expected)
			}
		})
	}
}

func TestStatusSameCodeChangedMessageAndRecoveryArePersisted(t *testing.T) {
	sc, cl, cr := statusFixture(t)
	sc.UpdateExceptionStatus(cr, &myErrors.Err{Code: 500, Msg: "first error"}, cr.Status.DeepCopy())
	before := cr.Status.DeepCopy()
	sc.UpdateExceptionStatus(cr, &myErrors.Err{Code: 500, Msg: "second error"}, before)
	if cl.updates != 2 || len(cr.Status.Event) != 1 || cr.Status.Event[0].Message != "second error" {
		t.Fatalf("message transition lost: %+v updates=%d", cr.Status, cl.updates)
	}
	if before.Event[0].Message != "first error" {
		t.Error("snapshot aliases mutable event slice")
	}
	sc.UpdateStatusRunning(cr, cr.Status.DeepCopy())
	if cl.updates != 3 || cr.Status.Phase != nacosv1.PhaseRunning || len(cr.Status.Event) != 2 || !cr.Status.Event[1].Status {
		t.Fatalf("recovery lost: %+v updates=%d", cr.Status, cl.updates)
	}
}

func TestStatusStableConditionOrderingAndSnapshotIsolation(t *testing.T) {
	sc, cl, cr := statusFixture(t)
	cr.Status.Conditions = []nacosv1.Condition{{PodName: "nacos-1"}, {PodName: "nacos-0"}}
	before := cr.Status.DeepCopy()
	sc.UpdateStatusRunning(cr, before)
	if before.Conditions[0].PodName != "nacos-1" {
		t.Fatal("sorting mutated pre-reconciliation snapshot")
	}
	if cr.Status.Conditions[0].PodName != "nacos-0" {
		t.Fatal("conditions not sorted by PodName")
	}
	before = cr.Status.DeepCopy()
	cr.Status.Conditions[0], cr.Status.Conditions[1] = cr.Status.Conditions[1], cr.Status.Conditions[0]
	sc.UpdateStatusRunning(cr, before)
	if cl.updates != 1 {
		t.Fatalf("stable conditions wrote %d times, want 1", cl.updates)
	}
}

func TestStatusHistoryRemainsBoundedAndFirstAppearTimeIsRetained(t *testing.T) {
	sc, _, cr := statusFixture(t)
	for code := 1; code <= EVENT_MAX_SIZE+3; code++ {
		sc.UpdateExceptionStatus(cr, &myErrors.Err{Code: code, Msg: "error"}, cr.Status.DeepCopy())
	}
	if len(cr.Status.Event) != EVENT_MAX_SIZE || cr.Status.Event[0].Code != 4 {
		t.Fatalf("history not bounded: %+v", cr.Status.Event)
	}
	first := cr.Status.Event[len(cr.Status.Event)-1].FirstAppearTime
	sc.UpdateExceptionStatus(cr, &myErrors.Err{Code: EVENT_MAX_SIZE + 3, Msg: "changed"}, cr.Status.DeepCopy())
	if !first.Equal(&cr.Status.Event[len(cr.Status.Event)-1].FirstAppearTime) {
		t.Error("same-code message update changed first-appear time")
	}
}

func TestStatusWriteFailurePropagatesToReconciliation(t *testing.T) {
	sc, cl, cr := statusFixture(t)
	expected := errors.New("status update conflict")
	cl.updateErr = expected
	defer func() {
		if got := recover(); got != expected {
			t.Errorf("write error not propagated: %v", got)
		}
	}()
	sc.UpdateExceptionStatus(cr, &myErrors.Err{Code: 500, Msg: "error"}, cr.Status.DeepCopy())
}
