package k8s

import (
	"errors"
	"testing"

	log "github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

func TestGetStatefulSetReadPodConditions(t *testing.T) {
	ready := corev1.PodCondition{Type: corev1.PodReady, Status: corev1.ConditionTrue}
	scheduled := corev1.PodCondition{Type: corev1.PodScheduled, Status: corev1.ConditionTrue}
	initialized := corev1.PodCondition{Type: corev1.PodInitialized, Status: corev1.ConditionTrue}
	containers := corev1.PodCondition{Type: corev1.ContainersReady, Status: corev1.ConditionTrue}
	sandbox := corev1.PodCondition{Type: corev1.PodConditionType("PodReadyToStartContainers"), Status: corev1.ConditionTrue}
	tests := []struct {
		name       string
		conditions []corev1.PodCondition
		want       int
	}{
		{"four conditions", []corev1.PodCondition{initialized, ready, containers, scheduled}, 1},
		{"five conditions", []corev1.PodCondition{sandbox, initialized, ready, containers, scheduled}, 1},
		{"additional conditions", []corev1.PodCondition{sandbox, initialized, ready, containers, scheduled,
			{Type: "CustomReadyGate", Status: corev1.ConditionTrue}}, 1},
		{"reordered four conditions", []corev1.PodCondition{scheduled, initialized, containers, ready}, 1},
		{"only Ready condition", []corev1.PodCondition{ready}, 1},
		{"Ready false", []corev1.PodCondition{initialized, {Type: corev1.PodReady, Status: corev1.ConditionFalse}, containers, scheduled}, 0},
		{"Ready unknown", []corev1.PodCondition{initialized, {Type: corev1.PodReady, Status: corev1.ConditionUnknown}, containers, scheduled}, 0},
		{"missing Ready", []corev1.PodCondition{sandbox, initialized, containers, scheduled}, 0},
		{"no conditions", nil, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sts := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{Name: "nacos", Namespace: "test"},
				Spec:       appsv1.StatefulSetSpec{Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "nacos"}}},
			}
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "nacos-0", Namespace: "test", Labels: map[string]string{"app": "nacos"}},
				Status:     corev1.PodStatus{Conditions: tt.conditions},
			}
			unrelated := pod.DeepCopy()
			unrelated.Name = "other-0"
			unrelated.Labels = map[string]string{"app": "other"}
			service := NewStatefulSetService(fake.NewSimpleClientset(sts, pod, unrelated), log.Discard())
			pods, err := service.GetStatefulSetReadPod("test", "nacos")
			if err != nil {
				t.Fatal(err)
			}
			if len(pods) != tt.want {
				t.Fatalf("got %d Ready Pods, want %d", len(pods), tt.want)
			}
			if len(pods) != 0 && pods[0].Name != pod.Name {
				t.Fatalf("selected unrelated Pod: %s", pods[0].Name)
			}
		})
	}
}

func TestGetStatefulSetReadPodPreservesAPIErrors(t *testing.T) {
	for _, tt := range []struct{ name, verb, resource string }{
		{"get StatefulSet", "get", "statefulsets"},
		{"list Pods", "list", "pods"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			sentinel := errors.New("API unavailable")
			sts := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{Name: "nacos", Namespace: "test"},
				Spec:       appsv1.StatefulSetSpec{Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "nacos"}}},
			}
			client := fake.NewSimpleClientset(sts)
			client.PrependReactor(tt.verb, tt.resource, func(ktesting.Action) (bool, runtime.Object, error) {
				return true, nil, sentinel
			})
			service := NewStatefulSetService(client, log.Discard())
			pods, err := service.GetStatefulSetReadPod("test", "nacos")
			if !errors.Is(err, sentinel) {
				t.Fatalf("got error %v, want original API error", err)
			}
			if len(pods) != 0 {
				t.Fatalf("returned Pods after query failure: %v", pods)
			}
		})
	}
}
