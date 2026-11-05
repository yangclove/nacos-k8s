package operator

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	log "github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	nacosv1alpha1 "nacos.io/nacos-operator/api/v1alpha1"
	myErrors "nacos.io/nacos-operator/pkg/errors"
	"nacos.io/nacos-operator/pkg/service/k8s"
	nacosClient "nacos.io/nacos-operator/pkg/service/nacos"
)

type checkK8sService struct {
	k8s.Services
	replicas int32
	pods     []corev1.Pod
	err      error
}

func (s *checkK8sService) GetStatefulSet(string, string) (*appsv1.StatefulSet, error) {
	return &appsv1.StatefulSet{Spec: appsv1.StatefulSetSpec{Replicas: &s.replicas}}, nil
}

func (s *checkK8sService) GetStatefulSetReadPod(string, string) ([]corev1.Pod, error) {
	return s.pods, s.err
}

func checkNacosResource(replicas int32) *nacosv1alpha1.Nacos {
	return &nacosv1alpha1.Nacos{
		ObjectMeta: metav1.ObjectMeta{Name: "nacos", Namespace: "test"},
		Spec:       nacosv1alpha1.NacosSpec{Replicas: &replicas, Type: TYPE_CLUSTER},
	}
}

func checkPanic(fn func()) (value interface{}) {
	defer func() { value = recover() }()
	fn()
	return nil
}

func TestCheckKindPreservesReadyQueryError(t *testing.T) {
	for _, ready := range []int{0, 2} {
		t.Run(fmt.Sprintf("%d returned Pods", ready), func(t *testing.T) {
			sentinel := errors.New("list Pods forbidden")
			service := &checkK8sService{replicas: 4, pods: make([]corev1.Pod, ready), err: sentinel}
			client := &CheckClient{k8sService: service, logger: log.Discard()}
			got := checkPanic(func() { client.CheckKind(checkNacosResource(4)) })
			if got != sentinel {
				t.Fatalf("got panic %v, want original query error %v", got, sentinel)
			}
		})
	}
}

func TestCheckKindRetainsReplicaThreshold(t *testing.T) {
	for _, tt := range []struct {
		name      string
		replicas  int32
		ready     int
		wantPanic bool
	}{
		{"four replicas below threshold", 4, 1, true},
		{"four replicas at existing threshold", 4, 2, false},
		{"all four Ready", 4, 4, false},
		{"three replicas below threshold", 3, 1, true},
		{"three replicas at threshold", 3, 2, false},
		{"standalone not Ready", 1, 0, true},
		{"standalone Ready", 1, 1, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			pods := make([]corev1.Pod, tt.ready)
			client := &CheckClient{k8sService: &checkK8sService{replicas: tt.replicas, pods: pods}, logger: log.Discard()}
			var got []corev1.Pod
			panicValue := checkPanic(func() { got = client.CheckKind(checkNacosResource(tt.replicas)) })
			if tt.wantPanic {
				err, ok := panicValue.(*myErrors.Err)
				if !ok || err.Code != myErrors.CODE_ERR_UNKNOW {
					t.Fatalf("expected Ready threshold error, got %v", panicValue)
				}
			} else if panicValue != nil || len(got) != tt.ready {
				t.Fatalf("got Pods=%d panic=%v, want %d Pods without panic", len(got), panicValue, tt.ready)
			}
		})
	}
}

type checkClusterNodesClient struct {
	responses map[string]nacosClient.ServersInfo
	err       error
	calls     []string
}

func (c *checkClusterNodesClient) GetClusterNodes(ip string) (nacosClient.ServersInfo, error) {
	c.calls = append(c.calls, ip)
	if c.err != nil {
		return nacosClient.ServersInfo{}, c.err
	}
	servers, ok := c.responses[ip]
	if !ok {
		return nacosClient.ServersInfo{}, fmt.Errorf("unexpected Pod IP %s", ip)
	}
	return servers, nil
}

func checkClusterServers(t *testing.T, replicas int, leader string) nacosClient.ServersInfo {
	t.Helper()
	members := make([]interface{}, 0, replicas)
	for i := 0; i < replicas; i++ {
		members = append(members, map[string]interface{}{
			"ip": fmt.Sprintf("10.0.0.%d", i+1), "port": 8848, "state": "UP",
			"extendInfo": map[string]interface{}{
				"version": "2.4.3",
				"raftMetaData": map[string]interface{}{
					"metaDataMap": map[string]interface{}{
						"naming_persistent_service": map[string]interface{}{"leader": leader},
					},
				},
			},
		})
	}
	data, err := json.Marshal(map[string]interface{}{"servers": members})
	if err != nil {
		t.Fatal(err)
	}
	var servers nacosClient.ServersInfo
	if err := json.Unmarshal(data, &servers); err != nil {
		t.Fatal(err)
	}
	return servers
}

func checkClusterPods(replicas int) []corev1.Pod {
	pods := make([]corev1.Pod, replicas)
	for i := range pods {
		pods[i] = corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("nacos-%d", i), Namespace: "test"},
			Spec:       corev1.PodSpec{NodeName: fmt.Sprintf("worker-%d", i)},
			Status:     corev1.PodStatus{PodIP: fmt.Sprintf("10.0.0.%d", i+1)},
		}
	}
	return pods
}

func checkClusterClient(t *testing.T, pods []corev1.Pod, leader string) (*CheckClient, *checkClusterNodesClient) {
	t.Helper()
	stub := &checkClusterNodesClient{responses: make(map[string]nacosClient.ServersInfo)}
	for _, pod := range pods {
		stub.responses[pod.Status.PodIP] = checkClusterServers(t, len(pods), leader)
	}
	return &CheckClient{logger: log.Discard(), nacosClient: stub}, stub
}

func TestCheckNacosHealthyClusterHasStableConditions(t *testing.T) {
	pods := checkClusterPods(4)
	client, stub := checkClusterClient(t, pods, "nacos-0.nacos-headless.test.svc.cluster.local:7848")
	nacos := checkNacosResource(4)
	shuffled := []corev1.Pod{pods[3], pods[1], pods[0], pods[2]}
	if got := checkPanic(func() { client.CheckNacos(nacos, shuffled) }); got != nil {
		t.Fatalf("healthy cluster failed: %v", got)
	}
	want := make([]nacosv1alpha1.Condition, 0, len(pods))
	for i, pod := range pods {
		role := "follower"
		if i == 0 {
			role = "leader"
		}
		want = append(want, nacosv1alpha1.Condition{
			Type: role, Status: "true", Instance: pod.Status.PodIP,
			PodName: pod.Name, NodeName: pod.Spec.NodeName,
		})
	}
	if !reflect.DeepEqual(nacos.Status.Conditions, want) {
		t.Fatalf("unexpected Conditions: got %+v, want %+v", nacos.Status.Conditions, want)
	}
	if nacos.Status.Version != "2.4.3" {
		t.Fatalf("unexpected version %q", nacos.Status.Version)
	}
	before := nacos.DeepCopy().Status
	if got := checkPanic(func() { client.CheckNacos(nacos, pods) }); got != nil {
		t.Fatalf("repeat cluster check failed: %v", got)
	}
	if !reflect.DeepEqual(nacos.Status, before) {
		t.Fatalf("Pod ordering changed status: before %+v, after %+v", before, nacos.Status)
	}
	counts := make(map[string]int)
	for _, ip := range stub.calls {
		counts[ip]++
	}
	for _, pod := range pods {
		if counts[pod.Status.PodIP] != 2 {
			t.Fatalf("Pod %s checked %d times, want twice", pod.Name, counts[pod.Status.PodIP])
		}
	}
}

func TestCheckNacosRejectsUnhealthyCluster(t *testing.T) {
	tests := []struct {
		name    string
		change  func(*nacosClient.ServersInfo)
		message string
	}{
		{"missing member", func(s *nacosClient.ServersInfo) { s.Servers = s.Servers[:3] }, "server num is not equal"},
		{"node not UP", func(s *nacosClient.ServersInfo) { s.Servers[1].State = "DOWN" }, "node is not up"},
		{"inconsistent leader", func(s *nacosClient.ServersInfo) {
			s.Servers[1].ExtendInfo.RaftMetaData.MetaDataMap.NamingPersistentService.Leader = "nacos-1.nacos-headless.test.svc.cluster.local:7848"
		}, "leader not equal"},
		{"all leaders missing", func(s *nacosClient.ServersInfo) {
			for i := range s.Servers {
				s.Servers[i].ExtendInfo.RaftMetaData.MetaDataMap.NamingPersistentService.Leader = ""
			}
		}, "leader missing"},
		{"one leader missing", func(s *nacosClient.ServersInfo) {
			s.Servers[1].ExtendInfo.RaftMetaData.MetaDataMap.NamingPersistentService.Leader = ""
		}, "leader missing"},
		{"whitespace leader", func(s *nacosClient.ServersInfo) {
			s.Servers[0].ExtendInfo.RaftMetaData.MetaDataMap.NamingPersistentService.Leader = "  "
		}, "leader missing"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pods := checkClusterPods(4)
			client, stub := checkClusterClient(t, pods, "nacos-0.nacos-headless.test.svc.cluster.local:7848")
			servers := stub.responses[pods[0].Status.PodIP]
			tt.change(&servers)
			stub.responses[pods[0].Status.PodIP] = servers
			got := checkPanic(func() { client.CheckNacos(checkNacosResource(4), pods) })
			err, ok := got.(*myErrors.Err)
			if !ok || err.Code != myErrors.CODE_CLUSTER_FAILE || !strings.Contains(err.Msg, tt.message) {
				t.Fatalf("got panic %v, want cluster error containing %q", got, tt.message)
			}
		})
	}
}

func TestCheckNacosRejectsDifferentPodViews(t *testing.T) {
	pods := checkClusterPods(4)
	client, stub := checkClusterClient(t, pods, "nacos-0.nacos-headless.test.svc.cluster.local:7848")
	stub.responses[pods[3].Status.PodIP] = checkClusterServers(t, 4, "nacos-1.nacos-headless.test.svc.cluster.local:7848")
	got := checkPanic(func() { client.CheckNacos(checkNacosResource(4), pods) })
	err, ok := got.(*myErrors.Err)
	if !ok || err.Code != myErrors.CODE_CLUSTER_FAILE || !strings.Contains(err.Msg, "leader not equal") {
		t.Fatalf("got panic %v, want inconsistent leader error", got)
	}
}

func TestCheckNacosPreservesClientError(t *testing.T) {
	pods := checkClusterPods(4)
	client, stub := checkClusterClient(t, pods, "nacos-0.nacos-headless.test.svc.cluster.local:7848")
	stub.err = errors.New("HTTP 501 Not Implemented")
	got := checkPanic(func() { client.CheckNacos(checkNacosResource(4), pods) })
	err, ok := got.(*myErrors.Err)
	if !ok || err.Code != myErrors.CODE_CLUSTER_FAILE || !strings.Contains(err.Msg, stub.err.Error()) {
		t.Fatalf("got panic %v, want original client error", got)
	}
}

func TestCheckNacosStandaloneAllowsMissingLeader(t *testing.T) {
	pods := checkClusterPods(1)
	client, _ := checkClusterClient(t, pods, "")
	nacos := checkNacosResource(1)
	nacos.Spec.Type = TYPE_STAND_ALONE
	if got := checkPanic(func() { client.CheckNacos(nacos, pods) }); got != nil {
		t.Fatalf("standalone without leader failed: %v", got)
	}
	if nacos.Status.Version != "2.4.3" || len(nacos.Status.Conditions) != 1 {
		t.Fatalf("standalone status not populated: %+v", nacos.Status)
	}
	if nacos.Status.Conditions[0].Type != "" {
		t.Fatalf("assigned a cluster role to standalone without leader: %+v", nacos.Status.Conditions[0])
	}
}
