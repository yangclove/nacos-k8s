package operator

import (
	"context"
	"sort"
	"time"

	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	log "github.com/go-logr/logr"
	nacosgroupv1alpha1 "nacos.io/nacos-operator/api/v1alpha1"
	myErrors "nacos.io/nacos-operator/pkg/errors"
	"nacos.io/nacos-operator/pkg/service/k8s"
)

type IStatusClient interface {
}

type StatusClient struct {
	logger log.Logger
	client client.Client
}

func NewStatusClient(logger log.Logger, k8sService k8s.Services, client client.Client) *StatusClient {
	return &StatusClient{client: client, logger: logger}
}

func (c *StatusClient) UpdateStatusRunning(nacos *nacosgroupv1alpha1.Nacos, previous *nacosgroupv1alpha1.NacosStatus) {
	c.updateLastEvent(nacos, 200, "", true)
	nacos.Status.Phase = nacosgroupv1alpha1.PhaseRunning
	c.UpdateStatus(nacos, previous)
}

// Compare against the snapshot taken before reconciliation changed any status fields.
func (c *StatusClient) UpdateStatus(nacos *nacosgroupv1alpha1.Nacos, previous *nacosgroupv1alpha1.NacosStatus) {
	sort.SliceStable(nacos.Status.Conditions, func(i, j int) bool {
		return nacos.Status.Conditions[i].PodName < nacos.Status.Conditions[j].PodName
	})
	if equality.Semantic.DeepEqual(previous, &nacos.Status) {
		return
	}
	myErrors.EnsureNormal(c.client.Status().Update(context.TODO(), nacos))
}

func (c *StatusClient) UpdateExceptionStatus(nacos *nacosgroupv1alpha1.Nacos, err *myErrors.Err, previous *nacosgroupv1alpha1.NacosStatus) {
	c.updateLastEvent(nacos, err.Code, err.Msg, false)
	nacos.Status.Phase = nacosgroupv1alpha1.PhaseFailed
	c.UpdateStatus(nacos, previous)
}

const EVENT_MAX_SIZE = 10

func (c *StatusClient) updateLastEvent(nacos *nacosgroupv1alpha1.Nacos, code int, msg string, status bool) {
	phase := nacosgroupv1alpha1.PhaseFailed
	if status {
		phase = nacosgroupv1alpha1.PhaseRunning
	}
	now := metav1.NewTime(time.Now())
	events := nacos.Status.Event
	if len(events) > 0 {
		last := &events[len(events)-1]
		if nacos.Status.Phase == phase && last.Code == code && last.Message == msg && last.Status == status {
			return
		}
		// Keep the existing history model: the same error code updates its latest event.
		if last.Code == code {
			last.Message = msg
			last.Status = status
			last.LastTransitionTime = now
			return
		}
	}
	nacos.Status.Event = append(events, nacosgroupv1alpha1.Event{
		Code: code, Status: status, Message: msg,
		FirstAppearTime: now, LastTransitionTime: now,
	})
	if len(nacos.Status.Event) > EVENT_MAX_SIZE {
		nacos.Status.Event = nacos.Status.Event[len(nacos.Status.Event)-EVENT_MAX_SIZE:]
	}
}
