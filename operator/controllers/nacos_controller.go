/*
Copyright 2021.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controllers

import (
	"context"
	"fmt"
	"time"

	"nacos.io/nacos-operator/pkg/service/operator"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	k8sErrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	nacosgroupv1alpha1 "nacos.io/nacos-operator/api/v1alpha1"

	myErrors "nacos.io/nacos-operator/pkg/errors"
)

// NacosReconciler reconciles a Nacos object
type NacosReconciler struct {
	client.Client
	Log            logr.Logger
	Scheme         *runtime.Scheme
	OperaterClient *operator.OperatorClient
}

// +kubebuilder:rbac:groups=nacos.io,resources=nacos,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=nacos.io,resources=nacos/status,verbs=get;update;patch
type reconcileFun func(nacos *nacosgroupv1alpha1.Nacos)

func (r *NacosReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	_ = context.Background()
	_ = r.Log.WithValues("nacos", req.NamespacedName)

	instance := &nacosgroupv1alpha1.Nacos{}
	err := r.Client.Get(ctx, req.NamespacedName, instance)
	if err != nil {
		if k8sErrors.IsNotFound(err) {
			return reconcile.Result{}, nil
		}
		r.Log.Error(err, "Unable to read Nacos resource")
		return reconcile.Result{RequeueAfter: time.Second * 5}, nil
	}

	// Capture all status fields before any reconciliation stage changes them.
	previous := instance.Status.DeepCopy()
	result := r.ReconcileWork(instance, previous)
	if result == false {
		return reconcile.Result{RequeueAfter: time.Second * 5}, nil
	} else {
		return reconcile.Result{RequeueAfter: time.Second * 30}, nil
	}

}

func (r *NacosReconciler) ReconcileWork(instance *nacosgroupv1alpha1.Nacos, previous *nacosgroupv1alpha1.NacosStatus) bool {
	// 处理全局异常处理中的异常
	defer func() {
		if err := recover(); err != nil {
			r.Log.Error(reconciliationError(err), "Unable to persist reconciliation status")
		}
	}()

	// 全局处理异常
	defer func() {
		if err := recover(); err != nil {
			// 可处理的异常
			r.globalExceptHandle(err, instance, previous)
		}
	}()

	for _, fun := range []reconcileFun{
		r.OperaterClient.PreCheck,
		// 保证资源能够创建
		r.OperaterClient.MakeEnsure,
		// 检查并保障
		r.OperaterClient.CheckAndMakeHeal,
	} {
		fun(instance)
	}

	r.OperaterClient.UpdateStatus(instance, previous)
	return true
}

func filterByLabel(label map[string]string) bool {
	v := label["middleware"]
	if v != "nacos" {
		return false
	} else {
		return true
	}
}

func (r *NacosReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&nacosgroupv1alpha1.Nacos{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Owns(&appsv1.StatefulSet{}).
		Complete(r)
}

// Keep custom error codes while handling ordinary API errors through the same retry path.
func (r *NacosReconciler) globalExceptHandle(err interface{}, instance *nacosgroupv1alpha1.Nacos, previous *nacosgroupv1alpha1.NacosStatus) {
	myerr, ok := err.(*myErrors.Err)
	if !ok {
		failure := reconciliationError(err)
		r.Log.Error(failure, "Reconciliation failed")
		myerr = myErrors.NewErr(failure)
	} else if myerr.Code != myErrors.CODE_NORMAL {
		r.Log.Info("Reconciliation deferred", "code", myerr.Code, "message", myerr.Msg)
	}
	if myerr.Code == myErrors.CODE_NORMAL {
		r.OperaterClient.StatusClient.UpdateStatus(instance, previous)
		return
	}
	// Retain the existing three-minute grace period while initially creating the cluster.
	if instance.Status.Phase != nacosgroupv1alpha1.PhaseCreating ||
		instance.CreationTimestamp.Add(time.Minute*3).Before(time.Now()) {
		r.OperaterClient.StatusClient.UpdateExceptionStatus(instance, myerr, previous)
	}
}

func reconciliationError(value interface{}) error {
	if err, ok := value.(error); ok {
		return err
	}
	return fmt.Errorf("%v", value)
}
