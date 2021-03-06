/*
Copyright 2019 The Kubernetes Authors.

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

package pluginmanager

import (
	"time"

	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/kubelet/config"
	"k8s.io/kubernetes/pkg/kubelet/pluginmanager/cache"
	"k8s.io/kubernetes/pkg/kubelet/pluginmanager/metrics"
	"k8s.io/kubernetes/pkg/kubelet/pluginmanager/operationexecutor"
	"k8s.io/kubernetes/pkg/kubelet/pluginmanager/pluginwatcher"
	"k8s.io/kubernetes/pkg/kubelet/pluginmanager/reconciler"
)

// PluginManager runs a set of asynchronous loops that figure out which plugins
// need to be registered/deregistered and makes it so.
type PluginManager interface {
	// Starts the plugin manager and all the asynchronous loops that it controls
	Run(sourcesReady config.SourcesReady, stopCh <-chan struct{})

	// AddHandler adds the given plugin handler for a specific plugin type, which
	// will be added to the actual state of world cache so that it can be passed to
	// the desired state of world cache in order to be used during plugin
	// registration/deregistration
	// 为制定的插件类型添加对应的插件处理程序
	AddHandler(pluginType string, pluginHandler cache.PluginHandler)
}

const (
	// loopSleepDuration is the amount of time the reconciler loop waits
	// between successive executions
	loopSleepDuration = 1 * time.Second
)

// NewPluginManager returns a new concrete instance implementing the
// PluginManager interface.
func NewPluginManager(
	sockDir string,
	recorder record.EventRecorder) PluginManager {
	// 实际数据状态
	asw := cache.NewActualStateOfWorld()
	// 期望数据状态
	dsw := cache.NewDesiredStateOfWorld()
	// 消费者 处理数据并调用对应的插件处理程序对插件进行注册
	reconciler := reconciler.NewReconciler(
		operationexecutor.NewOperationExecutor(
			operationexecutor.NewOperationGenerator(
				recorder,
			),
		),
		loopSleepDuration,
		dsw,
		asw,
	)

	pm := &pluginManager{
		desiredStateOfWorldPopulator: pluginwatcher.NewWatcher(
			sockDir,
			dsw,
		),
		reconciler:          reconciler,
		desiredStateOfWorld: dsw,
		actualStateOfWorld:  asw,
	}
	return pm
}

// pluginManager implements the PluginManager interface
type pluginManager struct {
	// desiredStateOfWorldPopulator (the plugin watcher) runs an asynchronous
	// periodic loop to populate the desiredStateOfWorld.
	// 生产者 通过watcher plugin的目录 将插件地址写入dsw中
	desiredStateOfWorldPopulator *pluginwatcher.Watcher

	// reconciler runs an asynchronous periodic loop to reconcile the
	// desiredStateOfWorld with the actualStateOfWorld by triggering register
	// and unregister operations using the operationExecutor.
	// 消费者 通过注册插件/注销插件 使期望状态与实际状态达成一致
	reconciler reconciler.Reconciler

	// actualStateOfWorld is a data structure containing the actual state of
	// the world according to the manager: i.e. which plugins are registered.
	// The data structure is populated upon successful completion of register
	// and unregister actions triggered by the reconciler.
	actualStateOfWorld cache.ActualStateOfWorld

	// desiredStateOfWorld is a data structure containing the desired state of
	// the world according to the plugin manager: i.e. what plugins are registered.
	// The data structure is populated by the desired state of the world
	// populator (plugin watcher).
	desiredStateOfWorld cache.DesiredStateOfWorld
}

var _ PluginManager = &pluginManager{}

func (pm *pluginManager) Run(sourcesReady config.SourcesReady, stopCh <-chan struct{}) {
	defer runtime.HandleCrash()

	pm.desiredStateOfWorldPopulator.Start(stopCh)
	klog.V(2).Infof("The desired_state_of_world populator (plugin watcher) starts")

	klog.Infof("Starting Kubelet Plugin Manager")
	go pm.reconciler.Run(stopCh)

	metrics.Register(pm.actualStateOfWorld, pm.desiredStateOfWorld)
	<-stopCh
	klog.Infof("Shutting down Kubelet Plugin Manager")
}

func (pm *pluginManager) AddHandler(pluginType string, handler cache.PluginHandler) {
	pm.reconciler.AddHandler(pluginType, handler)
}
