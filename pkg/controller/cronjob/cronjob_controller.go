/*
Copyright 2016 The Kubernetes Authors.

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

package cronjob

/*
I did not use watch or expectations.  Those add a lot of corner cases, and we aren't
expecting a large volume of jobs or cronJobs.  (We are favoring correctness
over scalability.  If we find a single controller thread is too slow because
there are a lot of Jobs or CronJobs, we can parallelize by Namespace.
If we find the load on the API server is too high, we can use a watch and
UndeltaStore.)

Just periodically list jobs and cronJobs, and then reconcile them.
*/

import (
	"context"
	"fmt"
	"sort"
	"time"

	"k8s.io/klog/v2"

	batchv1 "k8s.io/api/batch/v1"
	batchv1beta1 "k8s.io/api/batch/v1beta1"
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/pager"
	"k8s.io/client-go/tools/record"
	ref "k8s.io/client-go/tools/reference"
	"k8s.io/component-base/metrics/prometheus/ratelimiter"
)

// Utilities for dealing with Jobs and CronJobs and time.

// controllerKind contains the schema.GroupVersionKind for this controller type.
var controllerKind = batchv1beta1.SchemeGroupVersion.WithKind("CronJob")

// Controller is a controller for CronJobs.
type Controller struct {
	// 用于访问apiserver的client
	kubeClient clientset.Interface
	// 用于管理job 会使用访问apiserver的client 对job进行操作 如获取 新增 删除 更新
	jobControl jobControlInterface
	// 用于管理cronjob 会使用访问apiserver的client 对cronjob进行操作 如更新
	cjControl cjControlInterface
	// 用于管理pod 会使用访问apiserver的client 对pod进行操作 如新增 删除 更新
	podControl podControlInterface
	// 事件记录器 用于记录事件
	recorder record.EventRecorder
}

// NewController creates and initializes a new Controller.
func NewController(kubeClient clientset.Interface) (*Controller, error) {
	// 创建事件管理器
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartStructuredLogging(0)
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: kubeClient.CoreV1().Events("")})

	if kubeClient != nil && kubeClient.CoreV1().RESTClient().GetRateLimiter() != nil {
		if err := ratelimiter.RegisterMetricAndTrackRateLimiterUsage("cronjob_controller", kubeClient.CoreV1().RESTClient().GetRateLimiter()); err != nil {
			return nil, err
		}
	}

	jm := &Controller{
		kubeClient: kubeClient,
		jobControl: realJobControl{KubeClient: kubeClient},
		cjControl:  &realCJControl{KubeClient: kubeClient},
		podControl: &realPodControl{KubeClient: kubeClient},
		recorder:   eventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: "cronjob-controller"}),
	}

	return jm, nil
}

// Run starts the main goroutine responsible for watching and syncing jobs.
func (jm *Controller) Run(stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	klog.Infof("Starting CronJob Manager")
	// Check things every 10 second.
	// 每10秒钟调用一次syncAll
	go wait.Until(jm.syncAll, 10*time.Second, stopCh)
	<-stopCh
	klog.Infof("Shutting down CronJob Manager")
}

// syncAll lists all the CronJobs and Jobs and reconciles them.
func (jm *Controller) syncAll() {
	// List children (Jobs) before parents (CronJob).
	// This guarantees that if we see any Job that got orphaned by the GC orphan finalizer,
	// we must also see that the parent CronJob has non-nil DeletionTimestamp (see #42639).
	// Note that this only works because we are NOT using any caches here.
	// 获取所有的job
	jobListFunc := func(opts metav1.ListOptions) (runtime.Object, error) {
		return jm.kubeClient.BatchV1().Jobs(metav1.NamespaceAll).List(context.TODO(), opts)
	}
	// 以分页的方式获取job
	js := make([]batchv1.Job, 0)
	err := pager.New(pager.SimplePageFunc(jobListFunc)).EachListItem(context.Background(), metav1.ListOptions{}, func(object runtime.Object) error {
		jobTmp, ok := object.(*batchv1.Job)
		if !ok {
			return fmt.Errorf("expected type *batchv1.Job, got type %T", jobTmp)
		}
		js = append(js, *jobTmp)
		return nil
	})

	if err != nil {
		utilruntime.HandleError(fmt.Errorf("Failed to extract job list: %v", err))
		return
	}

	klog.V(4).Infof("Found %d jobs", len(js))
	// 获取所有的cronJob
	cronJobListFunc := func(opts metav1.ListOptions) (runtime.Object, error) {
		return jm.kubeClient.BatchV1beta1().CronJobs(metav1.NamespaceAll).List(context.TODO(), opts)
	}
	// 处理job 筛选出属于CronJob的job map[CronJobUID] = [Job]
	jobsByCj := groupJobsByParent(js)
	klog.V(4).Infof("Found %d groups", len(jobsByCj))
	err = pager.New(pager.SimplePageFunc(cronJobListFunc)).EachListItem(context.Background(), metav1.ListOptions{}, func(object runtime.Object) error {
		cj, ok := object.(*batchv1beta1.CronJob)
		if !ok {
			return fmt.Errorf("expected type *batchv1beta1.CronJob, got type %T", cj)
		}
		// 将cronjob及对应的关联job进行同步
		syncOne(cj, jobsByCj[cj.UID], time.Now(), jm.jobControl, jm.cjControl, jm.recorder)
		// 清理已经完成的jobs
		cleanupFinishedJobs(cj, jobsByCj[cj.UID], jm.jobControl, jm.cjControl, jm.recorder)
		return nil
	})

	if err != nil {
		utilruntime.HandleError(fmt.Errorf("Failed to extract cronJobs list: %v", err))
		return
	}
}

// cleanupFinishedJobs cleanups finished jobs created by a CronJob
func cleanupFinishedJobs(cj *batchv1beta1.CronJob, js []batchv1.Job, jc jobControlInterface,
	cjc cjControlInterface, recorder record.EventRecorder) {
	// If neither limits are active, there is no need to do anything.
	// 判断 如果历史失败Job次数 == 空 和 历史成功Job次数 == 空 那么不需要清理 因为默认不保存历史记录
	if cj.Spec.FailedJobsHistoryLimit == nil && cj.Spec.SuccessfulJobsHistoryLimit == nil {
		return
	}

	failedJobs := []batchv1.Job{}
	successfulJobs := []batchv1.Job{}

	for _, job := range js {
		isFinished, finishedStatus := getFinishedStatus(&job)
		// 如果已经job已经完成 并且是成功状态 那么加入successfulJobs
		if isFinished && finishedStatus == batchv1.JobComplete {
			successfulJobs = append(successfulJobs, job)
		} else if isFinished && finishedStatus == batchv1.JobFailed {
			// 如果已经job已经完成 并且是失败状态 那么加入failedJobs
			failedJobs = append(failedJobs, job)
		}
	}
	// 如果历史成功Job次数 不为空 那么删除掉 len(所有job) - SuccessfulJobsHistoryLimit
	if cj.Spec.SuccessfulJobsHistoryLimit != nil {
		removeOldestJobs(cj,
			successfulJobs,
			jc,
			*cj.Spec.SuccessfulJobsHistoryLimit,
			recorder)
	}
	// 如果历史失败Job次数 不为空 那么删除掉 len(所有job) - FailedJobsHistoryLimit
	if cj.Spec.FailedJobsHistoryLimit != nil {
		removeOldestJobs(cj,
			failedJobs,
			jc,
			*cj.Spec.FailedJobsHistoryLimit,
			recorder)
	}

	// Update the CronJob, in case jobs were removed from the list.
	// 更新cronJob状态
	if _, err := cjc.UpdateStatus(cj); err != nil {
		nameForLog := fmt.Sprintf("%s/%s", cj.Namespace, cj.Name)
		klog.Infof("Unable to update status for %s (rv = %s): %v", nameForLog, cj.ResourceVersion, err)
	}
}

// removeOldestJobs removes the oldest jobs from a list of jobs
func removeOldestJobs(cj *batchv1beta1.CronJob, js []batchv1.Job, jc jobControlInterface, maxJobs int32, recorder record.EventRecorder) {
	// 删除历史的Job
	numToDelete := len(js) - int(maxJobs)
	if numToDelete <= 0 {
		return
	}

	nameForLog := fmt.Sprintf("%s/%s", cj.Namespace, cj.Name)
	klog.V(4).Infof("Cleaning up %d/%d jobs from %s", numToDelete, len(js), nameForLog)
	// 将job根据startTime进行排序
	sort.Sort(byJobStartTime(js))
	// 删除job
	for i := 0; i < numToDelete; i++ {
		klog.V(4).Infof("Removing job %s from %s", js[i].Name, nameForLog)
		deleteJob(cj, &js[i], jc, recorder)
	}
}

// syncOne reconciles a CronJob with a list of any Jobs that it created.
// All known jobs created by "cj" should be included in "js".
// The current time is passed in to facilitate testing.
// It has no receiver, to facilitate testing.
func syncOne(cj *batchv1beta1.CronJob, js []batchv1.Job, now time.Time, jc jobControlInterface, cjc cjControlInterface, recorder record.EventRecorder) {
	nameForLog := fmt.Sprintf("%s/%s", cj.Namespace, cj.Name)

	childrenJobs := make(map[types.UID]bool)
	// 遍历所有的job
	for _, j := range js {
		childrenJobs[j.ObjectMeta.UID] = true
		// 遍历cronjob.status.active 判断job是否在active中
		found := inActiveList(*cj, j.ObjectMeta.UID)
		// 如果不存在 并且该job不处于finisehd状态 发送对应事件
		if !found && !IsJobFinished(&j) {
			recorder.Eventf(cj, v1.EventTypeWarning, "UnexpectedJob", "Saw a job that the controller did not create or forgot: %s", j.Name)
			// We found an unfinished job that has us as the parent, but it is not in our Active list.
			// This could happen if we crashed right after creating the Job and before updating the status,
			// or if our jobs list is newer than our cj status after a relist, or if someone intentionally created
			// a job that they wanted us to adopt.

			// TODO: maybe handle the adoption case?  Concurrency/suspend rules will not apply in that case, obviously, since we can't
			// stop users from creating jobs if they have permission.  It is assumed that if a
			// user has permission to create a job within a namespace, then they have permission to make any cronJob
			// in the same namespace "adopt" that job.  ReplicaSets and their Pods work the same way.
			// TBS: how to update cj.Status.LastScheduleTime if the adopted job is newer than any we knew about?
		} else if found && IsJobFinished(&j) {
			// 如果存在 并且该job已经处于finisehd状态 发送对应事件
			_, status := getFinishedStatus(&j)
			// 从cronjob.status.active里删除掉
			deleteFromActiveList(cj, j.ObjectMeta.UID)
			recorder.Eventf(cj, v1.EventTypeNormal, "SawCompletedJob", "Saw completed job: %s, status: %v", j.Name, status)
		}
	}

	// Remove any job reference from the active list if the corresponding job does not exist any more.
	// Otherwise, the cronjob may be stuck in active mode forever even though there is no matching
	// job running.
	// 遍历cronjob.status.active 判断job 是否在childrenJobs中 如果不在 那么从cronjob.status.active删除对应job
	for _, j := range cj.Status.Active {
		if found := childrenJobs[j.UID]; !found {
			recorder.Eventf(cj, v1.EventTypeNormal, "MissingJob", "Active job went missing: %v", j.Name)
			deleteFromActiveList(cj, j.UID)
		}
	}
	// 更新cronjob的状态
	updatedCJ, err := cjc.UpdateStatus(cj)
	if err != nil {
		klog.Errorf("Unable to update status for %s (rv = %s): %v", nameForLog, cj.ResourceVersion, err)
		return
	}
	*cj = *updatedCJ
	// 判断cronjob是否被删除
	if cj.DeletionTimestamp != nil {
		// The CronJob is being deleted.
		// Don't do anything other than updating status.
		return
	}
	// 判断cronjob Suspend
	if cj.Spec.Suspend != nil && *cj.Spec.Suspend {
		klog.V(4).Infof("Not starting job for %s because it is suspended", nameForLog)
		return
	}
	// 获取cronjob的调度时间列表
	times, err := getRecentUnmetScheduleTimes(*cj, now)
	if err != nil {
		recorder.Eventf(cj, v1.EventTypeWarning, "FailedNeedsStart", "Cannot determine if job needs to be started: %v", err)
		klog.Errorf("Cannot determine if %s needs to be started: %v", nameForLog, err)
		return
	}
	// TODO: handle multiple unmet start times, from oldest to newest, updating status as needed.
	// 如果时间列表为空 那么代表目前还不需要被调度
	if len(times) == 0 {
		klog.V(4).Infof("No unmet start times for %s", nameForLog)
		return
	}
	// 如果时间列表 大于0 代表有多次时间为满足调度
	if len(times) > 1 {
		klog.V(4).Infof("Multiple unmet start times for %s so only starting last one", nameForLog)
	}
	// 获取最后一次时间
	scheduledTime := times[len(times)-1]
	tooLate := false
	if cj.Spec.StartingDeadlineSeconds != nil {
		// (scheduledTime + StartingDeadlineSeconds) > now
		// now - scheduledTime > StartingDeadlineSeconds
		tooLate = scheduledTime.Add(time.Second * time.Duration(*cj.Spec.StartingDeadlineSeconds)).Before(now)
	}
	if tooLate {
		klog.V(4).Infof("Missed starting window for %s", nameForLog)
		recorder.Eventf(cj, v1.EventTypeWarning, "MissSchedule", "Missed scheduled time to start a job: %s", scheduledTime.Format(time.RFC1123Z))
		// TODO: Since we don't set LastScheduleTime when not scheduling, we are going to keep noticing
		// the miss every cycle.  In order to avoid sending multiple events, and to avoid processing
		// the cj again and again, we could set a Status.LastMissedTime when we notice a miss.
		// Then, when we call getRecentUnmetScheduleTimes, we can take max(creationTimestamp,
		// Status.LastScheduleTime, Status.LastMissedTime), and then so we won't generate
		// and event the next time we process it, and also so the user looking at the status
		// can see easily that there was a missed execution.
		return
	}
	// 判断cronjob的并发策略 如果策略是Forbid 意味着禁止并发执行Job 所以如果active大于0 那么直接return 跳过这次sync
	if cj.Spec.ConcurrencyPolicy == batchv1beta1.ForbidConcurrent && len(cj.Status.Active) > 0 {
		// Regardless which source of information we use for the set of active jobs,
		// there is some risk that we won't see an active job when there is one.
		// (because we haven't seen the status update to the SJ or the created pod).
		// So it is theoretically possible to have concurrency with Forbid.
		// As long the as the invocations are "far enough apart in time", this usually won't happen.
		//
		// TODO: for Forbid, we could use the same name for every execution, as a lock.
		// With replace, we could use a name that is deterministic per execution time.
		// But that would mean that you could not inspect prior successes or failures of Forbid jobs.
		klog.V(4).Infof("Not starting job for %s because of prior execution still running and concurrency policy is Forbid", nameForLog)
		return
	}
	// 判断并发策略 是否为replace 那么就删除所有已经存在的job
	if cj.Spec.ConcurrencyPolicy == batchv1beta1.ReplaceConcurrent {
		for _, j := range cj.Status.Active {
			klog.V(4).Infof("Deleting job %s of %s that was still running at next scheduled start time", j.Name, nameForLog)

			job, err := jc.GetJob(j.Namespace, j.Name)
			if err != nil {
				recorder.Eventf(cj, v1.EventTypeWarning, "FailedGet", "Get job: %v", err)
				return
			}
			if !deleteJob(cj, job, jc, recorder) {
				return
			}
		}
	}
	// 获取Job的模版
	jobReq, err := getJobFromTemplate(cj, scheduledTime)
	if err != nil {
		klog.Errorf("Unable to make Job from template in %s: %v", nameForLog, err)
		return
	}
	// 创建Job
	jobResp, err := jc.CreateJob(cj.Namespace, jobReq)
	if err != nil {
		// If the namespace is being torn down, we can safely ignore
		// this error since all subsequent creations will fail.
		if !errors.HasStatusCause(err, v1.NamespaceTerminatingCause) {
			recorder.Eventf(cj, v1.EventTypeWarning, "FailedCreate", "Error creating job: %v", err)
		}
		return
	}
	klog.V(4).Infof("Created Job %s for %s", jobResp.Name, nameForLog)
	// 生成创建Job的事件
	recorder.Eventf(cj, v1.EventTypeNormal, "SuccessfulCreate", "Created job %v", jobResp.Name)

	// ------------------------------------------------------------------ //

	// If this process restarts at this point (after posting a job, but
	// before updating the status), then we might try to start the job on
	// the next time.  Actually, if we re-list the SJs and Jobs on the next
	// iteration of syncAll, we might not see our own status update, and
	// then post one again.  So, we need to use the job name as a lock to
	// prevent us from making the job twice (name the job with hash of its
	// scheduled time).

	// Add the just-started job to the status list.
	// 获取job的ObjectReference 将它添加到cronjob.status.active中
	ref, err := getRef(jobResp)
	if err != nil {
		klog.V(2).Infof("Unable to make object reference for job for %s", nameForLog)
	} else {
		cj.Status.Active = append(cj.Status.Active, *ref)
	}
	// 更新最后一次调度时间 并且更新cronjob状态
	cj.Status.LastScheduleTime = &metav1.Time{Time: scheduledTime}
	if _, err := cjc.UpdateStatus(cj); err != nil {
		klog.Infof("Unable to update status for %s (rv = %s): %v", nameForLog, cj.ResourceVersion, err)
	}

	return
}

// deleteJob reaps a job, deleting the job, the pods and the reference in the active list
func deleteJob(cj *batchv1beta1.CronJob, job *batchv1.Job, jc jobControlInterface, recorder record.EventRecorder) bool {
	nameForLog := fmt.Sprintf("%s/%s", cj.Namespace, cj.Name)

	// delete the job itself...
	if err := jc.DeleteJob(job.Namespace, job.Name); err != nil {
		recorder.Eventf(cj, v1.EventTypeWarning, "FailedDelete", "Deleted job: %v", err)
		klog.Errorf("Error deleting job %s from %s: %v", job.Name, nameForLog, err)
		return false
	}
	// ... and its reference from active list
	deleteFromActiveList(cj, job.ObjectMeta.UID)
	recorder.Eventf(cj, v1.EventTypeNormal, "SuccessfulDelete", "Deleted job %v", job.Name)

	return true
}

func getRef(object runtime.Object) (*v1.ObjectReference, error) {
	return ref.GetReference(scheme.Scheme, object)
}
