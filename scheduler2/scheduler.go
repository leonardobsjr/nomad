package scheduler2

import (
	"fmt"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-multierror"
	"github.com/hashicorp/nomad/nomad/structs"
	v1 "github.com/hashicorp/nomad/scheduler"
)

type Scheduler interface {
	Process() error
}

type scheduler struct {
	eval     *structs.Evaluation
	logger   hclog.Logger
	eventsCh chan<- interface{}
	state    v1.State
	planner  v1.Planner

	job        *structs.Job
	plan       *structs.Plan
	planResult *structs.PlanResult
	ctx        *v1.EvalContext
	stack      *v1.GenericStack

	// followUpEvals are evals with WaitUntil set, which are delayed until that time
	// before being rescheduled
	followUpEvals  []*structs.Evaluation
	deployment     *structs.Deployment
	next           *structs.Evaluation
	blocked        *structs.Evaluation
	failedTGAllocs map[string]*structs.AllocMetric
	queuedAllocs   map[string]int

	strategy *schedulerStrategy

	downNodes         map[string]*structs.Node
	disconnectedNodes map[string]*structs.Node
}

// NewScheduler is the constructor method that initializes and returns a new scheduler.
func NewScheduler(
	eval *structs.Evaluation,
	logger hclog.Logger,
	eventsCh chan<- interface{},
	state v1.State,
	planner v1.Planner) Scheduler {

	s := &scheduler{
		eval:           eval,
		eventsCh:       eventsCh,
		state:          state,
		planner:        planner,
		failedTGAllocs: map[string]*structs.AllocMetric{},
	}

	s.strategy = &schedulerStrategy{s}
	s.logger = logger.With("eval_id", eval.ID, "job_id", eval.JobID, "namespace", eval.Namespace)

	return s
}

func (s *scheduler) Process() error {
	// TODO: validateTrigger should really be called by the constructor but that
	// might create too much interface breakage, so punting on that for now.
	if err := s.validateTrigger(); err != nil {
		return err
	}

	// Retry up to the maxScheduleAttempts and reset if progress is made.
	if err := retryMax(s.strategy.retryLimit(), s.process, s.progressMade); err != nil {
		return s.handleRetryExceeded(err)
	}

	// If the current evaluation is a blocked evaluation, and we didn't place
	// everything, do not update the status to complete.
	if s.isIncompleteBlocked() {
		return s.handleIncompleteBlockedEval()
	}

	// Update the status to complete
	return s.setStatus(structs.EvalStatusComplete, "")
}

func (s *scheduler) validateTrigger() error {
	// Verify the evaluation trigger reason is understood
	switch s.eval.TriggeredBy {
	case structs.EvalTriggerJobRegister, structs.EvalTriggerJobDeregister,
		structs.EvalTriggerNodeDrain, structs.EvalTriggerNodeUpdate,
		structs.EvalTriggerAllocStop,
		structs.EvalTriggerRollingUpdate, structs.EvalTriggerQueuedAllocs,
		structs.EvalTriggerPeriodicJob, structs.EvalTriggerMaxPlans,
		structs.EvalTriggerDeploymentWatcher, structs.EvalTriggerRetryFailedAlloc,
		structs.EvalTriggerFailedFollowUp, structs.EvalTriggerPreemption,
		structs.EvalTriggerScaling:
	default:
		desc := fmt.Sprintf("scheduler cannot handle '%s' evaluation reason",
			s.eval.TriggeredBy)
		return s.setStatus(structs.EvalStatusFailed, desc)
	}

	return nil
}

func (s *scheduler) handleRetryExceeded(err error) error {
	if statusErr, ok := err.(*v1.SetStatusError); ok {
		// Scheduling was tried but made no forward progress so create a
		// blocked eval to retry once resources become available.
		var mErr multierror.Error
		if err = s.createBlockedEval(true); err != nil {
			mErr.Errors = append(mErr.Errors, err)
		}
		if err := s.setStatus(statusErr.EvalStatus, err.Error()); err != nil {
			mErr.Errors = append(mErr.Errors, err)
		}
		return mErr.ErrorOrNil()
	}
	return err
}

func (s *scheduler) isIncompleteBlocked() bool {
	return s.eval.Status == structs.EvalStatusBlocked && len(s.failedTGAllocs) != 0
}

func (s *scheduler) handleIncompleteBlockedEval() error {
	e := s.ctx.Eligibility()
	newEval := s.eval.Copy()
	newEval.EscapedComputedClass = e.HasEscaped()
	newEval.ClassEligibility = e.GetClasses()
	newEval.QuotaLimitReached = e.QuotaLimitReached()
	return s.planner.ReblockEval(newEval)
}

// setStatus is used to update the status of the evaluation
func (s *scheduler) setStatus(status, desc string) error {

	s.logger.Debug("setting eval status", "status", status)
	newEval := s.eval.Copy()
	newEval.Status = status
	newEval.StatusDescription = desc
	newEval.DeploymentID = s.deployment.GetID()
	newEval.FailedTGAllocs = s.failedTGAllocs
	if s.next != nil {
		newEval.NextEval = s.next.ID
	}
	if s.blocked != nil {
		newEval.BlockedEval = s.blocked.ID
	}
	if s.queuedAllocs != nil {
		newEval.QueuedAllocations = s.queuedAllocs
	}

	return s.planner.UpdateEval(newEval)
}

// progressMade checks to see if the plan result made allocations or updates.
// If the result is nil, false is returned.
func (s *scheduler) progressMade() bool {
	return s.planResult != nil && (len(s.planResult.NodeUpdate) != 0 ||
		len(s.planResult.NodeAllocation) != 0 || s.planResult.Deployment != nil ||
		len(s.planResult.DeploymentUpdates) != 0)
}

// createBlockedEval creates a blocked eval and submits it to the planner. If
// failure is set to true, the eval's trigger reason reflects that.
func (s *scheduler) createBlockedEval(planFailure bool) error {
	e := s.ctx.Eligibility()
	escaped := e.HasEscaped()

	// Only store the eligible classes if the eval hasn't escaped.
	var classEligibility map[string]bool
	if !escaped {
		classEligibility = e.GetClasses()
	}

	s.blocked = s.eval.CreateBlockedEval(classEligibility, escaped, e.QuotaLimitReached(), s.failedTGAllocs)
	if planFailure {
		s.blocked.TriggeredBy = structs.EvalTriggerMaxPlans
		s.blocked.StatusDescription = blockedEvalMaxPlanDesc
	} else {
		s.blocked.StatusDescription = blockedEvalFailedPlacements
	}

	return s.planner.CreateEval(s.blocked)
}

// schedulerStrategy encapsulates the variable aspects of the scheduler based on
// eval.Type. We'll start with one strategy that returns behavior with switches
// and then, if necessary, refactor to an interface with multiple implementations
// by type once we better understand the variability.
type schedulerStrategy struct {
	scheduler *scheduler
}

func (ss *schedulerStrategy) retryLimit() int {
	switch ss.scheduler.eval.Type {
	case structs.JobTypeService:
		return maxServiceScheduleAttempts
	case structs.JobTypeBatch:
		return maxBatchScheduleAttempts
	// TODO: handle system and sysbatch
	default:
		// TODO: Probably should err here but that would break interface.
		return maxServiceScheduleAttempts
	}
}

func (ss *schedulerStrategy) initDeployment(ws membdb.WatchSet) error {
	if ss.scheduler.eval.Type != structs.JobTypeService {
		return nil
	}

	var err error
	ss.scheduler.deployment, err = ss.scheduler.state.LatestDeploymentByJobID(ws, s.eval.Namespace, s.eval.JobID)
	if err != nil {
		return fmt.Errorf("failed to get job deployment %q: %v", s.eval.JobID, err)
	}

	return nil
}

func (ss *schedulerStrategy) initStack() {
	isBatch := ss.scheduler.eval.Type == structs.JobTypeBatch

	ss.scheduler.stack = v1.NewGenericStack(isBatch, ss.scheduler.ctx)

	if !ss.scheduler.job.Stopped() {
		ss.scheduler.stack.SetJob(ss.scheduler.job)
	}
}

const (
	// maxServiceScheduleAttempts is used to limit the number of times
	// we will attempt to schedule if we continue to hit conflicts for services.
	maxServiceScheduleAttempts = 5

	// maxBatchScheduleAttempts is used to limit the number of times
	// we will attempt to schedule if we continue to hit conflicts for batch.
	maxBatchScheduleAttempts = 2

	// allocNotNeeded is the status used when a job no longer requires an allocation
	allocNotNeeded = "alloc not needed due to job update"

	// allocMigrating is the status used when we must migrate an allocation
	allocMigrating = "alloc is being migrated"

	// allocUpdating is the status used when a job requires an update
	allocUpdating = "alloc is being updated due to job update"

	// allocLost is the status used when an allocation is lost
	allocLost = "alloc is lost since its node is down"

	// allocInPlace is the status used when speculating on an in-place update
	allocInPlace = "alloc updating in-place"

	// allocNodeTainted is the status used when stopping an alloc because its
	// node is tainted.
	allocNodeTainted = "alloc not needed as node is tainted"

	// allocRescheduled is the status used when an allocation failed and was rescheduled
	allocRescheduled = "alloc was rescheduled because it failed"

	// blockedEvalMaxPlanDesc is the description used for blocked evals that are
	// a result of hitting the max number of plan attempts
	blockedEvalMaxPlanDesc = "created due to placement conflicts"

	// blockedEvalFailedPlacements is the description used for blocked evals
	// that are a result of failing to place all allocations.
	blockedEvalFailedPlacements = "created to place remaining allocations"

	// reschedulingFollowupEvalDesc is the description used when creating follow
	// up evals for delayed rescheduling
	reschedulingFollowupEvalDesc = "created for delayed rescheduling"

	// maxPastRescheduleEvents is the maximum number of past reschedule event
	// that we track when unlimited rescheduling is enabled
	maxPastRescheduleEvents = 5
)
