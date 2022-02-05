package scheduler2

import (
	"fmt"

	"github.com/hashicorp/go-memdb"
	"github.com/hashicorp/nomad/nomad/structs"
	v1 "github.com/hashicorp/nomad/scheduler"
)

// process is wrapped in retryMax to iteratively run the handler until we have no
// further work, or we've made the maximum number of attempts.
func (s *scheduler) process() (bool, error) {
	var err error

	// Perform initialization operations that can fail first.
	ws := memdb.NewWatchSet()
	if err = s.initJob(ws); err != nil {
		return false, err
	}
	if err = s.strategy.initDeployment(ws); err != nil {
		return false, err
	}

	s.queuedAllocs = s.initAllocQueue()
	s.followUpEvals = nil
	s.plan = s.eval.MakePlan(s.job)
	s.failedTGAllocs = nil
	s.ctx = v1.NewEvalContext(s.eventsCh, s.state, s.plan, s.logger)
	s.strategy.initStack()

	// Compute the target job allocations
	if err = s.computeJobAllocs(); err != nil {
		s.logger.Error("failed to compute job allocations", "error", err)
		return false, err
	}

	if err = s.maybeCreateBlockedEval(); err != nil {
		return false, err
	}

	// TODO: Should/can we call this before the blocked eval logic?
	// If the plan is a no-op, we can bail. If AnnotatePlan is set submit the plan
	// anyways to get the annotations.
	if s.plan.IsNoOp() && !s.eval.AnnotatePlan {
		return true, nil
	}

	if err = s.maybeCreateFollowUpEvals(); err != nil {
		return false, err
	}

	return s.trySubmitPlan()
}

func (s *scheduler) initJob(ws memdb.WatchSet) error {
	var err error
	s.job, err = s.state.JobByID(ws, s.eval.Namespace, s.eval.JobID)
	if err != nil {
		return fmt.Errorf("failed to get job %q: %v", s.eval.JobID, err)
	}
	return nil
}

func (s *scheduler) initAllocQueue() map[string]int {
	numTaskGroups := 0
	if !s.job.Stopped() {
		numTaskGroups = len(s.job.TaskGroups)
	}
	return make(map[string]int, numTaskGroups)
}

// TODO: This feels like its changed over time and needs reconsideration.
// If there are failed allocations, we need to create a blocked evaluation
// to place the failed allocations when resources become available. If the
// current evaluation is already a blocked eval, we reuse it. If not, submit
// a new eval to the planner in createBlockedEval. If rescheduling should
// be delayed, do that instead.
func (s *scheduler) delayInstead() bool {
	return len(s.followUpEvals) > 0 && s.eval.WaitUntil.IsZero()
}

// TODO: This feels like it's defensive code guarding against the compute operations
// failing to create a blocked eval. Sure seems like this should be handled there.
// This condition is triggered because:
// * The eval status is not blocked
// * No blocked eval was created by the compute operations
// * Failed allocations were detected by the compute operations
// * Followup evals were created by the compute operations
// * The eval is not configured with a delay (WaitUntil)
func (s *scheduler) maybeCreateBlockedEval() error {
	if s.eval.Status != structs.EvalStatusBlocked &&
		s.blocked == nil &&
		len(s.failedTGAllocs) != 0 &&
		!s.delayInstead() {

		if err := s.createBlockedEval(false); err != nil {
			s.logger.Error("failed to make blocked eval", "error", err)
			return err
		}

		s.logger.Debug("failed to place all allocations, blocked eval created", "blocked_eval_id", s.blocked.ID)
	}

	return nil
}

// Create follow up evals for any delayed reschedule eligible allocations, except in
// the case that this evaluation was already delayed.
func (s *scheduler) maybeCreateFollowUpEvals() error {
	if !s.delayInstead() {
		return nil
	}

	for _, eval := range s.followUpEvals {
		eval.PreviousEval = s.eval.ID
		// TODO(preetha) this should be batching evals before inserting them
		if err := s.planner.CreateEval(eval); err != nil {
			s.logger.Error("failed to make next eval for rescheduling", "error", err)
			return err
		}
		s.logger.Debug("found reschedulable allocs, followup eval created", "followup_eval_id", eval.ID)
	}

	return nil
}

func (s *scheduler) adjustQueuedAllocations() {
	// adjustQueuedAllocations decrements the number of allocations pending per task
	// group based on the number of allocations successfully placed
	if s.planResult == nil {
		return
	}

	for _, allocations := range s.planResult.NodeAllocation {
		for _, allocation := range allocations {
			// Ensure that the allocation is newly created. We check that
			// the CreateIndex is equal to the ModifyIndex in order to check
			// that the allocation was just created. We do not check that
			// the CreateIndex is equal to the results AllocIndex because
			// the allocations we get back have gone through the planner's
			// optimistic snapshot and thus their indexes may not be
			// correct, but they will be consistent.
			if allocation.CreateIndex != allocation.ModifyIndex {
				continue
			}

			if _, ok := s.queuedAllocs[allocation.TaskGroup]; ok {
				s.queuedAllocs[allocation.TaskGroup]--
			} else {
				s.logger.Error("allocation placed but task group is not in list of unplaced allocations", "task_group", allocation.TaskGroup)
			}
		}
	}
}

func (s *scheduler) trySubmitPlan() (bool, error) {
	// Submit the plan and store the results.
	var err error
	var newState v1.State
	s.planResult, newState, err = s.planner.SubmitPlan(s.plan)
	if err != nil {
		return false, err
	}

	// TODO: Validate moving this before adjustQueuedAllocations is ok.
	// Should be fine since we zero re-init on each try.
	// TODO: This might be a minor speed improvement in v1.
	// If we got a state refresh, try again since we have stale data
	if newState != nil {
		s.logger.Debug("refresh forced")
		s.state = newState
		return false, nil
	}

	// Decrement the number of allocations pending per task group based on the
	// number of allocations successfully placed
	s.adjustQueuedAllocations()

	// Try again if the plan was not fully committed, potential conflict
	fullCommit, expected, actual := s.planResult.FullCommit(s.plan)
	if !fullCommit {
		s.logger.Debug("plan didn't fully commit", "attempted", expected, "placed", actual)
		if newState == nil {
			return false, fmt.Errorf("missing state refresh after partial commit")
		}
		return false, nil
	}

	return true, nil
}
