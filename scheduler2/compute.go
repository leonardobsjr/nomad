package scheduler2

import (
	"fmt"

	log "github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-memdb"
	"github.com/hashicorp/nomad/nomad/structs"
)

// TODO: This and the reconciler is where the main performance and maintainability
// improvements should come. Hoping to introduce functional programming,
// and reduce iterations by using a new data structure based on maps, that reduce
// a lot of this logic from multiple O(n) operations to a limited number of O(1).

// computeJobAllocs is used to reconcile differences between the job,
// existing allocations and node status to update the allocations.
func (s *scheduler) computeJobAllocs() error {
	// TODO: Should/can I use the WatchSet from process? Could this improve v1 right now?
	// Lookup the allocations by JobID
	ws := memdb.NewWatchSet()
	allocs, err := s.state.AllocsByJob(ws, s.eval.Namespace, s.eval.JobID, true)
	if err != nil {
		return fmt.Errorf("failed to get allocs for job '%s': %v",
			s.eval.JobID, err)
	}

	// Determine the tainted nodes containing job allocs
	tainted, err := taintedNodes(s.state, allocs)
	if err != nil {
		return fmt.Errorf("failed to get tainted nodes for job '%s': %v",
			s.eval.JobID, err)
	}

	// Update the allocations which are in pending/running state on tainted
	// nodes to lost, but only if the scheduler has already marked them
	updateNonTerminalAllocsToLost(s.plan, tainted, allocs)

	reconciler := NewAllocReconciler(s.logger,
		genericAllocUpdateFn(s.ctx, s.stack, s.eval.ID),
		s.batch, s.eval.JobID, s.job, s.deployment, allocs, tainted, s.eval.ID, s.eval.Priority)
	results := reconciler.Compute()
	s.logger.Debug("reconciled current state with desired state", "results", log.Fmt("%#v", results))

	if s.eval.AnnotatePlan {
		s.plan.Annotations = &structs.PlanAnnotations{
			DesiredTGUpdates: results.desiredTGUpdates,
		}
	}

	// Add the deployment changes to the plan
	s.plan.Deployment = results.deployment
	s.plan.DeploymentUpdates = results.deploymentUpdates

	// Store all the follow up evaluations from rescheduled allocations
	if len(results.desiredFollowupEvals) > 0 {
		for _, evals := range results.desiredFollowupEvals {
			s.followUpEvals = append(s.followUpEvals, evals...)
		}
	}

	// Update the stored deployment
	if results.deployment != nil {
		s.deployment = results.deployment
	}

	// Handle the stop
	for _, stop := range results.stop {
		s.plan.AppendStoppedAlloc(stop.alloc, stop.statusDescription, stop.clientStatus, stop.followupEvalID)
	}

	// Handle the in-place updates
	for _, update := range results.inplaceUpdate {
		if update.DeploymentID != s.deployment.GetID() {
			update.DeploymentID = s.deployment.GetID()
			update.DeploymentStatus = nil
		}
		s.ctx.Plan().AppendAlloc(update, nil)
	}

	// Handle the annotation updates
	for _, update := range results.attributeUpdates {
		s.ctx.Plan().AppendAlloc(update, nil)
	}

	// Nothing remaining to do if placement is not required
	if len(results.place)+len(results.destructiveUpdate) == 0 {
		// If the job has been purged we don't have access to the job. Otherwise
		// set the queued allocs to zero. This is true if the job is being
		// stopped as well.
		if s.job != nil {
			for _, tg := range s.job.TaskGroups {
				s.queuedAllocs[tg.Name] = 0
			}
		}
		return nil
	}

	// Record the number of allocations that needs to be placed per Task Group
	for _, place := range results.place {
		s.queuedAllocs[place.taskGroup.Name] += 1
	}
	for _, destructive := range results.destructiveUpdate {
		s.queuedAllocs[destructive.placeTaskGroup.Name] += 1
	}

	// Compute the placements
	place := make([]placementResult, 0, len(results.place))
	for _, p := range results.place {
		place = append(place, p)
	}

	destructive := make([]placementResult, 0, len(results.destructiveUpdate))
	for _, p := range results.destructiveUpdate {
		destructive = append(destructive, p)
	}
	return s.computePlacements(destructive, place)
}
