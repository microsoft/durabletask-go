package helpers

// RejectAllWorkItemFilterName is the sentinel orchestration/activity name a
// worker advertises when its registry matches nothing, so the scheduler sends
// it no work instead of falling back to unfiltered delivery.
const RejectAllWorkItemFilterName = "\x00__durabletask_reject_all__"
