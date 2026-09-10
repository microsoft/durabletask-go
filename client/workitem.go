package client

import "time"

// workItemAbandonDelayError is an optional marker a work-item processing error
// can implement to ask the worker to defer redelivery of that work item.
type workItemAbandonDelayError interface {
	error
	WorkItemAbandonDelay() time.Duration
}
