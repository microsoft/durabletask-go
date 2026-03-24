package helpers

import (
	"time"

	"github.com/microsoft/durabletask-go/internal/protos"
)

// GetVisibleTime returns the visibility time for a new event when it should be delayed.
func GetVisibleTime(e *protos.HistoryEvent) *time.Time {
	if e == nil || e.Timestamp == nil {
		return nil
	}

	visibleTime := e.Timestamp.AsTime()
	if !visibleTime.After(time.Now()) {
		return nil
	}

	return &visibleTime
}
