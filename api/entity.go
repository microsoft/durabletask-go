package api

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// EntityID uniquely identifies an entity by its name and key.
type EntityID struct {
	Name string
	Key  string
}

// NewEntityID creates a new EntityID with the specified name and key.
func NewEntityID(name string, key string) EntityID {
	if err := helpers.ValidateEntityName(name); err != nil {
		panic(err)
	}
	return EntityID{Name: strings.ToLower(name), Key: key}
}

// String returns the entity instance ID in the format "@<name>@<key>".
func (e EntityID) String() string {
	return fmt.Sprintf("@%s@%s", strings.ToLower(e.Name), e.Key)
}

// EntityIDFromString parses an entity instance ID string in the format "@<name>@<key>".
func EntityIDFromString(s string) (EntityID, error) {
	name, key, err := helpers.ParseEntityInstanceID(s)
	if err != nil {
		return EntityID{}, err
	}
	return EntityID{Name: name, Key: key}, nil
}

// EntityMetadata contains metadata about an entity instance.
type EntityMetadata struct {
	InstanceID       EntityID
	LastModifiedTime time.Time
	BacklogQueueSize int32
	LockedBy         string
	SerializedState  string
}

// SignalEntityOptions is a functional option type for signaling an entity.
type SignalEntityOptions func(*protos.SignalEntityRequest) error

// WithSignalInput configures the input for an entity signal.
func WithSignalInput(input any) SignalEntityOptions {
	return func(req *protos.SignalEntityRequest) error {
		bytes, err := json.Marshal(input)
		if err != nil {
			return err
		}
		req.Input = wrapperspb.String(string(bytes))
		return nil
	}
}

// WithRawSignalInput configures a raw string input for an entity signal.
func WithRawSignalInput(input string) SignalEntityOptions {
	return func(req *protos.SignalEntityRequest) error {
		req.Input = wrapperspb.String(input)
		return nil
	}
}

// WithSignalScheduledTime configures a scheduled time for the entity signal.
func WithSignalScheduledTime(t time.Time) SignalEntityOptions {
	return func(req *protos.SignalEntityRequest) error {
		req.ScheduledTime = timestamppb.New(t)
		return nil
	}
}

// EntityQuery defines filter criteria for querying entities.
type EntityQuery struct {
	// InstanceIDStartsWith filters entities whose instance ID starts with this prefix.
	InstanceIDStartsWith string
	// LastModifiedFrom filters entities modified on or after this time.
	LastModifiedFrom time.Time
	// LastModifiedTo filters entities modified before this time.
	LastModifiedTo time.Time
	// IncludeState whether to include entity state in the results.
	IncludeState bool
	// IncludeTransient whether to include transient (stateless) entities.
	IncludeTransient bool
	// PageSize limits the number of entities returned per page.
	PageSize int32
	// ContinuationToken for fetching the next page of results.
	ContinuationToken string
}

// EntityQueryResults contains the results of an entity query.
type EntityQueryResults struct {
	Entities          []*EntityMetadata
	ContinuationToken string
}

// CleanEntityStorageRequest contains options for cleaning entity storage.
type CleanEntityStorageRequest struct {
	// ContinuationToken for resuming a previous cleanup operation.
	ContinuationToken string
	// RemoveEmptyEntities removes entities with no state and no locks.
	RemoveEmptyEntities bool
	// ReleaseOrphanedLocks releases locks held by non-running orchestrations.
	ReleaseOrphanedLocks bool
}

// CleanEntityStorageResult contains the results of a cleanup operation.
type CleanEntityStorageResult struct {
	// EmptyEntitiesRemoved is the number of empty entities removed.
	EmptyEntitiesRemoved int32
	// OrphanedLocksReleased is the number of orphaned locks released.
	OrphanedLocksReleased int32
	// ContinuationToken for resuming cleanup. Empty if complete.
	ContinuationToken string
}
