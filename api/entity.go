package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/microsoft/durabletask-go/internal/helpers"
	"github.com/microsoft/durabletask-go/internal/protos"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

var (
	// ErrEntityStateNotIncluded indicates that entity state was excluded from a metadata response.
	ErrEntityStateNotIncluded = errors.New("entity state was not included")
	// ErrEntityHasNoState indicates that an entity metadata response contains no application state.
	ErrEntityHasNoState = errors.New("entity has no state")
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
	return EntityID{Name: helpers.ToLowerInvariant(name), Key: key}
}

// String returns the entity instance ID in the format "@<name>@<key>".
func (e EntityID) String() string {
	return fmt.Sprintf("@%s@%s", helpers.ToLowerInvariant(e.Name), e.Key)
}

// MarshalJSON serializes entity IDs using the cross-SDK compact instance ID format.
func (e EntityID) MarshalJSON() ([]byte, error) {
	if err := helpers.ValidateEntityName(e.Name); err != nil {
		return nil, err
	}
	return json.Marshal(e.String())
}

// UnmarshalJSON parses an entity ID from the cross-SDK compact instance ID format.
func (e *EntityID) UnmarshalJSON(data []byte) error {
	if e == nil {
		return fmt.Errorf("entity ID target must not be nil")
	}
	var value string
	if err := json.Unmarshal(data, &value); err != nil {
		return fmt.Errorf("entity ID must be a compact string: %w", err)
	}
	parsed, err := EntityIDFromString(value)
	if err != nil {
		return err
	}
	*e = parsed
	return nil
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
	StateIncluded    bool
	HasState         bool
	SerializedState  string
	Converter        DataConverter `json:"-"`
}

// SignalEntityOptions is a functional option type for signaling an entity.
type SignalEntityOptions func(*protos.SignalEntityRequest, DataConverter) error

// WithSignalInput configures the input for an entity signal.
func WithSignalInput(input any) SignalEntityOptions {
	return func(req *protos.SignalEntityRequest, converter DataConverter) error {
		if isNilEntityValue(input) {
			req.Input = nil
			return nil
		}
		payload, err := SerializeData(converter, input)
		if err != nil {
			return err
		}
		req.Input = wrapperspb.String(payload)
		return nil
	}
}

func isNilEntityValue(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}

// WithRawSignalInput configures a raw string input for an entity signal.
func WithRawSignalInput(input string) SignalEntityOptions {
	return func(req *protos.SignalEntityRequest, _ DataConverter) error {
		req.Input = wrapperspb.String(input)
		return nil
	}
}

// WithSignalScheduledTime configures a scheduled time for the entity signal.
func WithSignalScheduledTime(t time.Time) SignalEntityOptions {
	return func(req *protos.SignalEntityRequest, _ DataConverter) error {
		req.ScheduledTime = timestamppb.New(t)
		return nil
	}
}

// ReadState deserializes entity state with the metadata converter.
func (m *EntityMetadata) ReadState(target any) error {
	if !m.StateIncluded {
		return ErrEntityStateNotIncluded
	}
	if !m.HasState {
		return ErrEntityHasNoState
	}
	if target == nil {
		return nil
	}
	return NormalizeDataConverter(m.Converter).Deserialize(m.SerializedState, target)
}

// GetEntityOptions controls an entity metadata request. The zero value includes state.
type GetEntityOptions struct {
	ExcludeState bool
}

// EntityQuery defines filter criteria for querying entities.
type EntityQuery struct {
	// InstanceIDStartsWith filters entities whose instance ID starts with this prefix.
	InstanceIDStartsWith string
	// LastModifiedFrom filters entities modified on or after this time.
	LastModifiedFrom time.Time
	// LastModifiedTo filters entities modified before this time.
	LastModifiedTo time.Time
	// ExcludeState omits entity state from the results. State is included by default.
	ExcludeState bool
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

// CleanEntityStorageOptions controls entity storage cleanup. The zero value
// removes empty entities, releases orphaned locks, and continues to completion.
type CleanEntityStorageOptions struct {
	// ContinuationToken for resuming a previous cleanup operation.
	ContinuationToken string
	// PreserveEmptyEntities disables removal of entities with no state and no locks.
	PreserveEmptyEntities bool
	// PreserveOrphanedLocks disables release of locks held by non-running orchestrations.
	PreserveOrphanedLocks bool
	// SinglePage returns after one backend cleanup request.
	SinglePage bool
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
