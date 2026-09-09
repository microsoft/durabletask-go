package task

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/microsoft/durabletask-go/api"
)

// VersionMatchStrategy determines which task versions a worker accepts.
type VersionMatchStrategy int

const (
	VersionMatchNone VersionMatchStrategy = iota
	VersionMatchStrict
	VersionMatchCurrentOrOlder
)

// VersionFailureStrategy determines how a worker handles a version mismatch.
type VersionFailureStrategy int

const (
	VersionFailureReject VersionFailureStrategy = iota
	VersionFailureFail
)

// UnversionedTaskVersion explicitly selects an unversioned task.
const UnversionedTaskVersion = ""

// VersioningOptions configures version-aware orchestration and activity dispatch.
type VersioningOptions struct {
	Version         string
	DefaultVersion  string
	MatchStrategy   VersionMatchStrategy
	FailureStrategy VersionFailureStrategy
}

// Validate checks that the versioning configuration is supported.
func (o VersioningOptions) Validate() error {
	switch o.MatchStrategy {
	case VersionMatchNone, VersionMatchStrict, VersionMatchCurrentOrOlder:
	default:
		return &versionConfigurationError{strategy: o.MatchStrategy}
	}
	switch o.FailureStrategy {
	case VersionFailureReject, VersionFailureFail:
	default:
		return fmt.Errorf("unknown version failure strategy %d", o.FailureStrategy)
	}
	if _, err := normalizeTaskVersionForRegistration(o.Version); err != nil {
		return fmt.Errorf("invalid worker version: %w", err)
	}
	if _, err := normalizeTaskVersionForRegistration(o.DefaultVersion); err != nil {
		return fmt.Errorf("invalid default version: %w", err)
	}
	return nil
}

// VersionMismatchError indicates that a work item is incompatible with this worker.
type VersionMismatchError struct {
	TaskVersion   string
	WorkerVersion string
	Strategy      VersionMatchStrategy
}

func (e *VersionMismatchError) Error() string {
	return fmt.Sprintf(
		"task version %q does not match worker version %q using strategy %d",
		e.TaskVersion,
		e.WorkerVersion,
		e.Strategy,
	)
}

func (*VersionMismatchError) DurableTaskErrorType() api.ErrorType {
	return versionMismatchErrorType
}

func (*VersionMismatchError) NonRetriable() bool {
	return true
}

func (*VersionMismatchError) Is(target error) bool {
	return target == api.ErrVersionMismatch
}

// WorkItemAbandonDelay asks the worker to delay redelivery of a
// version-incompatible activity work item instead of retrying it immediately.
func (*VersionMismatchError) WorkItemAbandonDelay() time.Duration {
	return time.Second
}

func (o *VersioningOptions) check(taskVersion string) error {
	if o == nil {
		return nil
	}
	if err := o.Validate(); err != nil {
		return err
	}
	if o.MatchStrategy == VersionMatchNone {
		return nil
	}
	comparison := compareVersions(taskVersion, o.Version)
	switch o.MatchStrategy {
	case VersionMatchStrict:
		if comparison == 0 {
			return nil
		}
	case VersionMatchCurrentOrOlder:
		if comparison <= 0 {
			return nil
		}
	default:
		return &versionConfigurationError{strategy: o.MatchStrategy}
	}

	return &VersionMismatchError{
		TaskVersion:   taskVersion,
		WorkerVersion: o.Version,
		Strategy:      o.MatchStrategy,
	}
}

func (o *VersioningOptions) defaultVersion() string {
	if o == nil {
		return ""
	}
	return o.DefaultVersion
}

type versionConfigurationError struct {
	strategy VersionMatchStrategy
}

func (e *versionConfigurationError) Error() string {
	return fmt.Sprintf("unknown version match strategy %d", e.strategy)
}

func (*versionConfigurationError) DurableTaskErrorType() api.ErrorType {
	return api.ErrorTypeVersionError
}

func (*versionConfigurationError) NonRetriable() bool {
	return true
}

func (*versionConfigurationError) Is(target error) bool {
	return target == api.ErrVersionMismatch
}

func compareVersions(left, right string) int {
	left = strings.TrimSpace(left)
	right = strings.TrimSpace(right)
	switch {
	case left == "" && right == "":
		return 0
	case left == "":
		return -1
	case right == "":
		return 1
	}

	leftParts, leftOK := numericVersion(left)
	rightParts, rightOK := numericVersion(right)
	if leftOK && rightOK {
		maxParts := max(len(leftParts), len(rightParts))
		for i := 0; i < maxParts; i++ {
			leftValue := -1
			if i < len(leftParts) {
				leftValue = leftParts[i]
			}
			rightValue := -1
			if i < len(rightParts) {
				rightValue = rightParts[i]
			}
			if leftValue < rightValue {
				return -1
			}
			if leftValue > rightValue {
				return 1
			}
		}
		return 0
	}
	return strings.Compare(strings.ToLower(left), strings.ToLower(right))
}

func numericVersion(version string) ([]int, bool) {
	parts := strings.Split(version, ".")
	if len(parts) < 2 || len(parts) > 4 {
		return nil, false
	}
	values := make([]int, len(parts))
	for i, part := range parts {
		value, err := strconv.Atoi(part)
		if err != nil || value < 0 {
			return nil, false
		}
		values[i] = value
	}
	return values, true
}
