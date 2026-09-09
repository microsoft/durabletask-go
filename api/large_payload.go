package api

import (
	"context"
	"errors"
	"strings"
)

const (
	DefaultLargePayloadThresholdBytes     = 64 * 1024
	DefaultLargePayloadMaxBytes           = 64 * 1024 * 1024
	DefaultAzureBlobPayloadThresholdBytes = 256 * 1024
	DefaultAzureBlobPayloadMaxBytes       = 10 * 1024 * 1024
	DurableTaskPayloadReferencePrefix     = "durabletask-payload:v1:"
	AzureBlobPayloadReferencePrefixV1     = "blob:v1:"
	AzureBlobPayloadReferencePrefixV2     = "blob:v2:"
)

var (
	ErrLargePayloadTooLarge  = errors.New("large payload exceeds the configured size limit")
	ErrLargePayloadReference = errors.New("invalid large payload reference")
	ErrLargePayloadIntegrity = errors.New("large payload integrity check failed")
)

// IsLargePayloadReference reports whether value uses a reserved Durable Task
// large-payload reference prefix. Recognized values must be validated and
// resolved rather than treated as application data.
func IsLargePayloadReference(value string) bool {
	return strings.HasPrefix(value, DurableTaskPayloadReferencePrefix) ||
		strings.HasPrefix(value, AzureBlobPayloadReferencePrefixV1) ||
		strings.HasPrefix(value, AzureBlobPayloadReferencePrefixV2)
}

// LargePayloadStore externalizes payload bytes and returns an opaque location.
type LargePayloadStore interface {
	Store(context.Context, []byte) (string, error)
}

// LargePayloadResolver resolves payload bytes from an opaque, untrusted location.
// Implementations must validate schemes, accounts, paths, and other allow-list
// constraints before reading data.
type LargePayloadResolver interface {
	Resolve(context.Context, string) ([]byte, error)
}

// LargePayloadTokenStore supports a native payload token format. Native token
// stores are useful when a backing service already has an interoperable token
// representation, such as Azure Blob Storage's blob:v2 URLs.
type LargePayloadTokenStore interface {
	StoreToken(context.Context, []byte) (string, error)
	ResolveToken(context.Context, string) ([]byte, error)
	IsLargePayloadToken(string) bool
}

// LargePayloadTokenValidator validates recognized native tokens before they
// are preserved or resolved.
type LargePayloadTokenValidator interface {
	ValidateLargePayloadToken(string) error
}

// LargePayloadDefaults supplies storage-specific defaults to use when an
// option is omitted. It is intentionally optional to preserve legacy defaults.
type LargePayloadDefaults interface {
	LargePayloadDefaults() (thresholdBytes, maxPayloadBytes int)
}

// InclusiveLargePayloadThreshold identifies stores whose threshold includes
// payloads exactly equal to the configured threshold.
type InclusiveLargePayloadThreshold interface {
	UsesInclusiveLargePayloadThreshold() bool
}

// LargePayloadOptionsValidator performs store-specific option validation after
// defaults are applied.
type LargePayloadOptionsValidator interface {
	ValidateLargePayloadOptions(thresholdBytes, maxPayloadBytes int) error
}

// LargePayloadOptions configures payload externalization and hydration.
type LargePayloadOptions struct {
	Store      LargePayloadStore
	Resolver   LargePayloadResolver
	TokenStore LargePayloadTokenStore
	// ThresholdBytes uses storage-specific defaults when zero.
	ThresholdBytes int
	// MaxPayloadBytes uses storage-specific defaults when zero.
	MaxPayloadBytes int
}

// NormalizeLargePayloadOptions validates options and applies generic or
// store-specific defaults. A nil input returns nil, nil.
func NormalizeLargePayloadOptions(options *LargePayloadOptions) (*LargePayloadOptions, error) {
	if options == nil {
		return nil, nil
	}
	normalized := *options
	if normalized.Store == nil {
		return nil, WrapInvalidArgument(errors.New("large payload store is required"))
	}
	if normalized.Resolver == nil {
		return nil, WrapInvalidArgument(errors.New("large payload resolver is required"))
	}
	if normalized.TokenStore == nil {
		if tokenStore, ok := normalized.Store.(LargePayloadTokenStore); ok {
			normalized.TokenStore = tokenStore
		}
	}
	defaultThreshold, defaultMax := DefaultLargePayloadThresholdBytes, DefaultLargePayloadMaxBytes
	if defaults, ok := normalized.Store.(LargePayloadDefaults); ok {
		defaultThreshold, defaultMax = defaults.LargePayloadDefaults()
	}
	if normalized.ThresholdBytes == 0 {
		normalized.ThresholdBytes = defaultThreshold
	}
	if normalized.MaxPayloadBytes == 0 {
		normalized.MaxPayloadBytes = defaultMax
	}
	if normalized.ThresholdBytes < 0 {
		return nil, WrapInvalidArgument(errors.New("large payload threshold cannot be negative"))
	}
	if normalized.MaxPayloadBytes <= 0 {
		return nil, WrapInvalidArgument(errors.New("large payload maximum must be greater than zero"))
	}
	if normalized.ThresholdBytes > normalized.MaxPayloadBytes {
		return nil, WrapInvalidArgument(
			errors.New("large payload threshold cannot exceed maximum payload size"),
		)
	}
	if validator, ok := normalized.Store.(LargePayloadOptionsValidator); ok {
		if err := validator.ValidateLargePayloadOptions(
			normalized.ThresholdBytes,
			normalized.MaxPayloadBytes,
		); err != nil {
			return nil, WrapInvalidArgument(err)
		}
	}
	return &normalized, nil
}
