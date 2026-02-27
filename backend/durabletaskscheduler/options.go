package durabletaskscheduler

import (
	"fmt"
	"strings"
)

const (
	// DefaultResourceID is the default Azure resource ID used for token authentication with DTS.
	DefaultResourceID = "https://durabletask.io"

	// DefaultAuthType is the default authentication type.
	DefaultAuthType = AuthTypeDefaultAzure
)

// AuthType represents the authentication type for connecting to DTS.
type AuthType string

const (
	// AuthTypeNone disables authentication (for local development).
	AuthTypeNone AuthType = "none"

	// AuthTypeDefaultAzure uses DefaultAzureCredential from the Azure Identity SDK.
	AuthTypeDefaultAzure AuthType = "defaultazure"
)

// Options configures the connection to a Durable Task Scheduler (DTS) endpoint.
type Options struct {
	// EndpointAddress is the DTS endpoint URL (e.g., "https://myscheduler.westus2.durabletask.io").
	EndpointAddress string

	// TaskHubName is the name of the task hub resource.
	TaskHubName string

	// AuthType specifies how to authenticate with the DTS service.
	AuthType AuthType

	// ResourceID is the Azure resource ID used for token scoping. Defaults to "https://durabletask.io".
	ResourceID string
}

// NewOptions creates a new Options with the given endpoint and task hub name.
func NewOptions(endpointAddress, taskHubName string) *Options {
	return &Options{
		EndpointAddress: endpointAddress,
		TaskHubName:     taskHubName,
		AuthType:        DefaultAuthType,
		ResourceID:      DefaultResourceID,
	}
}

// NewOptionsFromConnectionString parses a connection string into Options.
//
// The connection string format is:
//
//	Endpoint=https://{scheduler-name}.{region}.durabletask.io;TaskHub={taskhub-name};Authentication={auth-type}
//
// Required keys: Endpoint, TaskHub.
// Optional keys: Authentication (defaults to "defaultazure").
func NewOptionsFromConnectionString(connectionString string) (*Options, error) {
	opts := &Options{
		AuthType:   DefaultAuthType,
		ResourceID: DefaultResourceID,
	}

	parts := strings.Split(connectionString, ";")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("invalid connection string segment: %q", part)
		}

		key := strings.TrimSpace(kv[0])
		value := strings.TrimSpace(kv[1])

		switch strings.ToLower(key) {
		case "endpoint":
			opts.EndpointAddress = value
		case "taskhub":
			opts.TaskHubName = value
		case "authentication":
			opts.AuthType = AuthType(strings.ToLower(value))
		default:
			// Ignore unknown keys for forward compatibility
		}
	}

	if opts.EndpointAddress == "" {
		return nil, fmt.Errorf("connection string is missing required 'Endpoint' key")
	}
	if opts.TaskHubName == "" {
		return nil, fmt.Errorf("connection string is missing required 'TaskHub' key")
	}

	return opts, nil
}
