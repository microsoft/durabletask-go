package azuremanaged

import (
	"fmt"
	"os"
	"runtime/debug"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/google/uuid"
	"github.com/microsoft/durabletask-go/backend"
)

// DefaultResourceID is the default resource for which DTS access tokens are requested.
const DefaultResourceID = "https://durabletask.io"

// Options configures a connection to an Azure-managed Durable Task Scheduler (DTS) task hub.
type Options struct {
	// Endpoint is the DTS endpoint address, e.g. "myscheduler.westus2.durabletask.io" or
	// "myscheduler.westus2.durabletask.io:443". A scheme (https://) is optional and ignored;
	// when no port is specified, 443 is assumed.
	Endpoint string

	// TaskHub is the name of the task hub resource. Required.
	TaskHub string

	// Credential authenticates calls to DTS. When nil, no bearer token is attached and an
	// insecure (non-TLS) channel is used — only valid for local or emulator scenarios.
	Credential azcore.TokenCredential

	// ResourceID is the resource for which access tokens are requested. Defaults to
	// DefaultResourceID. The token scope is "<ResourceID>/.default".
	ResourceID string

	// Insecure forces an insecure (non-TLS) channel. It is implied when Credential is nil.
	Insecure bool

	// Logger is the logger used by the underlying client/worker. Defaults to backend.DefaultLogger().
	Logger backend.Logger
}

// optionsFromConnectionString builds Options by parsing a DTS connection string of the form
// "Endpoint=<address>;Authentication=<type>;TaskHub=<name>".
func optionsFromConnectionString(cs string) (*Options, error) {
	parsed, err := parseConnectionString(cs)
	if err != nil {
		return nil, err
	}
	endpoint, err := parsed.getRequired("Endpoint")
	if err != nil {
		return nil, err
	}
	taskHub, err := parsed.getRequired("TaskHub")
	if err != nil {
		return nil, err
	}
	credential, err := credentialFromConnectionString(parsed)
	if err != nil {
		return nil, err
	}
	return &Options{
		Endpoint:   endpoint,
		TaskHub:    taskHub,
		Credential: credential,
		// "Authentication=None" yields a nil credential, which implies an insecure channel.
		Insecure: credential == nil,
	}, nil
}

// validate checks required fields and applies defaults that don't depend on the call kind.
func (o *Options) validate() error {
	if o == nil {
		return fmt.Errorf("options cannot be nil")
	}
	if strings.TrimSpace(o.TaskHub) == "" {
		return fmt.Errorf("the TaskHub value cannot be empty")
	}
	if strings.TrimSpace(o.Endpoint) == "" {
		return fmt.Errorf("the Endpoint value cannot be empty")
	}
	return nil
}

func (o *Options) resourceID() string {
	if o.ResourceID != "" {
		return o.ResourceID
	}
	return DefaultResourceID
}

func (o *Options) logger() backend.Logger {
	if o.Logger != nil {
		return o.Logger
	}
	return backend.DefaultLogger()
}

// useInsecureChannel reports whether the channel should be insecure (non-TLS).
func (o *Options) useInsecureChannel() bool {
	return o.Insecure || o.Credential == nil
}

// normalizeEndpoint strips an optional scheme and ensures a port is present.
func normalizeEndpoint(endpoint string) string {
	endpoint = strings.TrimPrefix(endpoint, "https://")
	endpoint = strings.TrimPrefix(endpoint, "http://")
	endpoint = strings.TrimSuffix(endpoint, "/")
	if !strings.Contains(endpoint, ":") {
		endpoint += ":443"
	}
	return endpoint
}

// userAgent returns the value of the x-user-agent header, e.g. "durabletask-go/v0.7.0".
func userAgent() string {
	version := "unknown"
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, dep := range info.Deps {
			if dep.Path == "github.com/microsoft/durabletask-go" && dep.Version != "" {
				version = dep.Version
				break
			}
		}
	}
	return "durabletask-go/" + version
}

// generateWorkerID returns a worker identifier of the form "<hostname>:<pid>:<uuid>".
func generateWorkerID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	return fmt.Sprintf("%s:%d:%s", host, os.Getpid(), uuid.NewString())
}
