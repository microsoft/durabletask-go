// Package durabletaskscheduler configures Durable Task Scheduler (DTS)
// management clients and workers.
package durabletaskscheduler

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/task"
	"google.golang.org/grpc"
)

const (
	// DefaultResourceID is the Azure resource used to request DTS access tokens.
	DefaultResourceID = "https://durabletask.io"

	// DefaultMaxReceiveMessageSize and DefaultMaxSendMessageSize are the default
	// per-message gRPC bounds.
	DefaultMaxReceiveMessageSize = 64 * 1024 * 1024
	DefaultMaxSendMessageSize    = 64 * 1024 * 1024
	// DefaultKeepaliveTime and DefaultKeepaliveTimeout configure pings while a
	// gRPC stream is active.
	DefaultKeepaliveTime    = 2 * time.Minute
	DefaultKeepaliveTimeout = 20 * time.Second
	minimumKeepaliveTime    = 30 * time.Second
	minimumGRPCMessageSize  = 64 * 1024
)

type AuthenticationType string

const (
	AuthenticationNone               AuthenticationType = "None"
	AuthenticationDefaultAzure       AuthenticationType = "DefaultAzure"
	AuthenticationManagedIdentity    AuthenticationType = "ManagedIdentity"
	AuthenticationWorkloadIdentity   AuthenticationType = "WorkloadIdentity"
	AuthenticationEnvironment        AuthenticationType = "Environment"
	AuthenticationAzureCLI           AuthenticationType = "AzureCLI"
	AuthenticationAzurePowerShell    AuthenticationType = "AzurePowerShell"
	AuthenticationInteractiveBrowser AuthenticationType = "InteractiveBrowser"
	AuthenticationTokenCredential    AuthenticationType = "TokenCredential"
)

// Options configures connections to a Durable Task Scheduler endpoint.
type Options struct {
	EndpointAddress string
	TaskHubName     string
	Authentication  AuthenticationType

	// Credential is required by, and valid only for, AuthenticationTokenCredential.
	Credential azcore.TokenCredential

	// ResourceID is the DTS resource the access token is requested for. The
	// requested scope is the resource ID without surrounding whitespace or
	// trailing slashes plus "/.default". Exactly empty uses DefaultResourceID;
	// a whitespace-only value is rejected.
	ResourceID string

	// ClientID is used by ManagedIdentity, WorkloadIdentity, and
	// InteractiveBrowser. Other modes ignore it, except None and TokenCredential,
	// which reject it.
	ClientID string

	// TenantID is used by DefaultAzure, WorkloadIdentity, AzureCLI,
	// AzurePowerShell, and InteractiveBrowser. Environment and ManagedIdentity
	// ignore it; None and TokenCredential reject it.
	TenantID string

	// TokenFilePath is used only by WorkloadIdentity. Other modes ignore it,
	// except None and TokenCredential, which reject it.
	TokenFilePath string

	// AdditionallyAllowedTenants is used by DefaultAzure, WorkloadIdentity,
	// AzureCLI, AzurePowerShell, and InteractiveBrowser. Environment reads
	// AZURE_ADDITIONALLY_ALLOWED_TENANTS instead and ManagedIdentity ignores it;
	// None and TokenCredential reject it. Empty entries are dropped.
	AdditionallyAllowedTenants []string

	// AllowInsecureConnection must be true to use an http:// endpoint. Plaintext
	// connections are supported only when Authentication is None.
	AllowInsecureConnection bool

	// WorkerID overrides the generated worker identity. It is ignored by clients.
	WorkerID string

	// UserAgent overrides the x-user-agent metadata value.
	UserAgent string

	// HelloTimeout controls fail-fast connectivity checks.
	HelloTimeout time.Duration

	// MaxReceiveMessageSize and MaxSendMessageSize bound individual gRPC
	// messages. Zero uses the 64 MiB SDK default.
	MaxReceiveMessageSize int
	MaxSendMessageSize    int

	// KeepaliveTime controls client pings while a gRPC stream is active. Zero
	// disables keepalive, in which case KeepaliveTimeout is ignored.
	// KeepaliveTimeout bounds each ping acknowledgement.
	KeepaliveTime    time.Duration
	KeepaliveTimeout time.Duration

	// MaximumTimerInterval limits one physical durable timer action. Longer
	// timers are split deterministically. Zero uses the three-day default.
	MaximumTimerInterval time.Duration

	// ChannelRecreateFailureThreshold controls how many consecutive transport
	// failures cause the owned management client to create a new gRPC channel.
	// A non-positive value disables recreation. NewOptions defaults to 5.
	ChannelRecreateFailureThreshold int

	// ChannelRecreateMinInterval limits how often the owned management client
	// attempts channel recreation. NewOptions defaults to 30 seconds.
	ChannelRecreateMinInterval time.Duration

	// UnaryInterceptors and StreamInterceptors are applied in order to every
	// gRPC channel created for clients and workers, including replacements.
	UnaryInterceptors  []grpc.UnaryClientInterceptor
	StreamInterceptors []grpc.StreamClientInterceptor

	// LargePayloads enables external payload references for clients and workers.
	LargePayloads *api.LargePayloadOptions

	// Versioning configures worker version acceptance and the default version
	// applied by both the client and worker.
	Versioning *task.VersioningOptions

	// DataConverter configures application payload serialization for both clients and workers.
	DataConverter api.DataConverter

	dialer func(context.Context, string) (net.Conn, error)
}

func NewOptions(endpointAddress, taskHubName string) *Options {
	return &Options{
		EndpointAddress:                 endpointAddress,
		TaskHubName:                     taskHubName,
		Authentication:                  AuthenticationDefaultAzure,
		ResourceID:                      DefaultResourceID,
		HelloTimeout:                    30 * time.Second,
		MaxReceiveMessageSize:           DefaultMaxReceiveMessageSize,
		MaxSendMessageSize:              DefaultMaxSendMessageSize,
		KeepaliveTime:                   DefaultKeepaliveTime,
		KeepaliveTimeout:                DefaultKeepaliveTimeout,
		MaximumTimerInterval:            task.DefaultMaximumTimerInterval,
		ChannelRecreateFailureThreshold: 5,
		ChannelRecreateMinInterval:      30 * time.Second,
	}
}

func NewOptionsWithCredential(endpointAddress, taskHubName string, credential azcore.TokenCredential) *Options {
	options := NewOptions(endpointAddress, taskHubName)
	options.Authentication = AuthenticationTokenCredential
	options.Credential = credential
	return options
}

// NewOptionsFromConnectionString parses the DTS connection-string form:
//
//	Endpoint=<url>;TaskHub=<name>;Authentication=<type>
//
// Keys and Authentication values are case-insensitive, surrounding whitespace is
// trimmed, empty segments are skipped, and a repeated key uses its last value.
// Supported optional keys are ClientID, TenantID, TokenFilePath, and the
// comma-separated AdditionallyAllowedTenants.
func NewOptionsFromConnectionString(connectionString string) (*Options, error) {
	values := make(map[string]string)
	for _, segment := range strings.Split(connectionString, ";") {
		segment = strings.TrimSpace(segment)
		if segment == "" {
			continue
		}
		keyValue := strings.SplitN(segment, "=", 2)
		if len(keyValue) != 2 {
			return nil, fmt.Errorf("invalid connection string segment %q", segment)
		}
		key := strings.ToLower(strings.TrimSpace(keyValue[0]))
		value := strings.TrimSpace(keyValue[1])
		switch key {
		case "endpoint", "taskhub", "authentication", "clientid", "tenantid", "tokenfilepath", "additionallyallowedtenants":
			values[key] = value
		default:
			return nil, fmt.Errorf("unsupported connection string key %q", keyValue[0])
		}
	}

	endpoint, ok := values["endpoint"]
	if !ok || endpoint == "" {
		return nil, fmt.Errorf("connection string is missing required Endpoint")
	}
	taskHub, ok := values["taskhub"]
	if !ok || taskHub == "" {
		return nil, fmt.Errorf("connection string is missing required TaskHub")
	}
	authentication, ok := values["authentication"]
	if !ok || authentication == "" {
		return nil, fmt.Errorf("connection string is missing required Authentication")
	}

	options := NewOptions(endpoint, taskHub)
	parsedAuthentication, err := parseAuthenticationType(authentication)
	if err != nil {
		return nil, err
	}
	options.Authentication = parsedAuthentication
	options.ClientID = values["clientid"]
	options.TenantID = values["tenantid"]
	options.TokenFilePath = values["tokenfilepath"]
	options.AdditionallyAllowedTenants = normalizeAdditionallyAllowedTenants(
		strings.Split(values["additionallyallowedtenants"], ","),
	)
	if options.Authentication == AuthenticationNone {
		options.AllowInsecureConnection = true
	}
	if err := options.Validate(); err != nil {
		return nil, err
	}
	return options, nil
}

func parseAuthenticationType(value string) (AuthenticationType, error) {
	for _, authentication := range []AuthenticationType{
		AuthenticationDefaultAzure,
		AuthenticationManagedIdentity,
		AuthenticationWorkloadIdentity,
		AuthenticationEnvironment,
		AuthenticationAzureCLI,
		AuthenticationAzurePowerShell,
		AuthenticationInteractiveBrowser,
		AuthenticationNone,
	} {
		if strings.EqualFold(value, string(authentication)) {
			return authentication, nil
		}
	}
	switch {
	case strings.EqualFold(value, string(AuthenticationTokenCredential)):
		return "", fmt.Errorf(
			"authentication %q cannot be set from a connection string; use NewOptionsWithCredential",
			value,
		)
	case strings.EqualFold(value, "VisualStudio"), strings.EqualFold(value, "VisualStudioCode"):
		return "", fmt.Errorf(
			"authentication %q has no Azure Identity for Go equivalent; provide an explicit TokenCredential",
			value,
		)
	default:
		return "", fmt.Errorf("unsupported Authentication value %q", value)
	}
}

func (o *Options) Validate() error {
	if o == nil {
		return fmt.Errorf("DTS options are required")
	}
	if strings.TrimSpace(o.TaskHubName) == "" {
		return fmt.Errorf("DTS task hub name is required")
	}
	if o.TaskHubName != strings.TrimSpace(o.TaskHubName) {
		return fmt.Errorf("DTS task hub name cannot have leading or trailing whitespace")
	}
	if strings.ContainsAny(o.TaskHubName, "\r\n") {
		return fmt.Errorf("DTS task hub name cannot contain newlines")
	}
	if o.WorkerID != strings.TrimSpace(o.WorkerID) || strings.ContainsAny(o.WorkerID, "\r\n") {
		return fmt.Errorf("DTS worker ID cannot contain leading/trailing whitespace or newlines")
	}
	if o.UserAgent != strings.TrimSpace(o.UserAgent) || strings.ContainsAny(o.UserAgent, "\r\n") {
		return fmt.Errorf("DTS user agent cannot contain leading/trailing whitespace or newlines")
	}
	if o.HelloTimeout < 0 {
		return fmt.Errorf("DTS Hello timeout cannot be negative")
	}
	if o.MaxReceiveMessageSize < 0 {
		return fmt.Errorf("DTS maximum receive message size cannot be negative")
	}
	if o.MaxReceiveMessageSize > 0 && o.MaxReceiveMessageSize < minimumGRPCMessageSize {
		return fmt.Errorf("DTS maximum receive message size cannot be less than %d", minimumGRPCMessageSize)
	}
	if o.MaxSendMessageSize < 0 {
		return fmt.Errorf("DTS maximum send message size cannot be negative")
	}
	if o.MaxSendMessageSize > 0 && o.MaxSendMessageSize < minimumGRPCMessageSize {
		return fmt.Errorf("DTS maximum send message size cannot be less than %d", minimumGRPCMessageSize)
	}
	if o.KeepaliveTime < 0 {
		return fmt.Errorf("DTS keepalive time cannot be negative")
	}
	if o.KeepaliveTime > 0 && o.KeepaliveTime < minimumKeepaliveTime {
		return fmt.Errorf("DTS keepalive time cannot be less than %s", minimumKeepaliveTime)
	}
	if o.KeepaliveTimeout < 0 {
		return fmt.Errorf("DTS keepalive timeout cannot be negative")
	}
	if o.KeepaliveTime > 0 && o.KeepaliveTimeout >= o.KeepaliveTime {
		return fmt.Errorf("DTS keepalive timeout must be less than keepalive time")
	}
	if o.MaximumTimerInterval < 0 {
		return fmt.Errorf("DTS maximum timer interval cannot be negative")
	}
	if o.ChannelRecreateMinInterval < 0 {
		return fmt.Errorf("DTS channel recreate minimum interval cannot be negative")
	}
	if strings.ContainsAny(o.ResourceID, "\r\n") {
		return fmt.Errorf("DTS resource ID cannot contain newlines")
	}
	if o.ResourceID != "" && strings.TrimSpace(o.ResourceID) == "" {
		return fmt.Errorf("DTS resource ID cannot be blank")
	}
	if _, err := api.NormalizeLargePayloadOptions(o.LargePayloads); err != nil {
		return fmt.Errorf("invalid DTS large payload options: %w", err)
	}
	if o.Versioning != nil {
		if err := o.Versioning.Validate(); err != nil {
			return fmt.Errorf("invalid DTS versioning options: %w", err)
		}
	}
	for _, interceptor := range o.UnaryInterceptors {
		if interceptor == nil {
			return fmt.Errorf("DTS unary interceptors cannot contain nil")
		}
	}
	for _, interceptor := range o.StreamInterceptors {
		if interceptor == nil {
			return fmt.Errorf("DTS stream interceptors cannot contain nil")
		}
	}

	endpoint, err := normalizeEndpoint(o.EndpointAddress)
	if err != nil {
		return err
	}
	authentication := o.Authentication
	if authentication == "" {
		authentication = AuthenticationDefaultAzure
	}
	switch authentication {
	case AuthenticationNone:
		if o.Credential != nil {
			return fmt.Errorf("DTS credential must be nil when Authentication is None")
		}
		if fields := o.identityFieldsInUse(); len(fields) > 0 {
			return fmt.Errorf(
				"DTS Authentication None does not use %s",
				strings.Join(fields, ", "),
			)
		}
	case AuthenticationDefaultAzure:
		if o.Credential != nil {
			return fmt.Errorf("use Authentication TokenCredential for an explicit DTS credential")
		}
	case AuthenticationManagedIdentity,
		AuthenticationWorkloadIdentity,
		AuthenticationEnvironment,
		AuthenticationAzureCLI,
		AuthenticationAzurePowerShell,
		AuthenticationInteractiveBrowser:
		if o.Credential != nil {
			return fmt.Errorf("DTS credential must be nil when Authentication is %s", o.Authentication)
		}
	case AuthenticationTokenCredential:
		if o.Credential == nil {
			return fmt.Errorf("DTS TokenCredential authentication requires a credential")
		}
		if fields := o.identityFieldsInUse(); len(fields) > 0 {
			return fmt.Errorf(
				"DTS Authentication TokenCredential does not use %s; configure them on the credential itself",
				strings.Join(fields, ", "),
			)
		}
	default:
		return fmt.Errorf("unsupported DTS authentication type %q", o.Authentication)
	}
	if endpoint.Scheme == "http" {
		if !o.AllowInsecureConnection {
			return fmt.Errorf("plaintext DTS endpoint requires AllowInsecureConnection")
		}
		if authentication != AuthenticationNone {
			return fmt.Errorf("plaintext DTS endpoint cannot be used with credentials")
		}
	}
	return nil
}

// identityFieldsInUse lists the Azure Identity configuration fields that carry a
// value. It is used to reject configuration that the selected authentication
// mode can never consume.
func (o *Options) identityFieldsInUse() []string {
	var fields []string
	if strings.TrimSpace(o.ClientID) != "" {
		fields = append(fields, "ClientID")
	}
	if strings.TrimSpace(o.TenantID) != "" {
		fields = append(fields, "TenantID")
	}
	if strings.TrimSpace(o.TokenFilePath) != "" {
		fields = append(fields, "TokenFilePath")
	}
	if len(normalizeAdditionallyAllowedTenants(o.AdditionallyAllowedTenants)) > 0 {
		fields = append(fields, "AdditionallyAllowedTenants")
	}
	return fields
}

func normalizeEndpoint(endpointAddress string) (*url.URL, error) {
	endpointAddress = strings.TrimSpace(endpointAddress)
	if endpointAddress == "" {
		return nil, fmt.Errorf("DTS endpoint is required")
	}
	if !strings.Contains(endpointAddress, "://") {
		endpointAddress = "https://" + endpointAddress
	}
	endpoint, err := url.Parse(endpointAddress)
	if err != nil {
		return nil, fmt.Errorf("invalid DTS endpoint: %w", err)
	}
	if endpoint.Scheme != "https" && endpoint.Scheme != "http" {
		return nil, fmt.Errorf("DTS endpoint scheme must be https or http")
	}
	if endpoint.Hostname() == "" {
		return nil, fmt.Errorf("DTS endpoint must include a host")
	}
	if endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" {
		return nil, fmt.Errorf("DTS endpoint cannot include user info, query parameters, or a fragment")
	}
	if endpoint.Path != "" && endpoint.Path != "/" {
		return nil, fmt.Errorf("DTS endpoint cannot include a path")
	}
	return endpoint, nil
}
