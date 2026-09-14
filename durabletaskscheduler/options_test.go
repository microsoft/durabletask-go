package durabletaskscheduler

import (
	"testing"
	"time"

	"github.com/microsoft/durabletask-go/task"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestNewOptionsFromConnectionString(t *testing.T) {
	tests := []struct {
		name          string
		value         string
		wantEndpoint  string
		wantTaskHub   string
		wantAuth      AuthenticationType
		wantUnsafe    bool
		wantClientID  string
		wantTenantID  string
		wantTokenFile string
		wantTenants   []string
		wantErr       string
	}{
		{
			name:         "default azure",
			value:        "Endpoint=scheduler.example.com;TaskHub=hub;Authentication=DefaultAzure",
			wantEndpoint: "scheduler.example.com",
			wantTaskHub:  "hub",
			wantAuth:     AuthenticationDefaultAzure,
		},
		{
			name:         "default azure with tenant scoping",
			value:        "Endpoint=scheduler.example.com;TaskHub=hub;Authentication=DefaultAzure;TenantID=tenant;AdditionallyAllowedTenants=*",
			wantEndpoint: "scheduler.example.com",
			wantTaskHub:  "hub",
			wantAuth:     AuthenticationDefaultAzure,
			wantTenantID: "tenant",
			wantTenants:  []string{"*"},
		},
		{
			name:         "emulator",
			value:        "Endpoint=http://127.0.0.1:8080;TaskHub=default;Authentication=None",
			wantEndpoint: "http://127.0.0.1:8080",
			wantTaskHub:  "default",
			wantAuth:     AuthenticationNone,
			wantUnsafe:   true,
		},
		{
			name:         "managed identity",
			value:        "Endpoint=scheduler.example.com;TaskHub=hub;Authentication=ManagedIdentity;ClientID=client",
			wantEndpoint: "scheduler.example.com",
			wantTaskHub:  "hub",
			wantAuth:     AuthenticationManagedIdentity,
			wantClientID: "client",
		},
		{
			name:         "system-assigned managed identity",
			value:        "Endpoint=scheduler.example.com;TaskHub=hub;Authentication=ManagedIdentity",
			wantEndpoint: "scheduler.example.com",
			wantTaskHub:  "hub",
			wantAuth:     AuthenticationManagedIdentity,
		},
		{
			name:          "workload identity",
			value:         "Endpoint=scheduler.example.com;TaskHub=hub;Authentication=WorkloadIdentity;ClientID=client;TenantID=tenant;TokenFilePath=/token;AdditionallyAllowedTenants=one, two",
			wantEndpoint:  "scheduler.example.com",
			wantTaskHub:   "hub",
			wantAuth:      AuthenticationWorkloadIdentity,
			wantClientID:  "client",
			wantTenantID:  "tenant",
			wantTokenFile: "/token",
			wantTenants:   []string{"one", "two"},
		},
		{
			name:         "environment",
			value:        "Endpoint=scheduler.example.com;TaskHub=hub;Authentication=Environment",
			wantEndpoint: "scheduler.example.com",
			wantTaskHub:  "hub",
			wantAuth:     AuthenticationEnvironment,
		},
		{
			name:         "Azure CLI",
			value:        "Endpoint=scheduler.example.com;TaskHub=hub;Authentication=AzureCLI;TenantID=tenant",
			wantEndpoint: "scheduler.example.com",
			wantTaskHub:  "hub",
			wantAuth:     AuthenticationAzureCLI,
			wantTenantID: "tenant",
		},
		{
			name:         "Azure PowerShell",
			value:        "Endpoint=scheduler.example.com;TaskHub=hub;Authentication=AzurePowerShell;TenantID=tenant",
			wantEndpoint: "scheduler.example.com",
			wantTaskHub:  "hub",
			wantAuth:     AuthenticationAzurePowerShell,
			wantTenantID: "tenant",
		},
		{
			name:         "interactive browser",
			value:        "Endpoint=scheduler.example.com;TaskHub=hub;Authentication=InteractiveBrowser;ClientID=client;TenantID=tenant",
			wantEndpoint: "scheduler.example.com",
			wantTaskHub:  "hub",
			wantAuth:     AuthenticationInteractiveBrowser,
			wantClientID: "client",
			wantTenantID: "tenant",
		},
		{
			name:         "surrounding whitespace and empty segments are ignored",
			value:        "  Endpoint = scheduler.example.com ;; TaskHub =\thub\t; Authentication = AzureCLI ;",
			wantEndpoint: "scheduler.example.com",
			wantTaskHub:  "hub",
			wantAuth:     AuthenticationAzureCLI,
		},
		{
			name:        "additionally allowed tenants drops empty entries",
			value:       "Endpoint=scheduler.example.com;TaskHub=hub;Authentication=AzureCLI;AdditionallyAllowedTenants=one,,  ,two,",
			wantAuth:    AuthenticationAzureCLI,
			wantTenants: []string{"one", "two"},
		},
		{
			name:     "empty additionally allowed tenants",
			value:    "Endpoint=scheduler.example.com;TaskHub=hub;Authentication=AzureCLI;AdditionallyAllowedTenants=",
			wantAuth: AuthenticationAzureCLI,
		},
		{
			name:    "unsupported Visual Studio",
			value:   "Endpoint=scheduler.example.com;TaskHub=hub;Authentication=VisualStudio",
			wantErr: "no Azure Identity for Go equivalent",
		},
		{
			name:    "unsupported Visual Studio Code",
			value:   "Endpoint=scheduler.example.com;TaskHub=hub;Authentication=VisualStudioCode",
			wantErr: "no Azure Identity for Go equivalent",
		},
		{
			name:    "TokenCredential is programmatic only",
			value:   "Endpoint=scheduler.example.com;TaskHub=hub;Authentication=TokenCredential",
			wantErr: "NewOptionsWithCredential",
		},
		{
			name:    "missing authentication",
			value:   "Endpoint=https://scheduler.example.com;TaskHub=hub",
			wantErr: "missing required Authentication",
		},
		{
			name:    "blank authentication",
			value:   "Endpoint=https://scheduler.example.com;TaskHub=hub;Authentication=   ",
			wantErr: "missing required Authentication",
		},
		{
			name:    "missing endpoint",
			value:   "TaskHub=hub;Authentication=None",
			wantErr: "missing required Endpoint",
		},
		{
			name:    "blank endpoint",
			value:   "Endpoint= ;TaskHub=hub;Authentication=None",
			wantErr: "missing required Endpoint",
		},
		{
			name:    "missing task hub",
			value:   "Endpoint=https://scheduler.example.com;Authentication=None",
			wantErr: "missing required TaskHub",
		},
		{
			name:    "blank task hub",
			value:   "Endpoint=https://scheduler.example.com;TaskHub= ;Authentication=None",
			wantErr: "missing required TaskHub",
		},
		{
			name:    "empty connection string",
			value:   "",
			wantErr: "missing required Endpoint",
		},
		{
			name:    "segment without a separator",
			value:   "Endpoint=https://scheduler.example.com;TaskHub;Authentication=None",
			wantErr: `invalid connection string segment "TaskHub"`,
		},
		{
			name:    "segment with an empty key",
			value:   "Endpoint=https://scheduler.example.com;=orphan;TaskHub=hub;Authentication=None",
			wantErr: `unsupported connection string key ""`,
		},
		{
			name:    "unknown key",
			value:   "Endpoint=https://scheduler.example.com;TaskHub=hub;Authentication=None;Typo=value",
			wantErr: `unsupported connection string key "Typo"`,
		},
		{
			name:    "unsupported authentication",
			value:   "Endpoint=https://scheduler.example.com;TaskHub=hub;Authentication=Password",
			wantErr: `unsupported Authentication value "Password"`,
		},
		{
			name:    "endpoint with a query string",
			value:   "Endpoint=https://scheduler.example.com?tenant=a;TaskHub=hub;Authentication=None",
			wantErr: "query parameters",
		},
		{
			name:    "endpoint with a path",
			value:   "Endpoint=https://scheduler.example.com/hub;TaskHub=hub;Authentication=None",
			wantErr: "cannot include a path",
		},
		{
			name:    "identity fields are rejected for None",
			value:   "Endpoint=http://127.0.0.1:8080;TaskHub=default;Authentication=None;ClientID=client",
			wantErr: "Authentication None does not use ClientID",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			options, err := NewOptionsFromConnectionString(tt.value)
			if tt.wantErr != "" {
				require.Nil(t, options)
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			if tt.wantEndpoint != "" {
				require.Equal(t, tt.wantEndpoint, options.EndpointAddress)
			}
			if tt.wantTaskHub != "" {
				require.Equal(t, tt.wantTaskHub, options.TaskHubName)
			}
			require.Equal(t, tt.wantAuth, options.Authentication)
			require.Equal(t, tt.wantUnsafe, options.AllowInsecureConnection)
			require.Nil(t, options.Credential)
			require.Equal(t, DefaultResourceID, options.ResourceID)
			require.Equal(t, task.DefaultMaximumTimerInterval, options.MaximumTimerInterval)
			require.Equal(t, tt.wantClientID, options.ClientID)
			require.Equal(t, tt.wantTenantID, options.TenantID)
			require.Equal(t, tt.wantTokenFile, options.TokenFilePath)
			require.Equal(t, tt.wantTenants, options.AdditionallyAllowedTenants)
			require.Equal(t, 30*time.Second, options.HelloTimeout)
			require.Equal(t, DefaultMaxReceiveMessageSize, options.MaxReceiveMessageSize)
			require.Equal(t, DefaultMaxSendMessageSize, options.MaxSendMessageSize)
			require.Equal(t, DefaultKeepaliveTime, options.KeepaliveTime)
			require.Equal(t, DefaultKeepaliveTimeout, options.KeepaliveTimeout)
			require.Equal(t, 5, options.ChannelRecreateFailureThreshold)
			require.Equal(t, 30*time.Second, options.ChannelRecreateMinInterval)
			require.Empty(t, options.WorkerID)
			require.Empty(t, options.UserAgent)
			require.NoError(t, options.Validate())
		})
	}
}

// TestNewOptionsFromConnectionStringKeysAreCaseInsensitive covers every
// supported key in lower, upper, and mixed case.
func TestNewOptionsFromConnectionStringKeysAreCaseInsensitive(t *testing.T) {
	tests := []struct {
		name  string
		value string
	}{
		{
			name:  "canonical",
			value: "Endpoint=scheduler.example.com;TaskHub=hub;Authentication=WorkloadIdentity;ClientID=client;TenantID=tenant;TokenFilePath=/token;AdditionallyAllowedTenants=one",
		},
		{
			name:  "lower",
			value: "endpoint=scheduler.example.com;taskhub=hub;authentication=WorkloadIdentity;clientid=client;tenantid=tenant;tokenfilepath=/token;additionallyallowedtenants=one",
		},
		{
			name:  "upper",
			value: "ENDPOINT=scheduler.example.com;TASKHUB=hub;AUTHENTICATION=WorkloadIdentity;CLIENTID=client;TENANTID=tenant;TOKENFILEPATH=/token;ADDITIONALLYALLOWEDTENANTS=one",
		},
		{
			name:  "mixed",
			value: "EnDpOiNt=scheduler.example.com;tAsKhUb=hub;AuThEnTiCaTiOn=WorkloadIdentity;cLiEnTiD=client;TeNaNtId=tenant;ToKeNfIlEpAtH=/token;AdDiTiOnAlLyAlLoWeDtEnAnTs=one",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			options, err := NewOptionsFromConnectionString(tt.value)
			require.NoError(t, err)
			require.Equal(t, "scheduler.example.com", options.EndpointAddress)
			require.Equal(t, "hub", options.TaskHubName)
			require.Equal(t, AuthenticationWorkloadIdentity, options.Authentication)
			require.Equal(t, "client", options.ClientID)
			require.Equal(t, "tenant", options.TenantID)
			require.Equal(t, "/token", options.TokenFilePath)
			require.Equal(t, []string{"one"}, options.AdditionallyAllowedTenants)
		})
	}
}

// TestNewOptionsFromConnectionStringAuthenticationValuesAreCaseInsensitive
// covers every mode a connection string can name.
func TestNewOptionsFromConnectionStringAuthenticationValuesAreCaseInsensitive(t *testing.T) {
	tests := []struct {
		values []string
		want   AuthenticationType
	}{
		{values: []string{"None", "none", "NONE", "nOnE"}, want: AuthenticationNone},
		{
			values: []string{"DefaultAzure", "defaultazure", "DEFAULTAZURE", "dEfAuLtAzUrE"},
			want:   AuthenticationDefaultAzure,
		},
		{
			values: []string{"ManagedIdentity", "managedidentity", "MANAGEDIDENTITY"},
			want:   AuthenticationManagedIdentity,
		},
		{
			values: []string{"WorkloadIdentity", "workloadidentity", "WORKLOADIDENTITY"},
			want:   AuthenticationWorkloadIdentity,
		},
		{values: []string{"Environment", "environment", "ENVIRONMENT"}, want: AuthenticationEnvironment},
		{values: []string{"AzureCLI", "azurecli", "AZURECLI", "AzureCli"}, want: AuthenticationAzureCLI},
		{
			values: []string{"AzurePowerShell", "azurepowershell", "AZUREPOWERSHELL"},
			want:   AuthenticationAzurePowerShell,
		},
		{
			values: []string{"InteractiveBrowser", "interactivebrowser", "INTERACTIVEBROWSER"},
			want:   AuthenticationInteractiveBrowser,
		},
	}

	for _, tt := range tests {
		t.Run(string(tt.want), func(t *testing.T) {
			for _, value := range tt.values {
				endpoint := "scheduler.example.com"
				if tt.want == AuthenticationNone {
					endpoint = "http://127.0.0.1:8080"
				}
				options, err := NewOptionsFromConnectionString(
					"Endpoint=" + endpoint + ";TaskHub=hub;Authentication=" + value,
				)
				require.NoErrorf(t, err, "authentication value %q", value)
				require.Equalf(t, tt.want, options.Authentication, "authentication value %q", value)
			}
		})
	}
}

// TestNewOptionsFromConnectionStringDuplicateKeysUseLastValue matches the .NET
// connection-string builder, where a repeated key overwrites earlier values.
func TestNewOptionsFromConnectionStringDuplicateKeysUseLastValue(t *testing.T) {
	options, err := NewOptionsFromConnectionString(
		"Endpoint=first.example.com;TaskHub=first;Authentication=DefaultAzure;ClientID=first;" +
			"TenantID=first;TokenFilePath=/first;AdditionallyAllowedTenants=first;" +
			"endpoint=second.example.com;TASKHUB=second;authentication=WorkloadIdentity;" +
			"clientid=second;TenantId=second;tokenfilepath=/second;AdditionallyAllowedTenants=second",
	)
	require.NoError(t, err)
	require.Equal(t, "second.example.com", options.EndpointAddress)
	require.Equal(t, "second", options.TaskHubName)
	require.Equal(t, AuthenticationWorkloadIdentity, options.Authentication)
	require.Equal(t, "second", options.ClientID)
	require.Equal(t, "second", options.TenantID)
	require.Equal(t, "/second", options.TokenFilePath)
	require.Equal(t, []string{"second"}, options.AdditionallyAllowedTenants)
}

// TestNewOptionsFromConnectionStringDuplicateKeyCanBlankAValue confirms the last
// value wins even when it clears an earlier one.
func TestNewOptionsFromConnectionStringDuplicateKeyCanBlankAValue(t *testing.T) {
	options, err := NewOptionsFromConnectionString(
		"Endpoint=scheduler.example.com;TaskHub=hub;Authentication=ManagedIdentity;ClientID=client;ClientID=",
	)
	require.NoError(t, err)
	require.Empty(t, options.ClientID)

	_, err = NewOptionsFromConnectionString(
		"Endpoint=scheduler.example.com;TaskHub=hub;Authentication=AzureCLI;Authentication=",
	)
	require.ErrorContains(t, err, "missing required Authentication")
}

// TestNewOptionsFromConnectionStringValuesMayContainSeparators documents that
// only the first '=' separates a key from its value.
func TestNewOptionsFromConnectionStringValuesMayContainSeparators(t *testing.T) {
	options, err := NewOptionsFromConnectionString(
		"Endpoint=scheduler.example.com;TaskHub=hub;Authentication=WorkloadIdentity;TokenFilePath=/run/secrets/token=v2",
	)
	require.NoError(t, err)
	require.Equal(t, "/run/secrets/token=v2", options.TokenFilePath)
}

// TestNewOptionsFromConnectionStringDoesNotForceInsecureForHTTPS keeps the
// insecure opt-in tied to Authentication=None only.
func TestNewOptionsFromConnectionStringDoesNotForceInsecureForHTTPS(t *testing.T) {
	options, err := NewOptionsFromConnectionString(
		"Endpoint=https://scheduler.example.com;TaskHub=hub;Authentication=DefaultAzure",
	)
	require.NoError(t, err)
	require.False(t, options.AllowInsecureConnection)
}

func TestNewOptionsDefaults(t *testing.T) {
	options := NewOptions("scheduler.example.com", "hub")
	require.Equal(t, "scheduler.example.com", options.EndpointAddress)
	require.Equal(t, "hub", options.TaskHubName)
	require.Equal(t, AuthenticationDefaultAzure, options.Authentication)
	require.Equal(t, DefaultResourceID, options.ResourceID)
	require.Equal(t, 30*time.Second, options.HelloTimeout)
	require.Equal(t, DefaultMaxReceiveMessageSize, options.MaxReceiveMessageSize)
	require.Equal(t, DefaultMaxSendMessageSize, options.MaxSendMessageSize)
	require.Equal(t, DefaultKeepaliveTime, options.KeepaliveTime)
	require.Equal(t, DefaultKeepaliveTimeout, options.KeepaliveTimeout)
	require.Equal(t, task.DefaultMaximumTimerInterval, options.MaximumTimerInterval)
	require.Equal(t, 5, options.ChannelRecreateFailureThreshold)
	require.Equal(t, 30*time.Second, options.ChannelRecreateMinInterval)
	require.False(t, options.AllowInsecureConnection)
	require.Nil(t, options.Credential)
	require.NoError(t, options.Validate())
}

func TestOptionsValidatePlaintextGuard(t *testing.T) {
	options := NewOptions("http://127.0.0.1:8080", "default")
	options.AllowInsecureConnection = true
	require.ErrorContains(t, options.Validate(), "cannot be used with credentials")

	options.Authentication = AuthenticationNone
	options.AllowInsecureConnection = false
	require.ErrorContains(t, options.Validate(), "AllowInsecureConnection")

	options.AllowInsecureConnection = true
	require.NoError(t, options.Validate())
}

func TestOptionsValidateEndpoint(t *testing.T) {
	tests := []struct {
		endpoint string
		wantErr  string
	}{
		{endpoint: "scheduler.example.com"},
		{endpoint: "https://scheduler.example.com:443"},
		{endpoint: "scheduler.example.com:443"},
		{endpoint: "  scheduler.example.com  "},
		{endpoint: "https://scheduler.example.com/"},
		{endpoint: "", wantErr: "endpoint is required"},
		{endpoint: "   ", wantErr: "endpoint is required"},
		{endpoint: "ftp://scheduler.example.com", wantErr: "scheme"},
		{endpoint: "grpc://scheduler.example.com", wantErr: "scheme"},
		{endpoint: "https://", wantErr: "must include a host"},
		{endpoint: "https://scheduler.example.com/path", wantErr: "path"},
		{endpoint: "https://user@scheduler.example.com", wantErr: "user info"},
		{endpoint: "https://user:pass@scheduler.example.com", wantErr: "user info"},
		{endpoint: "https://scheduler.example.com?a=b", wantErr: "query parameters"},
		{endpoint: "https://scheduler.example.com#frag", wantErr: "fragment"},
		{endpoint: "https://scheduler.example.com:notaport", wantErr: "invalid DTS endpoint"},
	}

	for _, tt := range tests {
		t.Run(tt.endpoint, func(t *testing.T) {
			options := NewOptions(tt.endpoint, "hub")
			err := options.Validate()
			if tt.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tt.wantErr)
			}
		})
	}
}

// TestOptionsValidateTaskHubWorkerIDAndUserAgent covers the header-injection and
// required-field guards for values placed on outgoing metadata.
func TestOptionsValidateTaskHubWorkerIDAndUserAgent(t *testing.T) {
	tests := []struct {
		name      string
		taskHub   string
		workerID  string
		userAgent string
		wantErr   string
	}{
		{name: "defaults", taskHub: "hub"},
		{name: "explicit worker ID and user agent", taskHub: "hub", workerID: "worker", userAgent: "agent/1.0"},
		{name: "missing task hub", wantErr: "task hub name is required"},
		{name: "blank task hub", taskHub: "   ", wantErr: "task hub name is required"},
		{name: "padded task hub", taskHub: " hub", wantErr: "leading or trailing whitespace"},
		{name: "task hub newline", taskHub: "hub\nx", wantErr: "cannot contain newlines"},
		{name: "task hub carriage return", taskHub: "hub\rx", wantErr: "cannot contain newlines"},
		{name: "padded worker ID", taskHub: "hub", workerID: "worker ", wantErr: "worker ID"},
		{name: "worker ID newline", taskHub: "hub", workerID: "worker\nid", wantErr: "worker ID"},
		{name: "padded user agent", taskHub: "hub", userAgent: " agent", wantErr: "user agent"},
		{name: "user agent injection", taskHub: "hub", userAgent: "agent\r\nx: y", wantErr: "user agent"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			options := NewOptions("scheduler.example.com", tt.taskHub)
			options.WorkerID = tt.workerID
			options.UserAgent = tt.userAgent
			err := options.Validate()
			if tt.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tt.wantErr)
			}
		})
	}
}

func TestOptionsValidateNilReceiver(t *testing.T) {
	var options *Options
	require.ErrorContains(t, options.Validate(), "options are required")

	_, err := prepareOptions(nil)
	require.ErrorContains(t, err, "options are required")
}

func TestPrepareOptionsAppliesDefaultsAndCopiesTenants(t *testing.T) {
	options := &Options{
		EndpointAddress:            "scheduler.example.com",
		TaskHubName:                "hub",
		Authentication:             AuthenticationDefaultAzure,
		AdditionallyAllowedTenants: []string{"tenant"},
	}
	prepared, err := prepareOptions(options)
	require.NoError(t, err)
	require.Equal(t, DefaultResourceID, prepared.ResourceID)
	require.Equal(t, 30*time.Second, prepared.HelloTimeout)
	require.Equal(t, DefaultMaxReceiveMessageSize, prepared.MaxReceiveMessageSize)
	require.Equal(t, DefaultMaxSendMessageSize, prepared.MaxSendMessageSize)
	require.Equal(t, task.DefaultMaximumTimerInterval, prepared.MaximumTimerInterval)
	options.AdditionallyAllowedTenants[0] = "changed"
	require.Equal(t, []string{"tenant"}, prepared.AdditionallyAllowedTenants)
}

// TestPrepareOptionsResourceIDNormalization pins the documented contract:
// exactly empty defaults, surrounding whitespace on a real value is trimmed,
// and a whitespace-only value is rejected instead of collapsing to the default.
func TestPrepareOptionsResourceIDNormalization(t *testing.T) {
	for _, test := range []struct {
		name       string
		resourceID string
		want       string
		wantScope  string
		wantErr    string
	}{
		{
			name:      "empty defaults",
			want:      DefaultResourceID,
			wantScope: DefaultResourceID + "/.default",
		},
		{
			name:       "surrounding whitespace is trimmed",
			resourceID: "  https://custom.example.com  ",
			want:       "https://custom.example.com",
			wantScope:  "https://custom.example.com/.default",
		},
		{
			name:       "exact value is preserved",
			resourceID: "https://custom.example.com",
			want:       "https://custom.example.com",
			wantScope:  "https://custom.example.com/.default",
		},
		{name: "spaces only", resourceID: "   ", wantErr: "resource ID cannot be blank"},
		{name: "tab only", resourceID: "\t", wantErr: "resource ID cannot be blank"},
		{name: "newline only", resourceID: "\n", wantErr: "resource ID cannot contain newlines"},
	} {
		t.Run(test.name, func(t *testing.T) {
			options := NewOptions("scheduler.example.com", "hub")
			options.ResourceID = test.resourceID
			options.Authentication = AuthenticationNone
			options.EndpointAddress = "http://127.0.0.1:8080"
			options.AllowInsecureConnection = true

			prepared, err := prepareOptions(options)
			if test.wantErr != "" {
				require.ErrorContains(t, err, test.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.want, prepared.ResourceID)
			require.Equal(t, test.wantScope, newPerRPCCredentials(&prepared, clientRole, "").scope)
		})
	}
}

func TestOptionsValidateRejectsInvalidUserAgent(t *testing.T) {
	options := NewOptions("scheduler.example.com", "hub")
	options.UserAgent = "agent\r\ninjected"
	require.ErrorContains(t, options.Validate(), "user agent")
}

func TestOptionsValidateRejectsNegativeMaximumTimerInterval(t *testing.T) {
	options := NewOptions("scheduler.example.com", "hub")
	options.MaximumTimerInterval = -time.Second
	require.ErrorContains(t, options.Validate(), "maximum timer interval")
}

func TestOptionsValidateRejectsNegativeChannelRecreateInterval(t *testing.T) {
	options := NewOptions("scheduler.example.com", "hub")
	options.ChannelRecreateMinInterval = -time.Second
	require.ErrorContains(t, options.Validate(), "channel recreate minimum interval")
}

func TestOptionsValidateRejectsInvalidTransportLimits(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*Options)
		message string
	}{
		{
			name:    "negative receive size",
			mutate:  func(options *Options) { options.MaxReceiveMessageSize = -1 },
			message: "maximum receive message size",
		},
		{
			name:    "receive size below minimum",
			mutate:  func(options *Options) { options.MaxReceiveMessageSize = minimumGRPCMessageSize - 1 },
			message: "maximum receive message size",
		},
		{
			name:    "negative send size",
			mutate:  func(options *Options) { options.MaxSendMessageSize = -1 },
			message: "maximum send message size",
		},
		{
			name:    "send size below minimum",
			mutate:  func(options *Options) { options.MaxSendMessageSize = minimumGRPCMessageSize - 1 },
			message: "maximum send message size",
		},
		{
			name:    "negative keepalive time",
			mutate:  func(options *Options) { options.KeepaliveTime = -time.Second },
			message: "keepalive time",
		},
		{
			name:    "aggressive keepalive time",
			mutate:  func(options *Options) { options.KeepaliveTime = minimumKeepaliveTime - time.Second },
			message: "keepalive time",
		},
		{
			name:    "negative keepalive timeout",
			mutate:  func(options *Options) { options.KeepaliveTimeout = -time.Second },
			message: "keepalive timeout",
		},
		{
			name: "keepalive timeout not below interval",
			mutate: func(options *Options) {
				options.KeepaliveTimeout = options.KeepaliveTime
			},
			message: "must be less than keepalive time",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			options := NewOptions("scheduler.example.com", "hub")
			test.mutate(options)
			require.ErrorContains(t, options.Validate(), test.message)
		})
	}
}

func TestOptionsValidateRejectsNilInterceptors(t *testing.T) {
	options := NewOptions("scheduler.example.com", "hub")
	options.UnaryInterceptors = []grpc.UnaryClientInterceptor{nil}
	require.ErrorContains(t, options.Validate(), "unary interceptors")

	options.UnaryInterceptors = nil
	options.StreamInterceptors = []grpc.StreamClientInterceptor{nil}
	require.ErrorContains(t, options.Validate(), "stream interceptors")
}
