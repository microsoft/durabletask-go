package durabletaskscheduler

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/stretchr/testify/require"
)

// stubCredential is a placeholder azcore.TokenCredential. It never contacts
// Azure and is only used to assert credential plumbing.
type stubCredential struct{ name string }

func (stubCredential) GetToken(
	context.Context,
	policy.TokenRequestOptions,
) (azcore.AccessToken, error) {
	return azcore.AccessToken{Token: "stub", ExpiresOn: time.Now().Add(time.Hour)}, nil
}

// recordingFactory wraps a credential factory so the specs it receives can be
// asserted. It is passed explicitly instead of replacing process-global state,
// so concurrent tests never observe each other's factory.
type recordingFactory struct {
	observed []credentialSpec
	factory  credentialFactory
}

func newRecordingFactory(factory credentialFactory) *recordingFactory {
	return &recordingFactory{factory: factory}
}

func (r *recordingFactory) build(spec credentialSpec) (azcore.TokenCredential, error) {
	r.observed = append(r.observed, spec)
	return r.factory(spec)
}

// allAuthenticationTypes is the complete supported set. Tests iterate it so a
// newly added mode fails the exhaustiveness assertions until it is covered.
var allAuthenticationTypes = []AuthenticationType{
	AuthenticationNone,
	AuthenticationDefaultAzure,
	AuthenticationManagedIdentity,
	AuthenticationWorkloadIdentity,
	AuthenticationEnvironment,
	AuthenticationAzureCLI,
	AuthenticationAzurePowerShell,
	AuthenticationInteractiveBrowser,
	AuthenticationTokenCredential,
}

// TestCredentialSpecForEveryAuthenticationMode pins the exact Azure Identity
// inputs each mode consumes when every identity field is populated.
func TestCredentialSpecForEveryAuthenticationMode(t *testing.T) {
	tests := []struct {
		authentication AuthenticationType
		want           credentialSpec
	}{
		{
			authentication: AuthenticationNone,
			want:           credentialSpec{authentication: AuthenticationNone},
		},
		{
			authentication: AuthenticationDefaultAzure,
			want: credentialSpec{
				authentication:             AuthenticationDefaultAzure,
				tenantID:                   "tenant",
				additionallyAllowedTenants: []string{"one", "two"},
			},
		},
		{
			authentication: AuthenticationManagedIdentity,
			want: credentialSpec{
				authentication: AuthenticationManagedIdentity,
				clientID:       "client",
			},
		},
		{
			authentication: AuthenticationWorkloadIdentity,
			want: credentialSpec{
				authentication:             AuthenticationWorkloadIdentity,
				clientID:                   "client",
				tenantID:                   "tenant",
				tokenFilePath:              "/token",
				additionallyAllowedTenants: []string{"one", "two"},
			},
		},
		{
			authentication: AuthenticationEnvironment,
			want:           credentialSpec{authentication: AuthenticationEnvironment},
		},
		{
			authentication: AuthenticationAzureCLI,
			want: credentialSpec{
				authentication:             AuthenticationAzureCLI,
				tenantID:                   "tenant",
				additionallyAllowedTenants: []string{"one", "two"},
			},
		},
		{
			authentication: AuthenticationAzurePowerShell,
			want: credentialSpec{
				authentication:             AuthenticationAzurePowerShell,
				tenantID:                   "tenant",
				additionallyAllowedTenants: []string{"one", "two"},
			},
		},
		{
			authentication: AuthenticationInteractiveBrowser,
			want: credentialSpec{
				authentication:             AuthenticationInteractiveBrowser,
				clientID:                   "client",
				tenantID:                   "tenant",
				additionallyAllowedTenants: []string{"one", "two"},
			},
		},
		{
			authentication: AuthenticationTokenCredential,
			want:           credentialSpec{authentication: AuthenticationTokenCredential},
		},
	}

	covered := make(map[AuthenticationType]bool, len(tests))
	for _, tt := range tests {
		t.Run(string(tt.authentication), func(t *testing.T) {
			covered[tt.authentication] = true
			// Untrimmed values prove the spec normalizes before construction.
			options := &Options{
				Authentication:             tt.authentication,
				ClientID:                   "  client  ",
				TenantID:                   "\ttenant\t",
				TokenFilePath:              " /token ",
				AdditionallyAllowedTenants: []string{" one ", "", "  ", "two"},
			}
			spec, err := newCredentialSpec(options)
			require.NoError(t, err)
			require.Equal(t, tt.want, spec)
		})
	}
	for _, authentication := range allAuthenticationTypes {
		require.Truef(t, covered[authentication], "authentication %q is not covered", authentication)
	}
}

func TestCredentialSpecOmitsUnsetFields(t *testing.T) {
	for _, authentication := range allAuthenticationTypes {
		t.Run(string(authentication), func(t *testing.T) {
			spec, err := newCredentialSpec(&Options{Authentication: authentication})
			require.NoError(t, err)
			require.Equal(t, credentialSpec{authentication: authentication}, spec)
			require.Nil(t, spec.additionallyAllowedTenants)
		})
	}
}

func TestCredentialSpecRejectsUnknownAuthentication(t *testing.T) {
	for _, value := range []AuthenticationType{"", "VisualStudio", "defaultazure"} {
		t.Run(string(value), func(t *testing.T) {
			_, err := newCredentialSpec(&Options{Authentication: value})
			require.ErrorContains(t, err, "unsupported DTS authentication type")
		})
	}
}

func TestNormalizeAdditionallyAllowedTenants(t *testing.T) {
	require.Nil(t, normalizeAdditionallyAllowedTenants(nil))
	require.Nil(t, normalizeAdditionallyAllowedTenants([]string{"", "   ", "\t"}))
	require.Equal(
		t,
		[]string{"one", "two", "*"},
		normalizeAdditionallyAllowedTenants([]string{" one ", "", "two", " * "}),
	)
}

// TestResolveCredentialDispatchesEveryAzureIdentityMode asserts each mode that
// builds an Azure Identity credential reaches the factory with its own spec.
func TestResolveCredentialDispatchesEveryAzureIdentityMode(t *testing.T) {
	azureIdentityModes := []AuthenticationType{
		AuthenticationDefaultAzure,
		AuthenticationManagedIdentity,
		AuthenticationWorkloadIdentity,
		AuthenticationEnvironment,
		AuthenticationAzureCLI,
		AuthenticationAzurePowerShell,
		AuthenticationInteractiveBrowser,
	}
	for _, authentication := range azureIdentityModes {
		t.Run(string(authentication), func(t *testing.T) {
			built := stubCredential{name: string(authentication)}
			factory := newRecordingFactory(
				func(credentialSpec) (azcore.TokenCredential, error) { return built, nil },
			)
			options := &Options{
				Authentication:             authentication,
				ClientID:                   "client",
				TenantID:                   "tenant",
				TokenFilePath:              "/token",
				AdditionallyAllowedTenants: []string{"one"},
			}
			credential, err := resolveCredential(options, factory.build)
			require.NoError(t, err)
			require.Equal(t, built, credential)

			want, err := newCredentialSpec(options)
			require.NoError(t, err)
			require.Equal(t, []credentialSpec{want}, factory.observed)
		})
	}
}

func TestResolveCredentialNoneReturnsNilWithoutFactory(t *testing.T) {
	factory := newRecordingFactory(func(credentialSpec) (azcore.TokenCredential, error) {
		t.Fatal("Authentication None must not construct a credential")
		return nil, nil
	})
	credential, err := resolveCredential(&Options{Authentication: AuthenticationNone}, factory.build)
	require.NoError(t, err)
	require.Nil(t, credential)
	require.Empty(t, factory.observed)
}

func TestResolveCredentialTokenCredentialReturnsConfiguredCredential(t *testing.T) {
	factory := newRecordingFactory(func(credentialSpec) (azcore.TokenCredential, error) {
		t.Fatal("Authentication TokenCredential must not construct a credential")
		return nil, nil
	})
	configured := stubCredential{name: "explicit"}
	credential, err := resolveCredential(&Options{
		Authentication: AuthenticationTokenCredential,
		Credential:     configured,
	}, factory.build)
	require.NoError(t, err)
	require.Equal(t, configured, credential)
	require.Empty(t, factory.observed)
}

func TestResolveCredentialPropagatesFactoryError(t *testing.T) {
	failure := errors.New("credential construction failed")
	factory := newRecordingFactory(func(credentialSpec) (azcore.TokenCredential, error) {
		return nil, failure
	})
	_, err := resolveCredential(&Options{Authentication: AuthenticationAzureCLI}, factory.build)
	require.ErrorIs(t, err, failure)
}

func TestResolveCredentialRejectsUnsupportedAuthentication(t *testing.T) {
	_, err := resolveCredential(&Options{Authentication: "Password"}, newAzureIdentityCredential)
	require.ErrorContains(t, err, `unsupported DTS authentication type "Password"`)
}

// TestNewAzureIdentityCredentialRejectsNonIdentityModes covers the production
// factory branches that cannot construct a credential. The remaining branches
// call Azure Identity constructors and are exercised through the seam instead.
func TestNewAzureIdentityCredentialRejectsNonIdentityModes(t *testing.T) {
	for _, authentication := range []AuthenticationType{
		AuthenticationNone,
		AuthenticationTokenCredential,
		"Password",
	} {
		t.Run(string(authentication), func(t *testing.T) {
			_, err := newAzureIdentityCredential(credentialSpec{authentication: authentication})
			require.ErrorContains(t, err, "unsupported DTS authentication type")
		})
	}
}

// TestPrepareOptionsNormalizesResolvedCredential asserts every Azure Identity
// mode collapses to TokenCredential once its credential has been constructed.
func TestPrepareOptionsNormalizesResolvedCredential(t *testing.T) {
	for _, authentication := range allAuthenticationTypes {
		t.Run(string(authentication), func(t *testing.T) {
			built := stubCredential{name: string(authentication)}
			options := NewOptions("scheduler.example.com", "hub")
			options.Authentication = authentication
			switch authentication {
			case AuthenticationNone:
				options.EndpointAddress = "http://127.0.0.1:8080"
				options.AllowInsecureConnection = true
			case AuthenticationTokenCredential:
				options.Credential = built
			}

			prepared, err := prepareOptionsWith(
				options,
				func(credentialSpec) (azcore.TokenCredential, error) { return built, nil },
			)
			require.NoError(t, err)
			if authentication == AuthenticationNone {
				require.Equal(t, AuthenticationNone, prepared.Authentication)
				require.Nil(t, prepared.Credential)
				return
			}
			require.Equal(t, AuthenticationTokenCredential, prepared.Authentication)
			require.Equal(t, built, prepared.Credential)
		})
	}
}

func TestPrepareOptionsPropagatesCredentialFailure(t *testing.T) {
	failure := errors.New("no managed identity endpoint")
	options := NewOptions("scheduler.example.com", "hub")
	options.Authentication = AuthenticationManagedIdentity
	_, err := prepareOptionsWith(options, func(credentialSpec) (azcore.TokenCredential, error) {
		return nil, failure
	})
	require.ErrorIs(t, err, failure)
}

// TestPrepareOptionsValidatesBeforeConstructingCredential keeps invalid
// configuration from reaching Azure Identity.
func TestPrepareOptionsValidatesBeforeConstructingCredential(t *testing.T) {
	options := NewOptions("scheduler.example.com", "")
	options.Authentication = AuthenticationAzureCLI
	_, err := prepareOptionsWith(options, func(credentialSpec) (azcore.TokenCredential, error) {
		t.Fatal("invalid options must not construct a credential")
		return nil, nil
	})
	require.ErrorContains(t, err, "task hub name is required")
}

func TestTokenScope(t *testing.T) {
	tests := []struct {
		resourceID string
		want       string
	}{
		{resourceID: DefaultResourceID, want: "https://durabletask.io/.default"},
		{resourceID: "https://durabletask.io/", want: "https://durabletask.io/.default"},
		{resourceID: "https://durabletask.io///", want: "https://durabletask.io/.default"},
		{resourceID: "  https://durabletask.io  ", want: "https://durabletask.io/.default"},
		{resourceID: "https://custom.example.com", want: "https://custom.example.com/.default"},
	}
	for _, tt := range tests {
		t.Run(tt.resourceID, func(t *testing.T) {
			require.Equal(t, tt.want, tokenScope(tt.resourceID))
		})
	}
}

func TestOptionsValidateRejectsBlankOrInjectedResourceID(t *testing.T) {
	options := NewOptions("scheduler.example.com", "hub")
	options.ResourceID = "   "
	require.ErrorContains(t, options.Validate(), "resource ID cannot be blank")

	options.ResourceID = "https://durabletask.io\r\nx"
	require.ErrorContains(t, options.Validate(), "resource ID cannot contain newlines")
}

// TestOptionsValidateCredentialPresencePerMode covers the credential guard for
// every supported authentication mode plus the unset default.
func TestOptionsValidateCredentialPresencePerMode(t *testing.T) {
	tests := []struct {
		name           string
		authentication AuthenticationType
		credential     azcore.TokenCredential
		wantErr        string
	}{
		{name: "unset defaults to DefaultAzure", authentication: ""},
		{
			name:           "unset rejects credential",
			authentication: "",
			credential:     stubCredential{},
			wantErr:        "use Authentication TokenCredential",
		},
		{name: "DefaultAzure", authentication: AuthenticationDefaultAzure},
		{
			name:           "DefaultAzure with credential",
			authentication: AuthenticationDefaultAzure,
			credential:     stubCredential{},
			wantErr:        "use Authentication TokenCredential",
		},
		{name: "ManagedIdentity", authentication: AuthenticationManagedIdentity},
		{
			name:           "ManagedIdentity with credential",
			authentication: AuthenticationManagedIdentity,
			credential:     stubCredential{},
			wantErr:        "credential must be nil when Authentication is ManagedIdentity",
		},
		{name: "WorkloadIdentity", authentication: AuthenticationWorkloadIdentity},
		{
			name:           "WorkloadIdentity with credential",
			authentication: AuthenticationWorkloadIdentity,
			credential:     stubCredential{},
			wantErr:        "credential must be nil when Authentication is WorkloadIdentity",
		},
		{name: "Environment", authentication: AuthenticationEnvironment},
		{
			name:           "Environment with credential",
			authentication: AuthenticationEnvironment,
			credential:     stubCredential{},
			wantErr:        "credential must be nil when Authentication is Environment",
		},
		{name: "AzureCLI", authentication: AuthenticationAzureCLI},
		{
			name:           "AzureCLI with credential",
			authentication: AuthenticationAzureCLI,
			credential:     stubCredential{},
			wantErr:        "credential must be nil when Authentication is AzureCLI",
		},
		{name: "AzurePowerShell", authentication: AuthenticationAzurePowerShell},
		{
			name:           "AzurePowerShell with credential",
			authentication: AuthenticationAzurePowerShell,
			credential:     stubCredential{},
			wantErr:        "credential must be nil when Authentication is AzurePowerShell",
		},
		{name: "InteractiveBrowser", authentication: AuthenticationInteractiveBrowser},
		{
			name:           "InteractiveBrowser with credential",
			authentication: AuthenticationInteractiveBrowser,
			credential:     stubCredential{},
			wantErr:        "credential must be nil when Authentication is InteractiveBrowser",
		},
		{
			name:           "TokenCredential",
			authentication: AuthenticationTokenCredential,
			credential:     stubCredential{},
		},
		{
			name:           "TokenCredential without credential",
			authentication: AuthenticationTokenCredential,
			wantErr:        "TokenCredential authentication requires a credential",
		},
		{
			name:           "None rejects credential",
			authentication: AuthenticationNone,
			credential:     stubCredential{},
			wantErr:        "credential must be nil when Authentication is None",
		},
		{
			name:           "unknown authentication",
			authentication: "Password",
			wantErr:        `unsupported DTS authentication type "Password"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			options := NewOptions("scheduler.example.com", "hub")
			options.Authentication = tt.authentication
			options.Credential = tt.credential
			err := options.Validate()
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

// TestOptionsValidateIdentityFieldCombinations documents which modes accept
// tenant, client, token-file, and additional-tenant configuration.
func TestOptionsValidateIdentityFieldCombinations(t *testing.T) {
	type identityFields struct {
		clientID      string
		tenantID      string
		tokenFilePath string
		tenants       []string
	}
	all := identityFields{
		clientID:      "client",
		tenantID:      "tenant",
		tokenFilePath: "/token",
		tenants:       []string{"one"},
	}

	tests := []struct {
		name           string
		authentication AuthenticationType
		fields         identityFields
		wantErr        string
	}{
		{name: "DefaultAzure accepts all", authentication: AuthenticationDefaultAzure, fields: all},
		{name: "ManagedIdentity accepts all", authentication: AuthenticationManagedIdentity, fields: all},
		{name: "WorkloadIdentity accepts all", authentication: AuthenticationWorkloadIdentity, fields: all},
		{name: "Environment accepts all", authentication: AuthenticationEnvironment, fields: all},
		{name: "AzureCLI accepts all", authentication: AuthenticationAzureCLI, fields: all},
		{name: "AzurePowerShell accepts all", authentication: AuthenticationAzurePowerShell, fields: all},
		{name: "InteractiveBrowser accepts all", authentication: AuthenticationInteractiveBrowser, fields: all},
		{
			name:           "None rejects ClientID",
			authentication: AuthenticationNone,
			fields:         identityFields{clientID: "client"},
			wantErr:        "Authentication None does not use ClientID",
		},
		{
			name:           "None rejects TenantID",
			authentication: AuthenticationNone,
			fields:         identityFields{tenantID: "tenant"},
			wantErr:        "Authentication None does not use TenantID",
		},
		{
			name:           "None rejects TokenFilePath",
			authentication: AuthenticationNone,
			fields:         identityFields{tokenFilePath: "/token"},
			wantErr:        "Authentication None does not use TokenFilePath",
		},
		{
			name:           "None rejects AdditionallyAllowedTenants",
			authentication: AuthenticationNone,
			fields:         identityFields{tenants: []string{"one"}},
			wantErr:        "Authentication None does not use AdditionallyAllowedTenants",
		},
		{
			name:           "None reports every unusable field",
			authentication: AuthenticationNone,
			fields:         all,
			wantErr:        "ClientID, TenantID, TokenFilePath, AdditionallyAllowedTenants",
		},
		{
			name:           "None ignores blank fields",
			authentication: AuthenticationNone,
			fields:         identityFields{clientID: "  ", tenantID: "\t", tenants: []string{"", " "}},
		},
		{
			name:           "TokenCredential rejects identity fields",
			authentication: AuthenticationTokenCredential,
			fields:         all,
			wantErr:        "Authentication TokenCredential does not use ClientID, TenantID",
		},
		{
			name:           "TokenCredential ignores blank fields",
			authentication: AuthenticationTokenCredential,
			fields:         identityFields{tenants: []string{""}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			options := NewOptions("scheduler.example.com", "hub")
			options.Authentication = tt.authentication
			options.ClientID = tt.fields.clientID
			options.TenantID = tt.fields.tenantID
			options.TokenFilePath = tt.fields.tokenFilePath
			options.AdditionallyAllowedTenants = tt.fields.tenants
			switch tt.authentication {
			case AuthenticationNone:
				options.EndpointAddress = "http://127.0.0.1:8080"
				options.AllowInsecureConnection = true
			case AuthenticationTokenCredential:
				options.Credential = stubCredential{}
			}

			err := options.Validate()
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

// TestOptionsValidatePlaintextAndCredentialGuards covers every scheme,
// insecure-opt-in, and authentication combination.
func TestOptionsValidatePlaintextAndCredentialGuards(t *testing.T) {
	tests := []struct {
		name           string
		endpoint       string
		allowInsecure  bool
		authentication AuthenticationType
		wantErr        string
	}{
		{
			name:           "https with credentials",
			endpoint:       "https://scheduler.example.com",
			authentication: AuthenticationDefaultAzure,
		},
		{
			name:           "implicit https with credentials",
			endpoint:       "scheduler.example.com",
			authentication: AuthenticationDefaultAzure,
		},
		{
			name:           "https with None",
			endpoint:       "https://scheduler.example.com",
			authentication: AuthenticationNone,
		},
		{
			name:           "https allows the insecure opt-in",
			endpoint:       "https://scheduler.example.com",
			allowInsecure:  true,
			authentication: AuthenticationDefaultAzure,
		},
		{
			name:           "http without opt-in",
			endpoint:       "http://127.0.0.1:8080",
			authentication: AuthenticationNone,
			wantErr:        "requires AllowInsecureConnection",
		},
		{
			name:           "http with opt-in and None",
			endpoint:       "http://127.0.0.1:8080",
			allowInsecure:  true,
			authentication: AuthenticationNone,
		},
		{
			name:           "http with opt-in and credentials",
			endpoint:       "http://127.0.0.1:8080",
			allowInsecure:  true,
			authentication: AuthenticationDefaultAzure,
			wantErr:        "cannot be used with credentials",
		},
		{
			name:           "http with opt-in and AzureCLI",
			endpoint:       "http://127.0.0.1:8080",
			allowInsecure:  true,
			authentication: AuthenticationAzureCLI,
			wantErr:        "cannot be used with credentials",
		},
		{
			name:          "http with opt-in and default authentication",
			endpoint:      "http://127.0.0.1:8080",
			allowInsecure: true,
			wantErr:       "cannot be used with credentials",
		},
		{
			name:           "http without opt-in reports the opt-in first",
			endpoint:       "http://127.0.0.1:8080",
			authentication: AuthenticationDefaultAzure,
			wantErr:        "requires AllowInsecureConnection",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			options := NewOptions(tt.endpoint, "hub")
			options.Authentication = tt.authentication
			options.AllowInsecureConnection = tt.allowInsecure
			err := options.Validate()
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

// TestConnectRejectsCredentialsOnPlaintextTransport is the transport-level
// backstop for the Validate guard: gRPC refuses per-RPC credentials that
// require transport security on an insecure channel.
func TestConnectRejectsCredentialsOnPlaintextTransport(t *testing.T) {
	options := NewOptions("http://127.0.0.1:8080", "hub")
	options.Authentication = AuthenticationTokenCredential
	options.Credential = stubCredential{}
	options.AllowInsecureConnection = true

	connection, err := connect(options, clientRole, "")
	require.Error(t, err)
	require.Nil(t, connection)
}

func TestNewOptionsWithCredentialUsesTokenCredentialMode(t *testing.T) {
	credential := stubCredential{name: "explicit"}
	options := NewOptionsWithCredential("scheduler.example.com", "hub", credential)
	require.Equal(t, AuthenticationTokenCredential, options.Authentication)
	require.Equal(t, credential, options.Credential)
	require.Equal(t, DefaultResourceID, options.ResourceID)
	require.Equal(t, 30*time.Second, options.HelloTimeout)
	require.NoError(t, options.Validate())

	nilCredential := NewOptionsWithCredential("scheduler.example.com", "hub", nil)
	require.ErrorContains(t, nilCredential.Validate(), "requires a credential")
}

// TestRetryServiceConfigDefaults pins the client channel retry defaults.
func TestRetryServiceConfigDefaults(t *testing.T) {
	var parsed struct {
		MethodConfig []struct {
			Name        []map[string]any `json:"name"`
			RetryPolicy struct {
				MaxAttempts          int      `json:"maxAttempts"`
				InitialBackoff       string   `json:"initialBackoff"`
				MaxBackoff           string   `json:"maxBackoff"`
				BackoffMultiplier    float64  `json:"backoffMultiplier"`
				RetryableStatusCodes []string `json:"retryableStatusCodes"`
			} `json:"retryPolicy"`
		} `json:"methodConfig"`
	}
	require.NoError(t, json.Unmarshal([]byte(retryServiceConfig), &parsed))
	require.Len(t, parsed.MethodConfig, 1)
	method := parsed.MethodConfig[0]
	require.Equal(t, []map[string]any{{}}, method.Name)
	require.Equal(t, 5, method.RetryPolicy.MaxAttempts)
	require.Equal(t, "0.050s", method.RetryPolicy.InitialBackoff)
	require.Equal(t, "0.250s", method.RetryPolicy.MaxBackoff)
	require.InDelta(t, 2.0, method.RetryPolicy.BackoffMultiplier, 0)
	require.Equal(t, []string{"UNAVAILABLE"}, method.RetryPolicy.RetryableStatusCodes)
}

// azureIdentityEnvironmentVariables are neutralized so credential-construction
// tests do not depend on ambient developer or CI configuration.
var azureIdentityEnvironmentVariables = []string{
	"AZURE_TENANT_ID",
	"AZURE_CLIENT_ID",
	"AZURE_CLIENT_SECRET",
	"AZURE_CLIENT_CERTIFICATE_PATH",
	"AZURE_CLIENT_CERTIFICATE_PASSWORD",
	"AZURE_USERNAME",
	"AZURE_PASSWORD",
	"AZURE_FEDERATED_TOKEN_FILE",
	"AZURE_ADDITIONALLY_ALLOWED_TENANTS",
	"AZURE_AUTHORITY_HOST",
	"AZURE_TOKEN_CREDENTIALS",
}

func clearAzureIdentityEnvironment(t *testing.T) {
	t.Helper()
	for _, name := range azureIdentityEnvironmentVariables {
		t.Setenv(name, "")
		require.NoError(t, os.Unsetenv(name))
	}
}

// TestNewAzureIdentityCredentialConstructsOffline exercises the production
// factory for the modes whose Azure Identity constructors do no network I/O.
// ManagedIdentity and DefaultAzure are covered separately because their
// construction depends on the hosting environment.
func TestNewAzureIdentityCredentialConstructsOffline(t *testing.T) {
	tests := []struct {
		name    string
		spec    credentialSpec
		wantErr string
	}{
		{
			name: "WorkloadIdentity with explicit configuration",
			spec: credentialSpec{
				authentication:             AuthenticationWorkloadIdentity,
				clientID:                   "00000000-0000-0000-0000-000000000001",
				tenantID:                   "00000000-0000-0000-0000-000000000002",
				tokenFilePath:              "/var/run/secrets/token",
				additionallyAllowedTenants: []string{"*"},
			},
		},
		{
			name:    "WorkloadIdentity without configuration",
			spec:    credentialSpec{authentication: AuthenticationWorkloadIdentity},
			wantErr: "WorkloadIdentityCredential",
		},
		{
			name: "AzureCLI",
			spec: credentialSpec{
				authentication:             AuthenticationAzureCLI,
				tenantID:                   "00000000-0000-0000-0000-000000000002",
				additionallyAllowedTenants: []string{"*"},
			},
		},
		{
			name: "AzurePowerShell",
			spec: credentialSpec{
				authentication:             AuthenticationAzurePowerShell,
				tenantID:                   "00000000-0000-0000-0000-000000000002",
				additionallyAllowedTenants: []string{"*"},
			},
		},
		{
			name: "InteractiveBrowser",
			spec: credentialSpec{
				authentication:             AuthenticationInteractiveBrowser,
				clientID:                   "00000000-0000-0000-0000-000000000001",
				tenantID:                   "00000000-0000-0000-0000-000000000002",
				additionallyAllowedTenants: []string{"*"},
			},
		},
		{
			name:    "Environment without environment variables",
			spec:    credentialSpec{authentication: AuthenticationEnvironment},
			wantErr: "EnvironmentCredential",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clearAzureIdentityEnvironment(t)
			credential, err := newAzureIdentityCredential(tt.spec)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				require.Nil(t, credential)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, credential)
		})
	}
}

// TestNewAzureIdentityCredentialEnvironmentModeIgnoresSpecFields confirms
// EnvironmentCredential is configured only by environment variables.
func TestNewAzureIdentityCredentialEnvironmentModeIgnoresSpecFields(t *testing.T) {
	clearAzureIdentityEnvironment(t)
	_, err := newAzureIdentityCredential(credentialSpec{
		authentication: AuthenticationEnvironment,
		clientID:       "00000000-0000-0000-0000-000000000001",
		tenantID:       "00000000-0000-0000-0000-000000000002",
	})
	require.ErrorContains(t, err, "EnvironmentCredential")

	t.Setenv("AZURE_TENANT_ID", "00000000-0000-0000-0000-000000000002")
	t.Setenv("AZURE_CLIENT_ID", "00000000-0000-0000-0000-000000000001")
	t.Setenv("AZURE_CLIENT_SECRET", "not-a-real-secret")
	credential, err := newAzureIdentityCredential(
		credentialSpec{authentication: AuthenticationEnvironment},
	)
	require.NoError(t, err)
	require.NotNil(t, credential)
}

// TestNewAzureIdentityCredentialWorkloadIdentityUsesEnvironmentFallback matches
// Azure Identity behavior where unset fields fall back to the webhook-injected
// environment variables.
func TestNewAzureIdentityCredentialWorkloadIdentityUsesEnvironmentFallback(t *testing.T) {
	clearAzureIdentityEnvironment(t)
	t.Setenv("AZURE_TENANT_ID", "00000000-0000-0000-0000-000000000002")
	t.Setenv("AZURE_CLIENT_ID", "00000000-0000-0000-0000-000000000001")
	t.Setenv("AZURE_FEDERATED_TOKEN_FILE", "/var/run/secrets/token")

	credential, err := newAzureIdentityCredential(
		credentialSpec{authentication: AuthenticationWorkloadIdentity},
	)
	require.NoError(t, err)
	require.NotNil(t, credential)
}

// TestNewAzureIdentityCredentialDefaultAzureAcceptsTenantScoping covers the
// DefaultAzure branch, including the tenant fields that were previously dropped.
func TestNewAzureIdentityCredentialDefaultAzureAcceptsTenantScoping(t *testing.T) {
	clearAzureIdentityEnvironment(t)
	credential, err := newAzureIdentityCredential(credentialSpec{
		authentication:             AuthenticationDefaultAzure,
		tenantID:                   "00000000-0000-0000-0000-000000000002",
		additionallyAllowedTenants: []string{"*"},
	})
	require.NoError(t, err)
	require.NotNil(t, credential)
}
