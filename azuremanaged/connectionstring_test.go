package azuremanaged

import (
	"strings"
	"testing"
)

func TestParseConnectionString(t *testing.T) {
	cs, err := parseConnectionString("Endpoint=myhost.durabletask.io:443;Authentication=DefaultAzure;TaskHub=myhub")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := cs.get("endpoint"); got != "myhost.durabletask.io:443" {
		t.Errorf("Endpoint = %q", got)
	}
	// Lookups are case-insensitive.
	if got := cs.get("TASKHUB"); got != "myhub" {
		t.Errorf("TaskHub = %q", got)
	}
	if got := cs.get("Authentication"); got != "DefaultAzure" {
		t.Errorf("Authentication = %q", got)
	}
}

func TestParseConnectionStringTrimsAndSkipsEmptySegments(t *testing.T) {
	cs, err := parseConnectionString("  Endpoint = host ; ; TaskHub = hub ;")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := cs.get("endpoint"); got != "host" {
		t.Errorf("Endpoint = %q, want %q", got, "host")
	}
	if got := cs.get("taskhub"); got != "hub" {
		t.Errorf("TaskHub = %q, want %q", got, "hub")
	}
}

func TestParseConnectionStringInvalidSegment(t *testing.T) {
	if _, err := parseConnectionString("Endpoint=host;NoEquals"); err == nil {
		t.Fatal("expected an error for a segment without '='")
	}
}

func TestGetRequiredMissing(t *testing.T) {
	cs, _ := parseConnectionString("Endpoint=host")
	if _, err := cs.getRequired("TaskHub"); err == nil {
		t.Fatal("expected an error for a missing required property")
	}
}

func TestCredentialFromConnectionString(t *testing.T) {
	tests := []struct {
		name    string
		auth    string
		wantNil bool
		wantErr bool
	}{
		{name: "defaultazure", auth: "DefaultAzure"},
		{name: "case-insensitive and spaces", auth: "  default azure "},
		{name: "managedidentity", auth: "ManagedIdentity"},
		{name: "azurecli", auth: "AzureCLI"},
		{name: "none yields nil credential", auth: "None", wantNil: true},
		{name: "unsupported", auth: "Telepathy", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cs, _ := parseConnectionString("Endpoint=host;TaskHub=hub;Authentication=" + tt.auth)
			cred, err := credentialFromConnectionString(cs)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected an error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.wantNil && cred != nil {
				t.Errorf("expected nil credential, got %T", cred)
			}
			if !tt.wantNil && cred == nil {
				t.Error("expected a non-nil credential")
			}
		})
	}
}

func TestCredentialFromConnectionStringEnvironment(t *testing.T) {
	// EnvironmentCredential validates its required variables at construction time.
	t.Setenv("AZURE_TENANT_ID", "tenant")
	t.Setenv("AZURE_CLIENT_ID", "client")
	t.Setenv("AZURE_CLIENT_SECRET", "secret")

	cs, _ := parseConnectionString("Endpoint=host;TaskHub=hub;Authentication=Environment")
	cred, err := credentialFromConnectionString(cs)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cred == nil {
		t.Error("expected a non-nil credential")
	}
}

func TestCredentialFromConnectionStringMissingAuth(t *testing.T) {
	cs, _ := parseConnectionString("Endpoint=host;TaskHub=hub")
	if _, err := credentialFromConnectionString(cs); err == nil {
		t.Fatal("expected an error when Authentication is missing")
	}
}

func TestOptionsFromConnectionString(t *testing.T) {
	opts, err := optionsFromConnectionString("Endpoint=myhost:443;Authentication=None;TaskHub=hub")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if opts.Endpoint != "myhost:443" {
		t.Errorf("Endpoint = %q", opts.Endpoint)
	}
	if opts.TaskHub != "hub" {
		t.Errorf("TaskHub = %q", opts.TaskHub)
	}
	// Authentication=None implies an insecure channel with no credential.
	if opts.Credential != nil {
		t.Error("expected nil credential for Authentication=None")
	}
	if !opts.Insecure {
		t.Error("expected Insecure to be true for Authentication=None")
	}
}

func TestOptionsFromConnectionStringMissingRequired(t *testing.T) {
	for _, cs := range []string{
		"Authentication=None;TaskHub=hub",   // no Endpoint
		"Endpoint=host;Authentication=None", // no TaskHub
		"Endpoint=host;TaskHub=hub",         // no Authentication
	} {
		if _, err := optionsFromConnectionString(cs); err == nil {
			t.Errorf("expected an error for %q", cs)
		}
	}
}

func TestNormalizeEndpoint(t *testing.T) {
	tests := map[string]string{
		"https://host.durabletask.io":     "host.durabletask.io:443",
		"http://host.durabletask.io/":     "host.durabletask.io:443",
		"host.durabletask.io":             "host.durabletask.io:443",
		"localhost:8080":                  "localhost:8080",
		"https://host.durabletask.io:443": "host.durabletask.io:443",
	}
	for in, want := range tests {
		if got := normalizeEndpoint(in); got != want {
			t.Errorf("normalizeEndpoint(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestUserAgentPrefix(t *testing.T) {
	if ua := userAgent(); !strings.HasPrefix(ua, "durabletask-go/") {
		t.Errorf("userAgent() = %q, want prefix %q", ua, "durabletask-go/")
	}
}
