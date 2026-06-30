// Package azuremanaged provides client and worker constructors for connecting the
// Durable Task Framework for Go to an Azure-managed Durable Task Scheduler (DTS) task hub.
//
// It wraps the core client.TaskHubGrpcClient with the gRPC metadata and authentication
// that DTS expects: the task hub name, a user-agent header, and a bearer token obtained
// from an azcore.TokenCredential. Connections can be configured explicitly via Options or
// parsed from a DTS connection string of the form:
//
//	Endpoint=<address>;Authentication=<type>;TaskHub=<name>
package azuremanaged

import (
	"fmt"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
)

// connectionString holds the parsed key/value pairs of a DTS connection string. Keys are
// stored lower-cased so that lookups are case-insensitive, matching the other SDKs.
type connectionString struct {
	values map[string]string
}

// parseConnectionString splits a "key=value;key=value" connection string into its parts.
func parseConnectionString(cs string) (*connectionString, error) {
	values := make(map[string]string)
	for _, segment := range strings.Split(cs, ";") {
		segment = strings.TrimSpace(segment)
		if segment == "" {
			continue
		}
		key, value, found := strings.Cut(segment, "=")
		if !found {
			return nil, fmt.Errorf("invalid connection string segment %q: expected key=value", segment)
		}
		values[strings.ToLower(strings.TrimSpace(key))] = strings.TrimSpace(value)
	}
	return &connectionString{values: values}, nil
}

// get returns the value for a key (case-insensitive), or the empty string if absent.
func (c *connectionString) get(name string) string {
	return c.values[strings.ToLower(name)]
}

// getRequired returns the value for a key, or an error if it is missing or empty.
func (c *connectionString) getRequired(name string) (string, error) {
	if v := c.get(name); v != "" {
		return v, nil
	}
	return "", fmt.Errorf("the connection string is missing the required '%s' property", name)
}

// credentialFromConnectionString resolves an azcore.TokenCredential from the connection
// string's Authentication property. A nil credential with a nil error means "None" — no
// bearer token is attached, which is only valid for local/emulator scenarios.
func credentialFromConnectionString(cs *connectionString) (azcore.TokenCredential, error) {
	auth, err := cs.getRequired("Authentication")
	if err != nil {
		return nil, err
	}
	clientID := cs.get("ClientID")

	switch strings.ToLower(strings.ReplaceAll(auth, " ", "")) {
	case "defaultazure":
		return azidentity.NewDefaultAzureCredential(nil)
	case "managedidentity":
		opts := &azidentity.ManagedIdentityCredentialOptions{}
		if clientID != "" {
			opts.ID = azidentity.ClientID(clientID)
		}
		return azidentity.NewManagedIdentityCredential(opts)
	case "workloadidentity":
		opts := &azidentity.WorkloadIdentityCredentialOptions{}
		if clientID != "" {
			opts.ClientID = clientID
		}
		if tenantID := cs.get("TenantId"); tenantID != "" {
			opts.TenantID = tenantID
		}
		if tokenFilePath := cs.get("TokenFilePath"); tokenFilePath != "" {
			opts.TokenFilePath = tokenFilePath
		}
		if tenants := cs.get("AdditionallyAllowedTenants"); tenants != "" {
			opts.AdditionallyAllowedTenants = strings.Split(tenants, ",")
		}
		return azidentity.NewWorkloadIdentityCredential(opts)
	case "environment":
		return azidentity.NewEnvironmentCredential(nil)
	case "azurecli":
		return azidentity.NewAzureCLICredential(nil)
	case "azuredeveloper", "azuredevelopercli":
		return azidentity.NewAzureDeveloperCLICredential(nil)
	case "none":
		return nil, nil
	default:
		return nil, fmt.Errorf("the connection string contains an unsupported authentication type %q", auth)
	}
}
