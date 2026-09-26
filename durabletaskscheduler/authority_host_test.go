package durabletaskscheduler

import (
	"context"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/cloud"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/stretchr/testify/require"
)

func TestAuthorityHostConfigurationPerAuthenticationMode(t *testing.T) {
	for _, authentication := range allAuthenticationTypes {
		t.Run(string(authentication), func(t *testing.T) {
			options := NewOptions("scheduler.example.com", "hub")
			options.Authentication = authentication
			options.AuthorityHost = "  " + cloud.AzureGovernment.ActiveDirectoryAuthorityHost + "  "
			if authentication == AuthenticationTokenCredential {
				options.Credential = stubCredential{}
			}
			switch authentication {
			case AuthenticationNone, AuthenticationTokenCredential, AuthenticationManagedIdentity,
				AuthenticationAzureCLI, AuthenticationAzurePowerShell:
				require.ErrorContains(t, options.Validate(), "does not use AuthorityHost")
				return
			}
			require.NoError(t, options.Validate())
			spec, err := newCredentialSpec(options)
			require.NoError(t, err)
			require.Equal(t, cloud.AzureGovernment.ActiveDirectoryAuthorityHost, spec.authorityHost)
			prepared, err := prepareOptionsWith(options, func(observed credentialSpec) (azcore.TokenCredential, error) {
				require.Equal(t, spec, observed)
				return stubCredential{}, nil
			})
			require.NoError(t, err)
			require.Equal(t, options.EndpointAddress, prepared.EndpointAddress)
			require.Equal(t, options.ResourceID, prepared.ResourceID)
			require.Equal(t, options.AuthorityHost, prepared.AuthorityHost)
		})
	}
}

func TestAuthorityHostConnectionString(t *testing.T) {
	for _, key := range []string{"AuthorityHost", "authorityhost", "AUTHORITYHOST", "AuThOrItYhOsT"} {
		options, err := NewOptionsFromConnectionString(
			"Endpoint=scheduler.example.com;TaskHub=hub;Authentication=DefaultAzure;ResourceId=api://Custom;" +
				"AuthorityHost=https://previous.example;" + key + "= https://login.microsoftonline.us/ ",
		)
		require.NoError(t, err)
		require.Equal(t, "https://login.microsoftonline.us/", options.AuthorityHost)
		require.Equal(t, "api://Custom", options.ResourceID)
		require.Equal(t, "scheduler.example.com", options.EndpointAddress)
	}
	for _, authority := range []string{" ", "http://login.example", "login.example", "https://", "https://user@login.example", "https://login.example/tenant", "https://login.example?query=1", "https://login.example#fragment"} {
		options := NewOptions("scheduler.example.com", "hub")
		options.AuthorityHost = authority
		require.ErrorContains(t, options.Validate(), "AuthorityHost must be an HTTPS authority URL")
	}
}

// A deliberately invalid environment authority makes dropped options observable
// in the real Azure Identity constructors without network I/O or reflection.
func TestAzureIdentityAuthorityForwardingAndEnvironmentDefaults(t *testing.T) {
	for _, authentication := range []AuthenticationType{
		AuthenticationDefaultAzure, AuthenticationWorkloadIdentity,
		AuthenticationEnvironment, AuthenticationInteractiveBrowser,
	} {
		t.Run(string(authentication), func(t *testing.T) {
			clearAzureIdentityEnvironment(t)
			t.Setenv("AZURE_TENANT_ID", "00000000-0000-0000-0000-000000000002")
			t.Setenv("AZURE_CLIENT_ID", "00000000-0000-0000-0000-000000000001")
			t.Setenv("AZURE_CLIENT_SECRET", "not-a-real-secret")
			t.Setenv("AZURE_FEDERATED_TOKEN_FILE", "unused-token-file")
			t.Setenv("AZURE_TOKEN_CREDENTIALS", "EnvironmentCredential")
			t.Setenv("AZURE_AUTHORITY_HOST", "http://invalid-environment-authority.example")
			t.Setenv("REGION_NAME", "usgovvirginia")
			options := NewOptions("scheduler.example.com", "hub")
			options.Authentication = authentication

			prepared, err := prepareOptions(options)
			canceled, cancel := context.WithCancel(context.Background())
			cancel()
			if authentication == AuthenticationDefaultAzure {
				// DefaultAzure defers inner constructor errors until GetToken.
				require.NoError(t, err)
				_, err = prepared.Credential.GetToken(canceled, policy.TokenRequestOptions{Scopes: []string{GovernmentResourceID + "/.default"}})
			}
			require.ErrorContains(t, err, "cannot use an authority host without https",
				"omitted AuthorityHost must preserve AZURE_AUTHORITY_HOST")

			options.AuthorityHost = cloud.AzureGovernment.ActiveDirectoryAuthorityHost
			prepared, err = prepareOptions(options)
			require.NoError(t, err, "explicit AuthorityHost must override the invalid environment authority")
			require.NotNil(t, prepared.Credential)
			require.Equal(t, GovernmentResourceID, prepared.ResourceID)
			require.Equal(t, options.EndpointAddress, prepared.EndpointAddress)
			if authentication == AuthenticationDefaultAzure {
				_, err = prepared.Credential.GetToken(canceled, policy.TokenRequestOptions{Scopes: []string{GovernmentResourceID + "/.default"}})
				require.Error(t, err)
				require.NotContains(t, err.Error(), "cannot use an authority host without https",
					"DefaultAzure must forward the explicit authority to its inner credential")
			}

			t.Setenv("AZURE_AUTHORITY_HOST", "")
			options.AuthorityHost = ""
			prepared, err = prepareOptions(options)
			require.NoError(t, err, "omission must preserve Azure Identity's public default")
			require.NotNil(t, prepared.Credential)
			require.Empty(t, prepared.AuthorityHost, "government audience must not choose an authority")
		})
	}
}
