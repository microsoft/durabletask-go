package azuremanaged

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
)

// fakeCredential is a test azcore.TokenCredential that returns a preset token and counts calls.
type fakeCredential struct {
	token azcore.AccessToken
	err   error
	calls int
}

func (f *fakeCredential) GetToken(_ context.Context, _ policy.TokenRequestOptions) (azcore.AccessToken, error) {
	f.calls++
	if f.err != nil {
		return azcore.AccessToken{}, f.err
	}
	return f.token, nil
}

func TestAccessTokenManagerCachesToken(t *testing.T) {
	cred := &fakeCredential{token: azcore.AccessToken{Token: "abc", ExpiresOn: time.Now().Add(time.Hour)}}
	m := newAccessTokenManager(cred, []string{"https://durabletask.io/.default"})

	tok, err := m.getToken(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tok.Token != "abc" {
		t.Errorf("Token = %q, want %q", tok.Token, "abc")
	}

	// A second call within the validity window should reuse the cached token.
	if _, err := m.getToken(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cred.calls != 1 {
		t.Errorf("credential called %d times, want 1 (cached)", cred.calls)
	}
}

func TestAccessTokenManagerRefreshesNearExpiry(t *testing.T) {
	// A token already inside the refresh buffer should be refreshed on the next call.
	cred := &fakeCredential{token: azcore.AccessToken{Token: "stale", ExpiresOn: time.Now().Add(time.Minute)}}
	m := newAccessTokenManager(cred, []string{"scope"})

	if _, err := m.getToken(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Next token returned by the credential is fresh.
	cred.token = azcore.AccessToken{Token: "fresh", ExpiresOn: time.Now().Add(time.Hour)}
	tok, err := m.getToken(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if tok.Token != "fresh" {
		t.Errorf("Token = %q, want %q", tok.Token, "fresh")
	}
	if cred.calls != 2 {
		t.Errorf("credential called %d times, want 2 (refreshed)", cred.calls)
	}
}

func TestAccessTokenManagerPropagatesError(t *testing.T) {
	cred := &fakeCredential{err: errors.New("boom")}
	m := newAccessTokenManager(cred, []string{"scope"})
	if _, err := m.getToken(context.Background()); err == nil {
		t.Fatal("expected an error from the credential to be propagated")
	}
}
