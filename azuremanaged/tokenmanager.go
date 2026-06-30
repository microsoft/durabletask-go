package azuremanaged

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
)

// defaultRefreshBuffer is how long before expiry a cached token is proactively refreshed.
const defaultRefreshBuffer = 10 * time.Minute

// accessTokenManager caches an access token obtained from an azcore.TokenCredential and
// refreshes it shortly before it expires. It is safe for concurrent use.
type accessTokenManager struct {
	credential    azcore.TokenCredential
	scopes        []string
	refreshBuffer time.Duration

	mu    sync.Mutex
	token azcore.AccessToken
	valid bool
}

func newAccessTokenManager(credential azcore.TokenCredential, scopes []string) *accessTokenManager {
	return &accessTokenManager{
		credential:    credential,
		scopes:        scopes,
		refreshBuffer: defaultRefreshBuffer,
	}
}

// getToken returns a cached token, acquiring a new one when none is cached or the cached
// token is within refreshBuffer of expiry.
func (m *accessTokenManager) getToken(ctx context.Context) (azcore.AccessToken, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.valid && time.Until(m.token.ExpiresOn) > m.refreshBuffer {
		return m.token, nil
	}

	token, err := m.credential.GetToken(ctx, policy.TokenRequestOptions{Scopes: m.scopes})
	if err != nil {
		return azcore.AccessToken{}, fmt.Errorf("failed to acquire access token: %w", err)
	}
	m.token = token
	m.valid = true
	return token, nil
}
