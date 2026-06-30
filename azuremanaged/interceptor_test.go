package azuremanaged

import (
	"context"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"google.golang.org/grpc/metadata"
)

func metadataValue(md metadata.MD, key string) string {
	if vals := md.Get(key); len(vals) > 0 {
		return vals[0]
	}
	return ""
}

func TestAppendMetadataClient(t *testing.T) {
	i := &authInterceptor{taskHub: "myhub", userAgent: "durabletask-go/test"}

	ctx, err := i.appendMetadata(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		t.Fatal("expected outgoing metadata to be set")
	}
	if got := metadataValue(md, "taskhub"); got != "myhub" {
		t.Errorf("taskhub = %q, want %q", got, "myhub")
	}
	if got := metadataValue(md, "x-user-agent"); got != "durabletask-go/test" {
		t.Errorf("x-user-agent = %q", got)
	}
	if got := metadataValue(md, "workerid"); got != "" {
		t.Errorf("workerid = %q, want empty for a client", got)
	}
	if got := metadataValue(md, "authorization"); got != "" {
		t.Errorf("authorization = %q, want empty when no credential", got)
	}
}

func TestAppendMetadataWorkerWithToken(t *testing.T) {
	cred := &fakeCredential{token: azcore.AccessToken{Token: "tok", ExpiresOn: time.Now().Add(time.Hour)}}
	i := &authInterceptor{
		taskHub:      "myhub",
		userAgent:    "durabletask-go/test",
		workerID:     "host:1:abc",
		tokenManager: newAccessTokenManager(cred, []string{"scope"}),
	}

	ctx, err := i.appendMetadata(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	md, _ := metadata.FromOutgoingContext(ctx)
	if got := metadataValue(md, "workerid"); got != "host:1:abc" {
		t.Errorf("workerid = %q", got)
	}
	if got := metadataValue(md, "authorization"); got != "Bearer tok" {
		t.Errorf("authorization = %q, want %q", got, "Bearer tok")
	}
}
