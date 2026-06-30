package azuremanaged

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// authInterceptor injects the DTS task hub name, a user-agent header, an optional worker id,
// and (when a credential is configured) a bearer token into every outgoing gRPC call.
type authInterceptor struct {
	taskHub      string
	userAgent    string
	workerID     string              // empty for clients; set for workers
	tokenManager *accessTokenManager // nil when no credential is configured
}

// appendMetadata returns a context carrying the DTS metadata for an outgoing call.
func (i *authInterceptor) appendMetadata(ctx context.Context) (context.Context, error) {
	pairs := []string{
		"taskhub", i.taskHub,
		"x-user-agent", i.userAgent,
	}
	if i.workerID != "" {
		pairs = append(pairs, "workerid", i.workerID)
	}
	if i.tokenManager != nil {
		token, err := i.tokenManager.getToken(ctx)
		if err != nil {
			return nil, err
		}
		pairs = append(pairs, "authorization", "Bearer "+token.Token)
	}
	return metadata.AppendToOutgoingContext(ctx, pairs...), nil
}

// unaryClientInterceptor returns a grpc.UnaryClientInterceptor that attaches DTS metadata.
func (i *authInterceptor) unaryClientInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		ctx, err := i.appendMetadata(ctx)
		if err != nil {
			return err
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

// streamClientInterceptor returns a grpc.StreamClientInterceptor that attaches DTS metadata.
//
// Metadata is attached when the stream is established. The worker's work-item stream is
// long-lived; the bearer token is therefore validated at connection time and refreshed when
// the underlying client reconnects (the core worker reconnects on stream errors).
func (i *authInterceptor) streamClientInterceptor() grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		ctx, err := i.appendMetadata(ctx)
		if err != nil {
			return nil, err
		}
		return streamer(ctx, desc, cc, method, opts...)
	}
}
