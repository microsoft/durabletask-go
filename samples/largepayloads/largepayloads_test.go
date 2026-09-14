package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/protos"
	"github.com/microsoft/durabletask-go/samples/internal/dtssample"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestStorageCleanupWaitsForAcceptedUploads(t *testing.T) {
	for _, shutdownTimesOut := range []bool{false, true} {
		name := "drained before deletion"
		if shutdownTimesOut {
			name = "shutdown timeout retains container"
		}
		t.Run(name, func(t *testing.T) {
			uploadStarted := make(chan struct{})
			uploadFinished := make(chan struct{})
			releaseUpload := make(chan struct{})
			release := sync.OnceFunc(func() { close(releaseUpload) })
			var uploads, waits atomic.Int32
			var purged, completed, deleted atomic.Bool
			storage := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.Method {
				case http.MethodPut:
					_, _ = io.Copy(io.Discard, r.Body)
					if r.URL.Query().Get("restype") != "container" && uploads.Add(1) == 2 {
						// The worker's output upload is accepted but not yet acknowledged.
						close(uploadStarted)
						<-releaseUpload
						close(uploadFinished)
					}
					w.WriteHeader(http.StatusCreated)
				case http.MethodDelete:
					if !purged.Load() || !completed.Load() {
						t.Error("storage deleted before owned orchestration cleanup and worker drain")
					}
					deleted.Store(true)
					w.WriteHeader(http.StatusAccepted)
				default:
					t.Errorf("unexpected storage request: %s %s", r.Method, r.URL)
					w.WriteHeader(http.StatusBadRequest)
				}
			}))
			t.Cleanup(storage.Close)
			t.Cleanup(release)
			t.Setenv("AZURE_STORAGE_CONNECTION_STRING",
				"AccountName=account;AccountKey=ZmFrZS1hY2NvdW50LWtleQ==;BlobEndpoint="+storage.URL+"/account")
			t.Setenv("DTS_SAMPLE_ALLOW_INSECURE_STORAGE", "1")

			content := strings.Repeat("accepted-payload", 32)
			digest := sha256.Sum256([]byte(content))
			input, err := json.Marshal(sampleInput{Content: content, SHA256: hex.EncodeToString(digest[:])})
			require.NoError(t, err)
			items := make(chan *protos.WorkItem, 1)
			intakeStopped := make(chan struct{})
			server := grpc.NewServer(
				grpc.UnaryInterceptor(func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
					switch request := request.(type) {
					case *emptypb.Empty:
						return &emptypb.Empty{}, nil
					case *protos.CreateInstanceRequest:
						items <- &protos.WorkItem{
							CompletionToken: "upload",
							Request: &protos.WorkItem_ActivityRequest{ActivityRequest: &protos.ActivityRequest{
								Name:                  "ValidateLargePayload",
								Input:                 wrapperspb.String(string(input)),
								OrchestrationInstance: &protos.OrchestrationInstance{InstanceId: request.InstanceId},
							}},
						}
						return &protos.CreateInstanceResponse{InstanceId: request.InstanceId}, nil
					case *protos.GetInstanceRequest:
						runtimeStatus := api.RUNTIME_STATUS_RUNNING
						if info.FullMethod == protos.TaskHubSidecarService_WaitForInstanceCompletion_FullMethodName {
							if waits.Add(1) == 1 {
								select {
								case <-uploadStarted:
									return nil, status.Error(codes.InvalidArgument, "injected scenario failure")
								case <-ctx.Done():
									return nil, status.FromContextError(ctx.Err()).Err()
								}
							}
							runtimeStatus = api.RUNTIME_STATUS_TERMINATED
						}
						return &protos.GetInstanceResponse{
							Exists: true,
							OrchestrationState: &protos.OrchestrationState{
								InstanceId: request.InstanceId, OrchestrationStatus: runtimeStatus,
							},
						}, nil
					case *protos.TerminateRequest:
						return &protos.TerminateResponse{}, nil
					case *protos.PurgeInstancesRequest:
						purged.Store(true)
						return &protos.PurgeInstancesResponse{DeletedInstanceCount: 1}, nil
					case *protos.ActivityResponse:
						completed.Store(true)
						return &protos.CompleteTaskResponse{}, nil
					case *protos.AbandonActivityTaskRequest:
						return &protos.AbandonActivityTaskResponse{}, nil
					default:
						return handler(ctx, request)
					}
				}),
				grpc.StreamInterceptor(func(_ any, stream grpc.ServerStream, _ *grpc.StreamServerInfo, _ grpc.StreamHandler) error {
					select {
					case item := <-items:
						if err := stream.SendMsg(item); err != nil {
							return err
						}
					case <-stream.Context().Done():
						return nil
					}
					<-stream.Context().Done()
					close(intakeStopped)
					return nil
				}),
			)
			protos.RegisterTaskHubSidecarServiceServer(server, &protos.UnimplementedTaskHubSidecarServiceServer{})
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			t.Cleanup(server.Stop)
			go func() { _ = server.Serve(listener) }()
			t.Setenv(dtssample.ConnectionStringVariable, "Endpoint=http://"+listener.Addr().String()+";TaskHub=test;Authentication=None")

			done := make(chan error, 1)
			go func() {
				done <- run()
				close(done)
			}()
			t.Cleanup(func() {
				release()
				select {
				case <-done:
				case <-time.After(35 * time.Second):
					t.Error("sample did not finish after releasing its upload")
				}
			})
			select {
			case <-intakeStopped:
			case <-time.After(5 * time.Second):
				t.Fatal("sample did not reach worker shutdown with an accepted upload")
			}
			require.True(t, purged.Load())
			require.False(t, deleted.Load(), "intake cancellation alone does not drain accepted uploads")
			if !shutdownTimesOut {
				release()
			}
			select {
			case err := <-done:
				require.ErrorContains(t, err, "injected scenario failure")
				if shutdownTimesOut {
					require.ErrorIs(t, err, context.DeadlineExceeded)
					require.ErrorContains(t, err, "storage container dtgolarge")
					require.ErrorContains(t, err, "retained because worker shutdown did not confirm drain")
					require.False(t, deleted.Load())
				} else {
					require.NotErrorIs(t, err, context.DeadlineExceeded)
					require.True(t, completed.Load())
					require.True(t, deleted.Load())
				}
			case <-time.After(35 * time.Second):
				t.Fatal("sample did not bound worker shutdown")
			}
			release()
			select {
			case <-uploadFinished:
			case <-time.After(time.Second):
				t.Fatal("released upload did not finish")
			}
		})
	}
}

func TestStoredCompressionIsIndependentOfDownloadDecompression(t *testing.T) {
	const hash = "expected-payload-hash"
	content := []byte(strings.Repeat(hash, 10))
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	if _, err := writer.Write(content); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name                 string
		storedGzip           bool
		wantGzip             bool
		disableDecompression bool
	}{
		{name: "automatically decompressed gzip", storedGzip: true, wantGzip: true},
		{name: "raw gzip", storedGzip: true, wantGzip: true, disableDecompression: true},
		{name: "uncompressed"},
		{name: "reject unexpected plain storage", wantGzip: true},
		{name: "reject unexpected compressed storage", storedGzip: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body := content
				if test.storedGzip {
					body = compressed.Bytes()
					w.Header().Set("Content-Encoding", "gzip")
				}
				w.Header().Set("Content-Length", strconv.Itoa(len(body)))
				w.WriteHeader(http.StatusOK)
				if r.Method != http.MethodHead {
					if _, err := w.Write(body); err != nil {
						t.Error(err)
					}
				}
			}))
			defer server.Close()
			transport := http.DefaultTransport.(*http.Transport).Clone()
			transport.DisableCompression = test.disableDecompression
			defer transport.CloseIdleConnections()
			client, err := azblob.NewClientWithNoCredential(server.URL, &azblob.ClientOptions{
				ClientOptions: azcore.ClientOptions{Transport: &http.Client{Transport: transport}},
			})
			if err != nil {
				t.Fatal(err)
			}
			err = verifyStoredPayloads(t.Context(), client, "container",
				map[string]struct{}{"blob": {}}, hash, test.wantGzip)
			if test.storedGzip == test.wantGzip {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), "stored gzip=") {
				t.Fatalf("expected stored-compression mismatch, got %v", err)
			}
		})
	}
}

func TestAllowInsecureStorageRequiresLoopbackOptIn(t *testing.T) {
	t.Setenv("DTS_SAMPLE_ALLOW_INSECURE_STORAGE", "")
	if _, err := allowInsecureStorage("BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;AccountName=a;AccountKey=b"); err == nil {
		t.Fatal("expected plaintext storage without opt-in to fail")
	}

	t.Setenv("DTS_SAMPLE_ALLOW_INSECURE_STORAGE", "1")
	if _, err := allowInsecureStorage("BlobEndpoint=http://example.com:10000/account;AccountName=a;AccountKey=b"); err == nil {
		t.Fatal("expected non-loopback plaintext storage to fail")
	}
	if allow, err := allowInsecureStorage("BlobEndpoint=http://127.0.0.1:10000/account;AccountName=a;AccountKey=b"); err != nil || !allow {
		t.Fatalf("loopback plaintext storage allow=%v err=%v, want true nil", allow, err)
	}
}

func TestReadStorageSettingsAlwaysCreatesOwnedContainer(t *testing.T) {
	t.Setenv("AZURE_STORAGE_CONNECTION_STRING", "DefaultEndpointsProtocol=https;AccountName=acct;AccountKey=key;EndpointSuffix=core.windows.net")
	settings, err := readStorageSettings()
	if err != nil {
		t.Fatal(err)
	}
	if settings.container == "" {
		t.Fatalf("container=%q, want generated owned container", settings.container)
	}
}
