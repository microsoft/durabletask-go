// Command largepayloads verifies Azure Blob backed large-payload
// externalization and hydration against a live Durable Task Scheduler hub.
package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/microsoft/durabletask-go/api"
	durabletaskclient "github.com/microsoft/durabletask-go/client"
	"github.com/microsoft/durabletask-go/payload"
	"github.com/microsoft/durabletask-go/samples/internal/dtssample"
	"github.com/microsoft/durabletask-go/task"
)

const (
	payloadThresholdBytes = 64
	payloadMaxBytes       = 4 * 1024 * 1024
)

type sampleInput struct {
	Content string `json:"content"`
	SHA256  string `json:"sha256"`
}

type sampleOutput struct {
	Content string `json:"content"`
	SHA256  string `json:"sha256"`
	Length  int    `json:"length"`
}

type storageSettings struct {
	connectionString  string
	container         string
	allowInsecureHTTP bool
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("SAMPLE_OK largepayloads")
}

func run() (err error) {
	settings, err := readStorageSettings()
	if err != nil {
		return err
	}
	blobClient, err := azblob.NewClientFromConnectionString(settings.connectionString, nil)
	if err != nil {
		return fmt.Errorf("create Azure Blob reader: %w", err)
	}
	compressionEnabled := compressionSetting()
	store, err := payload.NewAzureBlobStore(payload.AzureBlobStoreOptions{
		ConnectionString:   settings.connectionString,
		Container:          settings.container,
		AllowInsecureHTTP:  settings.allowInsecureHTTP,
		CompressionEnabled: &compressionEnabled,
		MaxPayloadBytes:    payloadMaxBytes,
	})
	if err != nil {
		return fmt.Errorf("create Azure Blob payload store: %w", err)
	}
	cleanupStorage := func() error {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, cleanupErr := blobClient.DeleteContainer(cleanupCtx, settings.container, nil)
		if bloberror.HasCode(cleanupErr, bloberror.ContainerNotFound) {
			return nil
		}
		return cleanupErr
	}

	options, err := dtssample.Options()
	if err != nil {
		return err
	}
	options.LargePayloads = &api.LargePayloadOptions{
		Store:           store,
		Resolver:        store,
		ThresholdBytes:  payloadThresholdBytes,
		MaxPayloadBytes: payloadMaxBytes,
	}
	registry := task.NewTaskRegistry()
	if err := registry.AddOrchestratorN("LargePayloadsRoundTrip", largePayloadsRoundTrip); err != nil {
		return err
	}
	if err := registry.AddActivityN("ValidateLargePayload", validateLargePayload); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	app, err := dtssample.StartWithOptions(ctx, options, registry, durabletaskclient.WithAutoWorkItemFilters())
	if err != nil {
		err = errors.Join(err, cleanupStorage())
		return err
	}
	var ownedIDs []api.InstanceID
	defer func() {
		err = errors.Join(err, dtssample.Cleanup(app.Client, ownedIDs...), cleanupStorage(), app.Shutdown())
	}()

	content := strings.Repeat("large-payloads-sample-", 512)
	hash := sha256.Sum256([]byte(content))
	input := sampleInput{Content: content, SHA256: hex.EncodeToString(hash[:])}
	instanceID := dtssample.NewInstanceID("largepayloads")
	ownedIDs = append(ownedIDs, instanceID)
	if _, err := app.Client.ScheduleNewOrchestration(ctx, "LargePayloadsRoundTrip",
		api.WithInstanceID(instanceID),
		api.WithInput(input),
		api.WithTags(map[string]string{"sample": "largepayloads"})); err != nil {
		return err
	}
	metadata, err := app.Client.WaitForOrchestrationCompletion(ctx, instanceID, api.WithFetchPayloads(true))
	if err != nil {
		return err
	}
	if err := dtssample.RequireCompleted(metadata); err != nil {
		return err
	}
	var output sampleOutput
	if err := metadata.ReadOutput(&output); err != nil {
		return err
	}
	if output.Content != content || output.SHA256 != input.SHA256 || output.Length != len(content) {
		return fmt.Errorf("roundtrip mismatch: length=%d hash=%s", output.Length, output.SHA256)
	}

	created, err := listBlobNames(ctx, blobClient, settings.container)
	if err != nil {
		return err
	}
	if len(created) < 3 {
		return fmt.Errorf("expected at least 3 externalized payload blobs, found %d (%v)", len(created), created)
	}
	if err := verifyStoredPayloads(ctx, blobClient, settings.container, created, input.SHA256, compressionEnabled); err != nil {
		return err
	}
	fmt.Printf("verified %d Azure Blob payload object(s) in container %s\n", len(created), settings.container)
	return nil
}

func largePayloadsRoundTrip(ctx *task.OrchestrationContext) (any, error) {
	var input sampleInput
	if err := ctx.GetInput(&input); err != nil {
		return nil, err
	}
	var output sampleOutput
	if err := ctx.CallActivity("ValidateLargePayload", task.WithActivityInput(input)).Await(&output); err != nil {
		return nil, err
	}
	return output, nil
}

func validateLargePayload(ctx task.ActivityContext) (any, error) {
	var input sampleInput
	if err := ctx.GetInput(&input); err != nil {
		return nil, err
	}
	hash := sha256.Sum256([]byte(input.Content))
	if got := hex.EncodeToString(hash[:]); got != input.SHA256 {
		return nil, fmt.Errorf("activity received hash %s, want %s", got, input.SHA256)
	}
	return sampleOutput{Content: input.Content, SHA256: input.SHA256, Length: len(input.Content)}, nil
}

func readStorageSettings() (storageSettings, error) {
	connectionString := os.Getenv("AZURE_STORAGE_CONNECTION_STRING")
	if connectionString == "" {
		connectionString = os.Getenv("EXPORT_STORAGE_CONNECTION_STRING")
	}
	if connectionString == "" {
		return storageSettings{}, errors.New("AZURE_STORAGE_CONNECTION_STRING is required; EXPORT_STORAGE_CONNECTION_STRING is accepted for compatibility")
	}
	allowHTTP, err := allowInsecureStorage(connectionString)
	if err != nil {
		return storageSettings{}, err
	}
	container, err := randomContainerName("dtgolarge")
	if err != nil {
		return storageSettings{}, err
	}
	return storageSettings{
		connectionString:  connectionString,
		container:         container,
		allowInsecureHTTP: allowHTTP,
	}, nil
}

func compressionSetting() bool {
	value := strings.ToLower(strings.TrimSpace(os.Getenv("LARGEPAYLOADS_DISABLE_GZIP")))
	return value != "1" && value != "true"
}

func allowInsecureStorage(connectionString string) (bool, error) {
	if !strings.Contains(strings.ToLower(connectionString), "http://") &&
		!strings.Contains(strings.ToLower(connectionString), "usedevelopmentstorage=true") {
		return false, nil
	}
	if os.Getenv("DTS_SAMPLE_ALLOW_INSECURE_STORAGE") != "1" {
		return false, errors.New("plain HTTP storage endpoints require DTS_SAMPLE_ALLOW_INSECURE_STORAGE=1 and loopback Azurite endpoints")
	}
	if strings.Contains(strings.ToLower(connectionString), "usedevelopmentstorage=true") {
		return true, nil
	}
	endpoints := connectionStringEndpoints(connectionString)
	if len(endpoints) == 0 {
		return false, errors.New("plaintext storage connection strings must include explicit loopback service endpoints")
	}
	for _, endpoint := range endpoints {
		if strings.HasPrefix(strings.ToLower(endpoint), "http://") && !isLoopbackEndpoint(endpoint) {
			return false, fmt.Errorf("refusing non-loopback plaintext storage endpoint %q", endpoint)
		}
	}
	return true, nil
}

func connectionStringEndpoints(connectionString string) []string {
	var endpoints []string
	for _, part := range strings.Split(connectionString, ";") {
		key, value, ok := strings.Cut(part, "=")
		if ok && strings.HasSuffix(strings.ToLower(strings.TrimSpace(key)), "endpoint") {
			endpoints = append(endpoints, strings.TrimSpace(value))
		}
	}
	return endpoints
}

func isLoopbackEndpoint(endpoint string) bool {
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return false
	}
	host := parsed.Hostname()
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func randomContainerName(prefix string) (string, error) {
	var suffix [8]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		return "", fmt.Errorf("generate container name: %w", err)
	}
	return prefix + hex.EncodeToString(suffix[:]), nil
}

func listBlobNames(ctx context.Context, client *azblob.Client, container string) (map[string]struct{}, error) {
	names := map[string]struct{}{}
	pager := client.NewListBlobsFlatPager(container, nil)
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if bloberror.HasCode(err, bloberror.ContainerNotFound) {
			return names, nil
		}
		if err != nil {
			return nil, fmt.Errorf("list blobs in %s: %w", container, err)
		}
		for _, item := range page.Segment.BlobItems {
			if item.Name != nil {
				names[*item.Name] = struct{}{}
			}
		}
	}
	return names, nil
}

func verifyStoredPayloads(
	ctx context.Context,
	client *azblob.Client,
	container string,
	names map[string]struct{},
	wantHash string,
	wantGzip bool,
) error {
	sawHash := false
	for name := range names {
		response, err := client.DownloadStream(ctx, container, name, nil)
		if err != nil {
			return fmt.Errorf("download payload blob %s: %w", name, err)
		}
		body, readErr := io.ReadAll(response.Body)
		closeErr := response.Body.Close()
		if readErr != nil || closeErr != nil {
			return errors.Join(readErr, closeErr)
		}
		isGzip := response.ContentEncoding != nil && strings.EqualFold(*response.ContentEncoding, "gzip")
		if isGzip != wantGzip {
			return fmt.Errorf("payload blob %s gzip=%t, want %t", name, isGzip, wantGzip)
		}
		if isGzip {
			reader, err := gzip.NewReader(bytes.NewReader(body))
			if err != nil {
				return fmt.Errorf("decode gzip payload blob %s: %w", name, err)
			}
			body, readErr = io.ReadAll(reader)
			closeErr = reader.Close()
			if readErr != nil || closeErr != nil {
				return errors.Join(readErr, closeErr)
			}
		}
		if len(body) <= payloadThresholdBytes {
			return fmt.Errorf("payload blob %s contains only %d bytes, threshold is %d", name, len(body), payloadThresholdBytes)
		}
		if strings.Contains(string(body), wantHash) {
			sawHash = true
		}
	}
	if !sawHash {
		return fmt.Errorf("downloaded payload blobs did not contain expected hash %s", wantHash)
	}
	return nil
}
