package main

import (
	"bytes"
	"compress/gzip"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
)

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
