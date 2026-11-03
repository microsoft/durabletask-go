package payload

import (
	"bytes"
	"compress/gzip"
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/microsoft/durabletask-go/api"
	"github.com/stretchr/testify/require"
)

func TestAzureBlobStoreHTTPDownloadCompression(t *testing.T) {
	const maxPayloadBytes = 64
	compressed, err := gzipPayload([]byte("payload"))
	require.NoError(t, err)
	var emptyMembers bytes.Buffer
	for emptyMembers.Len() <= 2*maxPayloadBytes {
		writer := gzip.NewWriter(&emptyMembers)
		require.NoError(t, writer.Close())
	}
	oversizedCompressed := append(emptyMembers.Bytes(), compressed...)
	oversizedDecoded, err := gzipPayload(bytes.Repeat([]byte("x"), maxPayloadBytes+1))
	require.NoError(t, err)
	require.Greater(t, len(oversizedCompressed), 2*maxPayloadBytes)
	require.LessOrEqual(t, len(oversizedDecoded), 2*maxPayloadBytes)

	for _, test := range []struct {
		name     string
		body     []byte
		encoding string
		wantErr  error
	}{
		{name: "gzip", body: compressed, encoding: "gzip"},
		{name: "plain", body: []byte("payload")},
		{name: "compressed limit", body: oversizedCompressed, encoding: "gzip", wantErr: api.ErrLargePayloadTooLarge},
		{name: "decoded limit", body: oversizedDecoded, encoding: "gzip", wantErr: api.ErrLargePayloadTooLarge},
		{name: "invalid gzip", body: []byte("not gzip"), encoding: "gzip", wantErr: api.ErrLargePayloadIntegrity},
	} {
		t.Run(test.name, func(t *testing.T) {
			acceptEncoding := make(chan string, 2)
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				acceptEncoding <- r.Header.Get("Accept-Encoding")
				w.Header().Set("Content-Length", strconv.Itoa(len(test.body)))
				if test.encoding != "" {
					w.Header().Set("Content-Encoding", test.encoding)
				}
				_, _ = w.Write(test.body)
			}))
			defer server.Close()
			options := blobAccessOptions(server.URL+"/primary", "identity")
			options.Credential = &blobAccessRecorder{}
			options.AdditionalAccountURLs = []string{server.URL + "/additional"}
			options.MaxPayloadBytes = maxPayloadBytes
			clientOptions := newAzureBlobClientOptions()
			clientOptions.Retry.MaxRetries = -1
			httpClient := clientOptions.Transport.(*http.Client)
			transport := httpClient.Transport.(*http.Transport).Clone()
			transport.TLSClientConfig = server.Client().Transport.(*http.Transport).TLSClientConfig.Clone()
			httpClient.Transport = transport
			defer transport.CloseIdleConnections()
			store, err := newAzureBlobStore(options, clientOptions)
			require.NoError(t, err)
			for _, location := range []string{
				server.URL + "/primary/mycontainer/blob",
				server.URL + "/additional/othercontainer/blob",
			} {
				result, err := store.ResolveToken(context.Background(), azureBlobTokenV2+location)
				if test.wantErr != nil {
					require.ErrorIs(t, err, test.wantErr)
					require.Nil(t, result)
				} else {
					require.NoError(t, err)
					require.Equal(t, "payload", string(result))
				}
				require.Empty(t, <-acceptEncoding, "the HTTP transport must leave decompression to the payload store")
			}
		})
	}
}
