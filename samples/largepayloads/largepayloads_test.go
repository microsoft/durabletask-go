package main

import "testing"

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
