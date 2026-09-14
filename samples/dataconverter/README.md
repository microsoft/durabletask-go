# Custom data converter

This sample configures a small `api.DataConverter` on both the DTS management
client and worker. The converter writes `sample-dc:` plus base64-encoded JSON so
the custom encoding is visible in metadata and history without adding another
serialization dependency.

## Run

```bash
export DTS_CONNECTION_STRING='Endpoint=http://127.0.0.1:8080;TaskHub=default;Authentication=None'
go run ./samples/dataconverter
```

## What it proves

- Typed orchestration input/output, activity input/output, external event data,
  custom status, and entity state all round-trip through the configured
  converter.
- A raw-input workflow intentionally bypasses converter serialization and
  verifies that the raw metadata is unchanged.
- The converter can still read legacy/plain JSON payloads for compatibility.

`SAMPLE_OK dataconverter` is printed only after decoded values, raw encodings,
and entity cleanup are verified. The sample deletes only its exact entity key
and purges only its declared orchestration IDs.
