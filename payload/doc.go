// Package payload provides large-payload stores for Durable Task Scheduler.
//
// AzureBlobStore is the production interoperable implementation for application
// payloads. The separate exporthistory.AzureBlobHistoryStore writes exported
// history objects. MemoryStore is process-local and non-durable, while FileStore
// requires every client and worker to share the same durable filesystem.
package payload
