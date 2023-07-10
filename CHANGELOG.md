# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

This is a breaking change release that introduces the ability to run workflows in 
Go in an out-of-process worker process. It also contains various minor improvements.

### Added

- Added `client` package with `TaskHubGrpcClient` and related functions
- Added otel span events for external events, suspend, and resume operations
- Added termination support to task module
- Added sub-orchestration support to task module
- (Tests) Added test suite starter for Go-based orchestration execution logic

### Changed

- Renamed `WithJsonSerializableEventData` to `WithJsonEventPayload`
- Moved gRPC client and related functions from `api` package to `client` package
- Switched SQLite driver to pure-Go implementation (no CGO dependency) ([#17](https://github.com/microsoft/durabletask-go/pull/17)) - contributed by [@ItalyPaleAle](https://github.com/ItalyPaleAle)
- Orchestration metadata fetching now gets input and output data by default (previously had to opt-in)
- Removed "input" parameter from CallActivity APIs and replaced with options pattern
- Removed "reason" parameter from Termination APIs and replaced with options pattern
- Renamed api.WithJsonEventPayload to api.WithEventPayload
- (Tests) Switched from `assert` to `require` in several tests to simplify code

### Fixed

- Timeout error propagation in gRPC client
- Various static analysis warnings

## [v0.2.4] - 2023-05-25

### Fixed

- Fix ticker memory leak - contributed by [@yaron2](https://github.com/yaron2)
