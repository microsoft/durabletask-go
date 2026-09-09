package largepayload

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/protos"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

const referencePrefix = api.DurableTaskPayloadReferencePrefix

type reference struct {
	Location string `json:"location"`
	Size     int    `json:"size"`
	SHA256   string `json:"sha256"`
}

func Externalize(ctx context.Context, options *api.LargePayloadOptions, value *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
	if value == nil {
		return value, nil
	}
	if options == nil {
		if isRecognizedReference(value.GetValue()) {
			return nil, fmt.Errorf("%w: large payload reference requires a configured store and resolver", api.ErrFeatureNotSupported)
		}
		return value, nil
	}
	normalized, err := api.NormalizeLargePayloadOptions(options)
	if err != nil {
		return nil, err
	}
	isToken, err := isNativePayloadToken(normalized.TokenStore, value.GetValue())
	if err != nil {
		return nil, err
	}
	if isToken {
		return value, nil
	}
	if _, ok, err := parseReference(value.GetValue(), normalized.MaxPayloadBytes); ok || err != nil {
		return value, err
	}
	payload := []byte(value.GetValue())
	if len(payload) > normalized.MaxPayloadBytes {
		return nil, fmt.Errorf("%w: %d bytes exceeds %d", api.ErrLargePayloadTooLarge, len(payload), normalized.MaxPayloadBytes)
	}
	if !exceedsThreshold(normalized, len(payload)) {
		return value, nil
	}
	if normalized.TokenStore != nil {
		token, err := normalized.TokenStore.StoreToken(ctx, append([]byte(nil), payload...))
		if err != nil {
			return nil, fmt.Errorf("failed to store large payload: %w", err)
		}
		if strings.TrimSpace(token) == "" {
			return nil, errors.New("large payload store returned an empty token")
		}
		return wrapperspb.String(token), nil
	}
	location, err := normalized.Store.Store(ctx, append([]byte(nil), payload...))
	if err != nil {
		return nil, fmt.Errorf("failed to store large payload: %w", err)
	}
	if strings.TrimSpace(location) == "" {
		return nil, errors.New("large payload store returned an empty location")
	}
	digest := sha256.Sum256(payload)
	descriptor, err := json.Marshal(reference{
		Location: location,
		Size:     len(payload),
		SHA256:   hex.EncodeToString(digest[:]),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to encode large payload reference: %w", err)
	}
	return wrapperspb.String(referencePrefix + base64.RawURLEncoding.EncodeToString(descriptor)), nil
}

func Hydrate(ctx context.Context, options *api.LargePayloadOptions, value *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
	if value == nil {
		return value, nil
	}
	if options == nil {
		if isRecognizedReference(value.GetValue()) {
			return nil, fmt.Errorf("%w: large payload reference requires a configured resolver", api.ErrFeatureNotSupported)
		}
		return value, nil
	}
	normalized, err := api.NormalizeLargePayloadOptions(options)
	if err != nil {
		return nil, err
	}
	isToken, err := isNativePayloadToken(normalized.TokenStore, value.GetValue())
	if err != nil {
		return nil, err
	}
	if isToken {
		payload, err := normalized.TokenStore.ResolveToken(ctx, value.GetValue())
		if err != nil {
			return nil, fmt.Errorf("failed to resolve large payload: %w", err)
		}
		if len(payload) > normalized.MaxPayloadBytes {
			return nil, fmt.Errorf("%w: %d bytes exceeds %d", api.ErrLargePayloadTooLarge, len(payload), normalized.MaxPayloadBytes)
		}
		return wrapperspb.String(string(payload)), nil
	}
	ref, ok, err := parseReference(value.GetValue(), normalized.MaxPayloadBytes)
	if err != nil || !ok {
		return value, err
	}
	payload, err := normalized.Resolver.Resolve(ctx, ref.Location)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve large payload: %w", err)
	}
	if len(payload) != ref.Size {
		return nil, fmt.Errorf("%w: expected %d bytes, got %d", api.ErrLargePayloadIntegrity, ref.Size, len(payload))
	}
	digest := sha256.Sum256(payload)
	expectedDigest, err := hex.DecodeString(ref.SHA256)
	if err != nil || len(expectedDigest) != sha256.Size {
		return nil, fmt.Errorf("%w: invalid SHA-256 digest", api.ErrLargePayloadReference)
	}
	if subtle.ConstantTimeCompare(digest[:], expectedDigest) != 1 {
		return nil, fmt.Errorf("%w: SHA-256 mismatch", api.ErrLargePayloadIntegrity)
	}
	return wrapperspb.String(string(payload)), nil
}

func TransformHistoryEvent(
	ctx context.Context,
	options *api.LargePayloadOptions,
	event *protos.HistoryEvent,
	externalize bool,
) error {
	transform := Hydrate
	if externalize {
		transform = Externalize
	}
	plan := newTransformPlan(options, transform)
	planHistoryEvent(plan, event)
	return plan.run(ctx)
}

func planHistoryEvent(plan *transformPlan, event *protos.HistoryEvent) {
	if event == nil {
		return
	}
	var target **wrapperspb.StringValue
	switch {
	case event.GetExecutionStarted() != nil:
		target = &event.GetExecutionStarted().Input
	case event.GetExecutionCompleted() != nil:
		target = &event.GetExecutionCompleted().Result
	case event.GetExecutionTerminated() != nil:
		target = &event.GetExecutionTerminated().Input
	case event.GetTaskScheduled() != nil:
		target = &event.GetTaskScheduled().Input
	case event.GetTaskCompleted() != nil:
		target = &event.GetTaskCompleted().Result
	case event.GetSubOrchestrationInstanceCreated() != nil:
		target = &event.GetSubOrchestrationInstanceCreated().Input
	case event.GetSubOrchestrationInstanceCompleted() != nil:
		target = &event.GetSubOrchestrationInstanceCompleted().Result
	case event.GetEventSent() != nil:
		target = &event.GetEventSent().Input
	case event.GetEventRaised() != nil:
		target = &event.GetEventRaised().Input
	case event.GetGenericEvent() != nil:
		target = &event.GetGenericEvent().Data
	case event.GetHistoryState() != nil:
		planOrchestrationState(plan, event.GetHistoryState().OrchestrationState)
		return
	case event.GetContinueAsNew() != nil:
		target = &event.GetContinueAsNew().Input
	case event.GetExecutionSuspended() != nil:
		target = &event.GetExecutionSuspended().Input
	case event.GetExecutionResumed() != nil:
		target = &event.GetExecutionResumed().Input
	case event.GetExecutionRewound() != nil:
		target = &event.GetExecutionRewound().Input
	case event.GetEntityOperationSignaled() != nil:
		target = &event.GetEntityOperationSignaled().Input
	case event.GetEntityOperationCalled() != nil:
		target = &event.GetEntityOperationCalled().Input
	case event.GetEntityOperationCompleted() != nil:
		target = &event.GetEntityOperationCompleted().Output
	default:
		return
	}
	plan.add(target)
}

func planOrchestrationState(plan *transformPlan, state *protos.OrchestrationState) {
	if state == nil {
		return
	}
	plan.add(&state.Input)
	plan.add(&state.Output)
	plan.add(&state.CustomStatus)
}

func TransformOrchestratorRequest(
	ctx context.Context,
	options *api.LargePayloadOptions,
	request *protos.OrchestratorRequest,
) error {
	if request == nil {
		return nil
	}
	plan := newTransformPlan(options, Hydrate)
	for _, events := range [][]*protos.HistoryEvent{request.PastEvents, request.NewEvents} {
		for _, event := range events {
			planHistoryEvent(plan, event)
		}
	}
	return plan.run(ctx)
}

func TransformOrchestratorResponse(
	ctx context.Context,
	options *api.LargePayloadOptions,
	response *protos.OrchestratorResponse,
) error {
	if response == nil {
		return nil
	}
	plan := newTransformPlan(options, Externalize)
	plan.add(&response.CustomStatus)
	for _, action := range response.Actions {
		if action == nil {
			continue
		}
		switch {
		case action.GetScheduleTask() != nil:
			plan.add(&action.GetScheduleTask().Input)
		case action.GetCreateSubOrchestration() != nil:
			plan.add(&action.GetCreateSubOrchestration().Input)
		case action.GetSendEvent() != nil:
			plan.add(&action.GetSendEvent().Data)
		case action.GetCompleteOrchestration() != nil:
			plan.add(&action.GetCompleteOrchestration().Result)
		case action.GetTerminateOrchestration() != nil:
			plan.add(&action.GetTerminateOrchestration().Reason)
		case action.GetSendEntityMessage() != nil:
			message := action.GetSendEntityMessage()
			switch {
			case message.GetEntityOperationSignaled() != nil:
				plan.add(&message.GetEntityOperationSignaled().Input)
			case message.GetEntityOperationCalled() != nil:
				plan.add(&message.GetEntityOperationCalled().Input)
			}
		case action.GetRewindOrchestration() != nil:
			for _, event := range action.GetRewindOrchestration().NewHistory {
				planHistoryEvent(plan, event)
			}
		}
	}
	return plan.run(ctx)
}

func TransformActivityRequest(ctx context.Context, options *api.LargePayloadOptions, request *protos.ActivityRequest) error {
	if request == nil {
		return nil
	}
	var err error
	request.Input, err = Hydrate(ctx, options, request.Input)
	return err
}

func TransformActivityResponse(ctx context.Context, options *api.LargePayloadOptions, response *protos.ActivityResponse) error {
	if response == nil {
		return nil
	}
	var err error
	response.Result, err = Externalize(ctx, options, response.Result)
	return err
}

func TransformEntityBatchRequest(
	ctx context.Context,
	options *api.LargePayloadOptions,
	request *protos.EntityBatchRequest,
) error {
	if request == nil {
		return nil
	}
	plan := newTransformPlan(options, Hydrate)
	plan.add(&request.EntityState)
	for _, operation := range request.Operations {
		if operation == nil {
			continue
		}
		plan.add(&operation.Input)
	}
	return plan.run(ctx)
}

func TransformEntityBatchResult(
	ctx context.Context,
	options *api.LargePayloadOptions,
	result *protos.EntityBatchResult,
) error {
	if result == nil {
		return nil
	}
	plan := newTransformPlan(options, Externalize)
	plan.add(&result.EntityState)
	for _, operationResult := range result.Results {
		if operationResult == nil || operationResult.GetSuccess() == nil {
			continue
		}
		plan.add(&operationResult.GetSuccess().Result)
	}
	for _, action := range result.Actions {
		if action == nil {
			continue
		}
		switch {
		case action.GetSendSignal() != nil:
			plan.add(&action.GetSendSignal().Input)
		case action.GetStartNewOrchestration() != nil:
			plan.add(&action.GetStartNewOrchestration().Input)
		}
	}
	return plan.run(ctx)
}

func TransformOrchestrationState(ctx context.Context, options *api.LargePayloadOptions, state *protos.OrchestrationState) error {
	plan := newTransformPlan(options, Hydrate)
	planOrchestrationState(plan, state)
	return plan.run(ctx)
}

func parseReference(value string, maxPayloadBytes int) (reference, bool, error) {
	if !strings.HasPrefix(value, referencePrefix) {
		return reference{}, false, nil
	}
	encoded := strings.TrimPrefix(value, referencePrefix)
	descriptor, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return reference{}, true, fmt.Errorf("%w: invalid base64 descriptor", api.ErrLargePayloadReference)
	}
	var ref reference
	if err := json.Unmarshal(descriptor, &ref); err != nil {
		return reference{}, true, fmt.Errorf("%w: invalid JSON descriptor", api.ErrLargePayloadReference)
	}
	if strings.TrimSpace(ref.Location) == "" || ref.Size < 0 || ref.Size > maxPayloadBytes {
		return reference{}, true, fmt.Errorf("%w: invalid location or size", api.ErrLargePayloadReference)
	}
	if len(ref.SHA256) != sha256.Size*2 {
		return reference{}, true, fmt.Errorf("%w: invalid SHA-256 digest", api.ErrLargePayloadReference)
	}
	return ref, true, nil
}

func isRecognizedReference(value string) bool {
	return api.IsLargePayloadReference(value)
}

func isBlobReference(value string) bool {
	return strings.HasPrefix(value, api.AzureBlobPayloadReferencePrefixV1) ||
		strings.HasPrefix(value, api.AzureBlobPayloadReferencePrefixV2)
}

// isNativePayloadToken reports whether value is a native token that the
// configured store can handle, validating it when the store supports
// validation. Azure Blob tokens are rejected when no token store can resolve
// them so they are never mistaken for opaque payload data.
func isNativePayloadToken(store api.LargePayloadTokenStore, value string) (bool, error) {
	if store == nil || !store.IsLargePayloadToken(value) {
		if isBlobReference(value) {
			return false, fmt.Errorf(
				"%w: Azure Blob payload token requires an Azure Blob token store",
				api.ErrFeatureNotSupported,
			)
		}
		return false, nil
	}
	if validator, ok := store.(api.LargePayloadTokenValidator); ok {
		if err := validator.ValidateLargePayloadToken(value); err != nil {
			return false, err
		}
	}
	return true, nil
}

// exceedsThreshold reports whether a payload is large enough to externalize.
// Stores such as Azure Blob externalize payloads exactly at the threshold,
// while the built-in reference store externalizes only above it.
func exceedsThreshold(options *api.LargePayloadOptions, size int) bool {
	if size > options.ThresholdBytes {
		return true
	}
	if size < options.ThresholdBytes {
		return false
	}
	policy, ok := options.Store.(api.InclusiveLargePayloadThreshold)
	return ok && policy.UsesInclusiveLargePayloadThreshold()
}
