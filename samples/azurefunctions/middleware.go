package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/api/protos"
	"github.com/dapr/durabletask-go/task"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// NOTE: For more details on custom handlers, see https://learn.microsoft.com/en-us/azure/azure-functions/functions-custom-handlers.

type InvokeRequest struct {
	Data     map[string]interface{}
	Metadata map[string]interface{}
}

type InvokeResponse struct {
	Outputs     map[string]string
	Logs        []string
	ReturnValue json.RawMessage
}

func MapOrchestrator(o task.Orchestrator) func(http.ResponseWriter, *http.Request) {
	r := task.NewTaskRegistry()
	if err := r.AddOrchestratorN("*", o); err != nil {
		panic(fmt.Errorf("ERROR: Failed to register the orchestrator function: %w", err))
	}
	executor := task.NewTaskExecutor(r)

	return func(w http.ResponseWriter, httpReq *http.Request) {
		var invokeRequest InvokeRequest
		d := json.NewDecoder(httpReq.Body)
		d.Decode(&invokeRequest)

		// TODO: Give the schema, construct the context object and invoke the orchestrator
		contextParam := invokeRequest.Data["context"]
		base64encodedPayload := contextParam.(string)

		if err := json.Unmarshal([]byte(base64encodedPayload), &base64encodedPayload); err != nil {
			fmt.Printf("ERROR: Failed to json-decode context payload string: %v\n", err)
			return
		}

		protoBytes, err := base64.StdEncoding.DecodeString(base64encodedPayload)
		if err != nil {
			fmt.Printf("ERROR: Failed to base64-decode request string: %v\n", err)
			return
		}

		var request protos.OrchestratorRequest
		if err := proto.Unmarshal(protoBytes, &request); err != nil {
			fmt.Printf("ERROR: Failed to deserialize request protobuf: %v\n", err)
			return
		}
		fmt.Printf("Orchestrator request for instance ID '%s': %v\n", request.InstanceId, &request)

		results, err := executor.ExecuteOrchestrator(context.TODO(), api.InstanceID(request.InstanceId), request.PastEvents, request.NewEvents)
		if err != nil {
			fmt.Printf("ERROR: Unexpected failure executing the orchestrator function: %v\n", err)
			return
		}
		fmt.Printf("Orchestrator returned a response: %v\n", results.Response)

		respBytes, err := proto.Marshal(results.Response)
		if err != nil {
			fmt.Printf("ERROR: Failed to marshal orchestrator results to protobuf: %v\n", err)
			return
		}

		base64bytes := base64.StdEncoding.EncodeToString(respBytes)
		fmt.Printf("Sending back base64 encoded string: %s\n", base64bytes)

		// Send the response back to the Functions host in a JSON envelope
		invokeResponse := &InvokeResponse{ReturnValue: []byte(`"` + base64bytes + `"`)}
		responseJson, err := json.Marshal(invokeResponse)
		if err != nil {
			fmt.Printf("ERROR: Failed to marshal response payload to JSON: %v\n", err)
			return
		}
		fmt.Println("Sending response JSON:", string(responseJson))
		w.Header().Set("Content-Type", "application/json")
		w.Write(responseJson)
	}
}

func MapActivity(a task.Activity) func(http.ResponseWriter, *http.Request) {
	r := task.NewTaskRegistry()
	if err := r.AddActivityN("*", a); err != nil {
		panic(fmt.Errorf("ERROR: Failed to register the activity function: %w", err))
	}
	executor := task.NewTaskExecutor(r)

	return func(w http.ResponseWriter, r *http.Request) {
		var invokeRequest InvokeRequest
		d := json.NewDecoder(r.Body)
		d.Decode(&invokeRequest)

		fmt.Println("Activity request:", invokeRequest)

		sys := invokeRequest.Metadata["sys"].(map[string]interface{})
		name := sys["MethodName"].(string)
		instanceID := invokeRequest.Metadata["instanceId"].(string)

		var rawInput *wrapperspb.StringValue
		if data, ok := invokeRequest.Metadata["data"]; ok && data != nil {
			rawInputStr := data.(string)
			rawInput = wrapperspb.String(rawInputStr)
		}

		ts := &protos.HistoryEvent{
			EventId:   -1,
			Timestamp: timestamppb.New(time.Now()),
			EventType: &protos.HistoryEvent_TaskScheduled{
				TaskScheduled: &protos.TaskScheduledEvent{
					Name:  name,
					Input: rawInput,
				},
			},
		}
		e, err := executor.ExecuteActivity(context.TODO(), api.InstanceID(instanceID), ts)
		if err != nil {
			panic(fmt.Errorf("ERROR: Activity execution failed with an error: %w", err))
		}

		// Send the response back to the Functions host in a JSON envelope
		var returnValue string
		var statusCode int
		if tc := e.GetTaskCompleted(); tc != nil {
			fmt.Printf("Task completed: %v\n", tc.Result.GetValue())
			returnValue = tc.Result.GetValue()
			statusCode = 200
		} else if tf := e.GetTaskFailed(); tf != nil {
			fmt.Printf("Task failed: %v\n", tf.FailureDetails)
			statusCode = 500
		} else {
			panic(fmt.Errorf("Unexpected event type: %v", e))
		}

		invokeResponse := &InvokeResponse{ReturnValue: []byte(returnValue)}
		responseJson, err := json.Marshal(invokeResponse)
		if err != nil {
			fmt.Printf("ERROR: Failed to marshal response payload to JSON: %v\n", err)
			return
		}
		fmt.Printf("Sending %d response: %s\n", statusCode, string(responseJson))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(statusCode)
		w.Write(responseJson)
	}
}
