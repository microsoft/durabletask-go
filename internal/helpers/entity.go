package helpers

// EntityRequestEventName is the event name used for all entity operation requests.
// This matches the .NET DTFx EntityMessageEventNames.RequestMessageEventName constant.
const EntityRequestEventName = "op"

const entitySignalPropertyPrefix = "__durabletask.entity.signal."

func EntitySignalProperty(requestID string) string {
	return entitySignalPropertyPrefix + requestID
}

// EntityRequestMessage is the payload sent to an entity for operation requests.
// This matches the .NET DTFx RequestMessage format for wire compatibility.
type EntityRequestMessage struct {
	ID               string `json:"id"`
	ParentInstanceID string `json:"parentInstanceId,omitempty"`
	IsSignal         bool   `json:"isSignal"`
	Operation        string `json:"operation"`
	Input            string `json:"input,omitempty"`
}
