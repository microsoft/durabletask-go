package api

import (
	"encoding/json"
	"fmt"
)

// DataConverter serializes application payloads into the text fields used by
// the Durable Task protocol. Implementations must return valid UTF-8, be
// deterministic, and be safe for concurrent use. Because converter identity is
// not stored in the protocol, replacements must remain able to decode payloads
// written by earlier deployments.
type DataConverter interface {
	Serialize(value any) (string, error)
	Deserialize(payload string, target any) error
}

// JSONDataConverter preserves the SDK's standard encoding/json payload format.
type JSONDataConverter struct{}

func (JSONDataConverter) Serialize(value any) (string, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func (JSONDataConverter) Deserialize(payload string, target any) error {
	if target == nil {
		return nil
	}
	if err := json.Unmarshal([]byte(payload), target); err != nil {
		return fmt.Errorf("failed to deserialize payload: %w", err)
	}
	return nil
}

// DefaultDataConverter returns the converter used when none is configured.
func DefaultDataConverter() DataConverter {
	return JSONDataConverter{}
}

// NormalizeDataConverter returns the default JSON converter for nil inputs.
func NormalizeDataConverter(converter DataConverter) DataConverter {
	if converter == nil {
		return DefaultDataConverter()
	}
	return converter
}

// SerializeData serializes a typed payload and rejects an empty encoding, which
// cannot be distinguished from an absent payload across all protocol surfaces.
func SerializeData(converter DataConverter, value any) (string, error) {
	payload, err := NormalizeDataConverter(converter).Serialize(value)
	if err != nil {
		return "", err
	}
	if payload == "" {
		return "", fmt.Errorf("data converter returned an empty payload")
	}
	return payload, nil
}

func deserializePayload(converter DataConverter, payload string, target any) error {
	if target == nil || payload == "" {
		return nil
	}
	return NormalizeDataConverter(converter).Deserialize(payload, target)
}
