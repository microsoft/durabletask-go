package helpers

import (
	"fmt"
	"strings"
)

// ValidateEntityName applies the durable-entity naming constraints used by the wire format.
func ValidateEntityName(name string) error {
	switch {
	case name == "":
		return fmt.Errorf("invalid entity id: entity name must not be empty")
	case strings.Contains(name, "@"):
		return fmt.Errorf("invalid entity id: entity name %q must not contain '@'", name)
	default:
		return nil
	}
}

// ParseEntityInstanceID parses an entity instance ID in the format "@<name>@<key>".
func ParseEntityInstanceID(instanceID string) (string, string, error) {
	if !strings.HasPrefix(instanceID, "@") {
		return "", "", fmt.Errorf("invalid entity instance ID format: %q", instanceID)
	}

	trimmed := instanceID[1:]
	name, key, ok := strings.Cut(trimmed, "@")
	if !ok {
		return "", "", fmt.Errorf("invalid entity instance ID format: missing second '@'")
	}
	if err := ValidateEntityName(name); err != nil {
		return "", "", err
	}
	return strings.ToLower(name), key, nil
}

// IsEntityInstanceID reports whether the instance ID uses the reserved entity format.
func IsEntityInstanceID(instanceID string) bool {
	_, _, err := ParseEntityInstanceID(instanceID)
	return err == nil
}

// ValidateOrchestrationInstanceID rejects orchestration IDs that collide with entity instance IDs.
func ValidateOrchestrationInstanceID(instanceID string) error {
	if instanceID == "" {
		return nil
	}
	if IsEntityInstanceID(instanceID) {
		return fmt.Errorf("orchestration instance ID %q uses the reserved entity format", instanceID)
	}
	return nil
}
