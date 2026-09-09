package helpers

const entitySignalPropertyPrefix = "__durabletask.entity.signal."

func EntitySignalProperty(requestID string) string {
	return entitySignalPropertyPrefix + requestID
}
