package backendfactory

import (
	"fmt"
	"strings"
	"time"

	"github.com/microsoft/durabletask-go/backend"
	"github.com/microsoft/durabletask-go/backend/sqlite"
)

const (
	SqliteBackendType = "sqlite"
)

type BackendFactory func(metadata map[string]string, log backend.Logger) (backend.Backend, error)

var backendFactories = map[string]BackendFactory{
	SqliteBackendType: getSqliteBackend,
}

func getSqliteBackend(metadata map[string]string, log backend.Logger) (backend.Backend, error) {
	sqliteOptions := &sqlite.SqliteOptions{
		OrchestrationLockTimeout: 2 * time.Minute,
		ActivityLockTimeout:      2 * time.Minute,
		FilePath:                 "",
	}

	for key, value := range metadata {
		switch strings.ToLower(key) {
		case "connectionstring":
			sqliteOptions.FilePath = value
		case "orchestrationlocktimeout":
			if duration, err := time.ParseDuration(value); err == nil {
				sqliteOptions.OrchestrationLockTimeout = duration
			} else {
				log.Errorf("Invalid orchestrationLockTimeout provided in backend workflow component: %v", err)
			}
		case "activitylocktimeout":
			if duration, err := time.ParseDuration(value); err == nil {
				sqliteOptions.ActivityLockTimeout = duration
			} else {
				log.Errorf("Invalid activityLockTimeout provided in backend workflow component: %v", err)
			}
		}
	}

	return sqlite.NewSqliteBackend(sqliteOptions, log), nil
}

func InitializeBackend(backendType string, metadata map[string]string, log backend.Logger) (backend.Backend, error) {
	if factory, ok := backendFactories[backendType]; ok {
		return factory(metadata, log)
	}

	return nil, fmt.Errorf("Invalid backend type: %s", backendType)
}
