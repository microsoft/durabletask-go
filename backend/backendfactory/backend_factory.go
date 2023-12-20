package backendfactory

import (
	"fmt"
	"time"

	"github.com/microsoft/durabletask-go/backend"
	"github.com/microsoft/durabletask-go/backend/sqlite"
)

const (
	SqliteBackendType = "workflowbackend.sqlite"
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

	if connectionString, ok := metadata["connectionString"]; ok {
		sqliteOptions.FilePath = connectionString
	}

	if orchestrationLockTimeout, ok := metadata["orchestrationLockTimeout"]; ok {
		if duration, err := time.ParseDuration(orchestrationLockTimeout); err == nil {
			sqliteOptions.OrchestrationLockTimeout = duration
		} else {
			log.Errorf("Invalid orchestrationLockTimeout provided in backend workflow component: %v", err)
		}
	}

	if activityLockTimeout, ok := metadata["activityLockTimeout"]; ok {
		if duration, err := time.ParseDuration(activityLockTimeout); err == nil {
			sqliteOptions.ActivityLockTimeout = duration
		} else {
			log.Errorf("Invalid activityLockTimeout provided in backend workflow component: %v", err)
		}
	}

	return sqlite.NewSqliteBackend(sqliteOptions, log), nil
}

func InitializeWorkflowBackend(backendType string, metadata map[string]string, log backend.Logger) (backend.Backend, error) {
	if factory, ok := backendFactories[backendType]; ok {
		return factory(metadata, log)
	}

	return nil, fmt.Errorf("Invalid backend type: %s", backendType)
}
