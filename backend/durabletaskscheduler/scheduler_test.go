package durabletaskscheduler

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewOptions(t *testing.T) {
	opts := NewOptions("https://myscheduler.westus2.durabletask.io", "my-task-hub")

	assert.Equal(t, "https://myscheduler.westus2.durabletask.io", opts.EndpointAddress)
	assert.Equal(t, "my-task-hub", opts.TaskHubName)
	assert.Equal(t, DefaultAuthType, opts.AuthType)
	assert.Equal(t, DefaultResourceID, opts.ResourceID)
}

func TestNewOptionsFromConnectionString(t *testing.T) {
	tests := []struct {
		name        string
		connStr     string
		wantOpts    *Options
		wantErr     bool
		errContains string
	}{
		{
			name:    "full connection string",
			connStr: "Endpoint=https://myscheduler.westus2.durabletask.io;TaskHub=my-task-hub;Authentication=defaultazure",
			wantOpts: &Options{
				EndpointAddress: "https://myscheduler.westus2.durabletask.io",
				TaskHubName:     "my-task-hub",
				AuthType:        AuthTypeDefaultAzure,
				ResourceID:      DefaultResourceID,
			},
		},
		{
			name:    "minimal connection string (no auth)",
			connStr: "Endpoint=https://myscheduler.westus2.durabletask.io;TaskHub=my-task-hub",
			wantOpts: &Options{
				EndpointAddress: "https://myscheduler.westus2.durabletask.io",
				TaskHubName:     "my-task-hub",
				AuthType:        DefaultAuthType,
				ResourceID:      DefaultResourceID,
			},
		},
		{
			name:    "auth type none",
			connStr: "Endpoint=http://localhost:4001;TaskHub=test;Authentication=none",
			wantOpts: &Options{
				EndpointAddress: "http://localhost:4001",
				TaskHubName:     "test",
				AuthType:        AuthTypeNone,
				ResourceID:      DefaultResourceID,
			},
		},
		{
			name:    "case insensitive keys",
			connStr: "endpoint=https://test.durabletask.io;taskhub=hub1;authentication=DefaultAzure",
			wantOpts: &Options{
				EndpointAddress: "https://test.durabletask.io",
				TaskHubName:     "hub1",
				AuthType:        AuthTypeDefaultAzure,
				ResourceID:      DefaultResourceID,
			},
		},
		{
			name:    "extra whitespace",
			connStr: " Endpoint = https://test.durabletask.io ; TaskHub = hub1 ",
			wantOpts: &Options{
				EndpointAddress: "https://test.durabletask.io",
				TaskHubName:     "hub1",
				AuthType:        DefaultAuthType,
				ResourceID:      DefaultResourceID,
			},
		},
		{
			name:    "unknown keys are ignored",
			connStr: "Endpoint=https://test.durabletask.io;TaskHub=hub1;ClientID=abc-123",
			wantOpts: &Options{
				EndpointAddress: "https://test.durabletask.io",
				TaskHubName:     "hub1",
				AuthType:        DefaultAuthType,
				ResourceID:      DefaultResourceID,
			},
		},
		{
			name:        "missing endpoint",
			connStr:     "TaskHub=my-task-hub",
			wantErr:     true,
			errContains: "Endpoint",
		},
		{
			name:        "missing task hub",
			connStr:     "Endpoint=https://test.durabletask.io",
			wantErr:     true,
			errContains: "TaskHub",
		},
		{
			name:        "empty string",
			connStr:     "",
			wantErr:     true,
			errContains: "Endpoint",
		},
		{
			name:        "invalid segment",
			connStr:     "Endpoint=https://test.durabletask.io;TaskHub=hub1;badformat",
			wantErr:     true,
			errContains: "invalid connection string segment",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts, err := NewOptionsFromConnectionString(tt.connStr)

			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errContains)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantOpts.EndpointAddress, opts.EndpointAddress)
			assert.Equal(t, tt.wantOpts.TaskHubName, opts.TaskHubName)
			assert.Equal(t, tt.wantOpts.AuthType, opts.AuthType)
			assert.Equal(t, tt.wantOpts.ResourceID, opts.ResourceID)
		})
	}
}

func TestNewBackend(t *testing.T) {
	opts := NewOptions("https://test.durabletask.io", "test-hub")
	be := NewBackend(opts, nil)

	require.NotNil(t, be)
	assert.Contains(t, be.String(), "test.durabletask.io")
	assert.Contains(t, be.String(), "test-hub")
	assert.NotEmpty(t, be.workerName)
}

func TestBackend_EnsureStarted(t *testing.T) {
	opts := NewOptions("https://test.durabletask.io", "test-hub")
	be := NewBackend(opts, nil)

	// Should fail before Start is called
	err := be.ensureStarted()
	require.Error(t, err)
}
