package api

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_API_EntityID_String(t *testing.T) {
	id := NewEntityID("Counter", "myCounter")
	assert.Equal(t, "@counter@myCounter", id.String())
}

func Test_API_EntityIDFromString(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    EntityID
		wantErr bool
	}{
		{name: "valid", input: "@counter@key1", want: EntityID{Name: "counter", Key: "key1"}},
		{name: "empty key", input: "@entity@", want: EntityID{Name: "entity", Key: ""}},
		{name: "invalid no prefix", input: "no-at-sign", wantErr: true},
		{name: "invalid no second @", input: "@onlyone", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := EntityIDFromString(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
