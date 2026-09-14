package api

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_API_EntityID_String(t *testing.T) {
	id := NewEntityID("Counter", "myCounter")
	assert.Equal(t, "@counter@myCounter", id.String())
}

func Test_API_EntityID_JSON(t *testing.T) {
	type payload struct {
		Entity EntityID
	}
	encoded, err := json.Marshal(payload{Entity: NewEntityID("Counter", "Key@Part")})
	require.NoError(t, err)
	assert.JSONEq(t, `{"Entity":"@counter@Key@Part"}`, string(encoded))

	var decoded payload
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	assert.Equal(t, NewEntityID("counter", "Key@Part"), decoded.Entity)

	for _, invalid := range []string{
		`null`,
		`{"Name":"counter","Key":"key"}`,
		`"counter@key"`,
		`"@@key"`,
	} {
		t.Run(invalid, func(t *testing.T) {
			require.Error(t, json.Unmarshal([]byte(invalid), new(EntityID)))
		})
	}
}

func Test_API_EntityMetadata_StateAvailability(t *testing.T) {
	var state int
	require.ErrorIs(t, (&EntityMetadata{}).ReadState(&state), ErrEntityStateNotIncluded)
	require.ErrorIs(t, (&EntityMetadata{StateIncluded: true}).ReadState(&state), ErrEntityHasNoState)
	require.NoError(t, (&EntityMetadata{
		StateIncluded:   true,
		HasState:        true,
		SerializedState: "42",
	}).ReadState(&state))
	assert.Equal(t, 42, state)
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
		{name: "invalid empty name", input: "@@key1", wantErr: true},
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

func Test_API_NewEntityID_InvalidNamePanics(t *testing.T) {
	assert.Panics(t, func() { NewEntityID("", "key") })
	assert.Panics(t, func() { NewEntityID("bad@name", "key") })
}

func FuzzEntityIDRoundTrip(f *testing.F) {
	f.Add("Counter", "key")
	f.Add("name", "key@with@separators")
	f.Fuzz(func(t *testing.T, name, key string) {
		if name == "" || strings.Contains(name, "@") {
			t.Skip()
		}
		entityID := NewEntityID(name, key)
		parsed, err := EntityIDFromString(entityID.String())
		require.NoError(t, err)
		require.Equal(t, entityID, parsed)
	})
}
