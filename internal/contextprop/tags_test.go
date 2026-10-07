package contextprop

import (
	"testing"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/tagcodec"
	"github.com/stretchr/testify/require"
)

func TestEncodeCallerData(t *testing.T) {
	for _, test := range []struct {
		name   string
		fields api.ContextFields
		tags   map[string]string
		want   map[string]string
	}{
		{name: "absent"},
		{name: "empty", fields: api.ContextFields{}, tags: map[string]string{}},
		{
			name: "user tags only",
			tags: map[string]string{"tenant": ""},
			want: map[string]string{tagcodec.ContextEncodingTag: "1", "tenant": ""},
		},
		{
			name:   "fields only",
			fields: api.ContextFields{"tenant": ""},
			want:   map[string]string{tagcodec.ContextEncodingTag: "1", tagcodec.ContextFieldPrefix + "tenant": ""},
		},
		{
			name:   "same key in separate namespaces",
			fields: api.ContextFields{"tenant": "context"},
			tags:   map[string]string{"tenant": "user"},
			want: map[string]string{
				tagcodec.ContextEncodingTag:            "1",
				tagcodec.ContextFieldPrefix + "tenant": "context",
				"tenant":                               "user",
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			encoded := Encode(test.fields, test.tags)
			require.Equal(t, test.want, encoded)
			fields := api.ContextFields(tagcodec.DecodeContextFields(encoded))
			if len(test.fields) == 0 {
				require.Nil(t, fields, "user tags must not become context fields")
			} else {
				require.Equal(t, test.fields, fields)
				fields["tenant"] = "decoded mutation"
				require.Equal(t, test.want, encoded)
			}
			if test.fields != nil {
				test.fields["tenant"] = "caller mutation"
			}
			if test.tags != nil {
				test.tags["tenant"] = "caller mutation"
			}
			require.Equal(t, test.want, encoded)
		})
	}
}

func TestCloneDoesNotAliasTags(t *testing.T) {
	require.Nil(t, Clone[map[string]string](nil))
	require.Nil(t, Clone(map[string]string{}))
	original := map[string]string{"empty": ""}
	cloned := Clone(original)
	cloned["empty"] = "changed"
	require.Equal(t, map[string]string{"empty": ""}, original)
}
