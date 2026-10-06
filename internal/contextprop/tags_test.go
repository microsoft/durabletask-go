package contextprop

import (
	"testing"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/tagcodec"
	"github.com/stretchr/testify/require"
)

func TestEncodeDecode(t *testing.T) {
	tags := Encode(&api.OrchestrationContextInfo{
		InstanceID:       "instance",
		Name:             "orchestration",
		Version:          "v1",
		ParentInstanceID: "parent",
	}, api.ContextFields{"tenant": "alpha"})

	info, fields := Decode(tags)
	if info.InstanceID != "instance" ||
		info.Name != "orchestration" ||
		info.Version != "v1" ||
		info.ParentInstanceID != "parent" {
		t.Fatalf("unexpected info: %+v", info)
	}

	if fields["tenant"] != "alpha" {
		t.Fatalf("tenant = %q, want alpha", fields["tenant"])
	}
}

func TestEncodeSeparatesContextFieldsAndUserTags(t *testing.T) {
	tags := Encode(
		nil,
		api.ContextFields{"tenant": "context"},
		map[string]string{"team": "tag"},
	)
	_, fields := Decode(tags)
	if fields["tenant"] != "context" {
		t.Fatalf("tenant = %q, want context", fields["tenant"])
	}
	userTags := tagcodec.DecodeUserTags(tags)
	if userTags["team"] != "tag" {
		t.Fatalf("team = %q, want tag", userTags["team"])
	}
	if _, ok := fields["team"]; ok {
		t.Fatalf("user tag leaked into context fields: %v", fields)
	}
}

func TestEncodeOverwritesReservedCallerFields(t *testing.T) {
	tags := Encode(&api.OrchestrationContextInfo{}, api.ContextFields{
		api.ReservedContextFieldPrefix + "orchestration_version": "spoofed",
	})
	info, fields := Decode(tags)
	if info.Version != "" {
		t.Fatalf("version = %q, want empty", info.Version)
	}
	if fields != nil {
		t.Fatalf("reserved field leaked into caller fields: %v", fields)
	}
}

func TestEncodeWithoutIdentity(t *testing.T) {
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
			encoded := Encode(nil, test.fields, test.tags)
			require.Equal(t, test.want, encoded)
			info, fields := Decode(encoded)
			require.Equal(t, api.OrchestrationContextInfo{}, info)
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

func TestDecodeRetainedTagFormats(t *testing.T) {
	for _, test := range []struct {
		name   string
		tags   map[string]string
		fields api.ContextFields
		user   map[string]string
	}{
		{name: "absent"},
		{name: "empty", tags: map[string]string{}},
		{
			name:   "legacy plain fields",
			tags:   map[string]string{"tenant": "plain", "empty": ""},
			fields: api.ContextFields{"tenant": "plain", "empty": ""},
			user:   map[string]string{"tenant": "plain", "empty": ""},
		},
		{
			name: "legacy namespaced fields and user tags",
			tags: map[string]string{
				tagcodec.ContextFieldPrefix + "tenant": "context",
				tagcodec.UserTagPrefix + "tenant":      "user",
				tagcodec.ContextFieldPrefix + "empty":  "",
			},
			fields: api.ContextFields{"tenant": "context", "empty": ""},
			user:   map[string]string{"tenant": "user"},
		},
		{
			name: "encoded plain user tags are not fields",
			tags: map[string]string{
				tagcodec.ContextEncodingTag: "1",
				"tenant":                    "user",
				"empty":                     "",
			},
			user: map[string]string{"tenant": "user", "empty": ""},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, fields := Decode(test.tags)
			require.Equal(t, test.fields, fields)
			require.Equal(t, test.user, tagcodec.DecodeUserTagsOrPlain(test.tags))
			_, missing := fields["missing"]
			require.False(t, missing)
			if _, present := test.fields["empty"]; present {
				value, ok := fields["empty"]
				require.True(t, ok)
				require.Empty(t, value)
			}
		})
	}
}

func TestIdentityEncodingRetainsExplicitEmptyValues(t *testing.T) {
	encoded := Encode(&api.OrchestrationContextInfo{}, nil)
	require.Equal(t, map[string]string{
		tagcodec.ContextEncodingTag: "1",
		instanceIDTag:               "",
		nameTag:                     "",
		versionTag:                  "",
		parentInstanceIDTag:         "",
	}, encoded)
	_, fields := Decode(encoded)
	require.Nil(t, fields)
	require.Nil(t, tagcodec.DecodeUserTagsOrPlain(encoded))
}

func TestCloneDoesNotAliasTags(t *testing.T) {
	require.Nil(t, Clone[map[string]string](nil))
	require.Nil(t, Clone(map[string]string{}))
	original := map[string]string{"empty": ""}
	cloned := Clone(original)
	cloned["empty"] = "changed"
	require.Equal(t, map[string]string{"empty": ""}, original)
}
