package contextprop

import (
	"maps"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/tagcodec"
)

const (
	instanceIDTag       = api.ReservedContextFieldPrefix + "instance_id"
	nameTag             = api.ReservedContextFieldPrefix + "orchestration_name"
	versionTag          = api.ReservedContextFieldPrefix + "orchestration_version"
	parentInstanceIDTag = api.ReservedContextFieldPrefix + "parent_instance_id"
)

// Encode returns a new tag map containing immutable fields and orchestration identity.
func Encode(
	info api.OrchestrationContextInfo,
	fields api.ContextFields,
	userTags ...map[string]string,
) map[string]string {
	tags := tagcodec.EncodeContextFields(fields)
	if len(userTags) > 0 {
		tags = tagcodec.Merge(tags, tagcodec.EncodeUserTags(userTags[0]))
	}
	if tags == nil {
		tags = make(map[string]string, 5)
	}
	tags[tagcodec.ContextEncodingTag] = "1"
	tags[instanceIDTag] = string(info.InstanceID)
	tags[nameTag] = info.Name
	tags[versionTag] = info.Version
	tags[parentInstanceIDTag] = string(info.ParentInstanceID)
	return tags
}

// Decode separates orchestration identity from caller-supplied immutable fields.
func Decode(tags map[string]string) (api.OrchestrationContextInfo, api.ContextFields) {
	info := api.OrchestrationContextInfo{
		InstanceID:       api.InstanceID(tags[instanceIDTag]),
		Name:             tags[nameTag],
		Version:          tags[versionTag],
		ParentInstanceID: api.InstanceID(tags[parentInstanceIDTag]),
	}
	return info, api.ContextFields(tagcodec.DecodeContextFields(tags))
}

// Clone returns a defensive copy of tags, or nil when there is nothing to copy.
func Clone[T ~map[string]string](tags T) map[string]string {
	if len(tags) == 0 {
		return nil
	}
	copyOfTags := make(map[string]string, len(tags))
	maps.Copy(copyOfTags, tags)
	return copyOfTags
}
