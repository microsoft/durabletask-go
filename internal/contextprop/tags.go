package contextprop

import (
	"maps"

	"github.com/microsoft/durabletask-go/api"
	"github.com/microsoft/durabletask-go/internal/tagcodec"
)

// Encode returns a new tag map containing caller fields and user tags.
func Encode(fields api.ContextFields, userTags map[string]string) map[string]string {
	return tagcodec.Merge(tagcodec.EncodeContextFields(fields), tagcodec.EncodeUserTags(userTags))
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
