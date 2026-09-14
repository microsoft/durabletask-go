package tagcodec

import (
	"maps"
	"strings"
)

const (
	ContextPrefix      = "__durabletask.context."
	ContextFieldPrefix = ContextPrefix + "field."
	ContextEncodingTag = ContextPrefix + "encoding"
	UserTagPrefix      = "__durabletask.tags."
)

func EncodeContextFields(fields map[string]string) map[string]string {
	if len(fields) == 0 {
		return nil
	}
	encoded := make(map[string]string, len(fields))
	encoded[ContextEncodingTag] = "1"
	for key, value := range fields {
		if strings.HasPrefix(key, ContextPrefix) || strings.HasPrefix(key, UserTagPrefix) {
			continue
		}
		encoded[ContextFieldPrefix+key] = value
	}
	return encoded
}

func DecodeContextFields(tags map[string]string) map[string]string {
	var fields map[string]string
	encoded := tags[ContextEncodingTag] == "1"
	for key, value := range tags {
		switch {
		case strings.HasPrefix(key, ContextFieldPrefix):
			if fields == nil {
				fields = make(map[string]string)
			}
			fields[strings.TrimPrefix(key, ContextFieldPrefix)] = value
		case !encoded && !strings.HasPrefix(key, ContextPrefix) && !strings.HasPrefix(key, UserTagPrefix):
			// Backward compatibility for histories written before fields had a namespace.
			if fields == nil {
				fields = make(map[string]string)
			}
			fields[key] = value
		}
	}
	return fields
}

func EncodeUserTags(tags map[string]string) map[string]string {
	if len(tags) == 0 {
		return nil
	}
	encoded := make(map[string]string, len(tags)+1)
	encoded[ContextEncodingTag] = "1"
	for key, value := range tags {
		encoded[key] = value
	}
	return encoded
}

func DecodeUserTags(tags map[string]string) map[string]string {
	var decoded map[string]string
	for key, value := range tags {
		switch {
		case strings.HasPrefix(key, UserTagPrefix):
			key = strings.TrimPrefix(key, UserTagPrefix)
		case strings.HasPrefix(key, ContextPrefix):
			continue
		}
		if decoded == nil {
			decoded = make(map[string]string)
		}
		decoded[key] = value
	}
	return decoded
}

// DecodeUserTagsOrPlain supports services that return user tags without the Go
// SDK namespace while filtering Durable Task context metadata.
func DecodeUserTagsOrPlain(tags map[string]string) map[string]string {
	return DecodeUserTags(tags)
}

func Merge(destination map[string]string, encoded map[string]string) map[string]string {
	if len(encoded) == 0 {
		return destination
	}
	if destination == nil {
		destination = make(map[string]string, len(encoded))
	}
	maps.Copy(destination, encoded)
	return destination
}
