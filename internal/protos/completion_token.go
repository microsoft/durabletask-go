package protos

import "google.golang.org/protobuf/encoding/protowire"

const (
	workItemCompletionTokenField               = 10 // WorkItem.completionToken proto field number
	orchestratorResponseCompletionTokenField    = 4  // OrchestratorResponse.completionToken proto field number
	activityResponseCompletionTokenField        = 5  // ActivityResponse.completionToken proto field number
)

// GetWorkItemCompletionToken extracts the completionToken (field 10) from a WorkItem's
// unknown fields. This field exists in the upstream proto but is not yet in the generated code.
func GetWorkItemCompletionToken(wi *WorkItem) string {
	return extractStringField(wi.ProtoReflect().GetUnknown(), workItemCompletionTokenField)
}

// SetOrchestratorResponseCompletionToken sets the completionToken (field 4) on an
// OrchestratorResponse as an unknown field. This field exists in the upstream proto
// but is not yet in the generated code.
func SetOrchestratorResponseCompletionToken(resp *OrchestratorResponse, token string) {
	if token == "" {
		return
	}
	b := protowire.AppendTag(nil, orchestratorResponseCompletionTokenField, protowire.BytesType)
	b = protowire.AppendString(b, token)
	raw := resp.ProtoReflect().GetUnknown()
	raw = append(raw, b...)
	resp.ProtoReflect().SetUnknown(raw)
}

// SetActivityResponseCompletionToken sets the completionToken (field 5) on an
// ActivityResponse as an unknown field. This field exists in the upstream proto
// but is not yet in the generated code.
func SetActivityResponseCompletionToken(resp *ActivityResponse, token string) {
	if token == "" {
		return
	}
	b := protowire.AppendTag(nil, activityResponseCompletionTokenField, protowire.BytesType)
	b = protowire.AppendString(b, token)
	raw := resp.ProtoReflect().GetUnknown()
	raw = append(raw, b...)
	resp.ProtoReflect().SetUnknown(raw)
}

// extractStringField extracts a string field with the given field number from raw protobuf bytes.
func extractStringField(raw []byte, fieldNum protowire.Number) string {
	for len(raw) > 0 {
		num, typ, n := protowire.ConsumeTag(raw)
		if n < 0 {
			break
		}
		raw = raw[n:]

		switch typ {
		case protowire.BytesType:
			val, vn := protowire.ConsumeBytes(raw)
			if vn < 0 {
				return ""
			}
			if num == fieldNum {
				return string(val)
			}
			raw = raw[vn:]
		case protowire.VarintType:
			_, vn := protowire.ConsumeVarint(raw)
			if vn < 0 {
				return ""
			}
			raw = raw[vn:]
		case protowire.Fixed32Type:
			_, vn := protowire.ConsumeFixed32(raw)
			if vn < 0 {
				return ""
			}
			raw = raw[vn:]
		case protowire.Fixed64Type:
			_, vn := protowire.ConsumeFixed64(raw)
			if vn < 0 {
				return ""
			}
			raw = raw[vn:]
		default:
			return ""
		}
	}
	return ""
}
