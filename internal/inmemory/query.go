package inmemory

import (
	"cmp"
	"context"
	"encoding/base64"
	"slices"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/microsoft/durabletask-go/internal/protos"
)

// QueryInstances returns instances that match a query, ordered by creation time.
func (s *Server) QueryInstances(_ context.Context, req *protos.QueryInstancesRequest) (*protos.QueryInstancesResponse, error) {
	query := req.GetQuery()
	var cursorTime time.Time
	var cursorID string
	if token := query.GetContinuationToken().GetValue(); token != "" {
		var err error
		cursorTime, cursorID, err = decodeInstanceCursor(token)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid continuation token %q", token)
		}
	}
	pageSize := int(query.GetMaxInstanceCount())
	if pageSize <= 0 {
		pageSize = defaultQueryPageSize
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	var matches []*instance
	for _, inst := range s.instances {
		if len(query.GetRuntimeStatus()) > 0 && !slices.Contains(query.GetRuntimeStatus(), inst.status) {
			continue
		}
		if !inRange(inst.createdAt, query.GetCreatedTimeFrom(), query.GetCreatedTimeTo()) {
			continue
		}
		if prefix := query.GetInstanceIdPrefix().GetValue(); prefix != "" && !strings.HasPrefix(inst.id, prefix) {
			continue
		}
		matches = append(matches, inst)
	}
	slices.SortFunc(matches, func(a, b *instance) int {
		return cmp.Or(a.createdAt.Compare(b.createdAt), cmp.Compare(a.id, b.id))
	})

	resp := &protos.QueryInstancesResponse{}
	start := 0
	if cursorID != "" {
		start = slices.IndexFunc(matches, func(inst *instance) bool {
			return cmp.Or(inst.createdAt.Compare(cursorTime), cmp.Compare(inst.id, cursorID)) > 0
		})
		if start < 0 {
			start = len(matches)
		}
	}
	end := min(start+pageSize, len(matches))
	for _, inst := range matches[start:end] {
		resp.OrchestrationState = append(resp.OrchestrationState, inst.state(query.GetFetchInputsAndOutputs()))
	}
	if end < len(matches) {
		last := matches[end-1]
		resp.ContinuationToken = wrapperspb.String(encodeInstanceCursor(last.createdAt, last.id))
	}
	return resp, nil
}

func encodeInstanceCursor(createdAt time.Time, instanceID string) string {
	id := base64.RawURLEncoding.EncodeToString([]byte(instanceID))
	return strconv.FormatInt(createdAt.UnixNano(), 10) + ":" + id
}

func decodeInstanceCursor(token string) (time.Time, string, error) {
	timestamp, encodedID, ok := strings.Cut(token, ":")
	if !ok {
		return time.Time{}, "", status.Error(codes.InvalidArgument, "missing cursor separator")
	}
	nanos, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		return time.Time{}, "", err
	}
	id, err := base64.RawURLEncoding.DecodeString(encodedID)
	if err != nil || len(id) == 0 {
		return time.Time{}, "", status.Error(codes.InvalidArgument, "invalid cursor instance ID")
	}
	return time.Unix(0, nanos), string(id), nil
}

// ListInstanceIds returns IDs of terminal instances, ordered by completion time.
func (s *Server) ListInstanceIds(_ context.Context, req *protos.ListInstanceIdsRequest) (*protos.ListInstanceIdsResponse, error) {
	pageSize := int(req.GetPageSize())
	if pageSize <= 0 {
		pageSize = defaultQueryPageSize
	}
	var cursorTime time.Time
	var cursorID string
	if token := req.GetLastInstanceKey().GetValue(); token != "" {
		var err error
		cursorTime, cursorID, err = decodeInstanceCursor(token)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "invalid continuation token %q", token)
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	var matches []*instance
	for _, inst := range s.instances {
		if !isTerminal(inst.status) {
			continue
		}
		if len(req.GetRuntimeStatus()) > 0 && !slices.Contains(req.GetRuntimeStatus(), inst.status) {
			continue
		}
		if !inRange(inst.completedAt, req.GetCompletedTimeFrom(), req.GetCompletedTimeTo()) {
			continue
		}
		matches = append(matches, inst)
	}
	slices.SortFunc(matches, func(a, b *instance) int {
		return cmp.Or(a.completedAt.Compare(b.completedAt), cmp.Compare(a.id, b.id))
	})

	start := 0
	if cursorID != "" {
		start = slices.IndexFunc(matches, func(inst *instance) bool {
			return cmp.Or(inst.completedAt.Compare(cursorTime), cmp.Compare(inst.id, cursorID)) > 0
		})
		if start < 0 {
			start = len(matches)
		}
	}
	resp := &protos.ListInstanceIdsResponse{}
	end := min(start+pageSize, len(matches))
	for _, inst := range matches[start:end] {
		resp.InstanceIds = append(resp.InstanceIds, inst.id)
	}
	if end < len(matches) {
		last := matches[end-1]
		resp.LastInstanceKey = wrapperspb.String(encodeInstanceCursor(last.completedAt, last.id))
	}
	return resp, nil
}

// PurgeInstances deletes terminal instances and, optionally, their sub-orchestrations.
func (s *Server) PurgeInstances(_ context.Context, req *protos.PurgeInstancesRequest) (*protos.PurgeInstancesResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	deleted := 0
	switch r := req.GetRequest().(type) {
	case *protos.PurgeInstancesRequest_InstanceId:
		inst, ok := s.instances[r.InstanceId]
		if ok && !isTerminal(inst.status) {
			return nil, notCompletedError(inst.id)
		}
		if ok {
			deleted = s.purgeLocked(inst, req.GetRecursive())
		}
	case *protos.PurgeInstancesRequest_InstanceBatch:
		for _, id := range r.InstanceBatch.GetInstanceIds() {
			if inst, ok := s.instances[id]; ok && isTerminal(inst.status) {
				deleted += s.purgeLocked(inst, req.GetRecursive())
			}
		}
	case *protos.PurgeInstancesRequest_PurgeInstanceFilter:
		filter := r.PurgeInstanceFilter
		var matches []*instance
		for _, inst := range s.instances {
			if !isTerminal(inst.status) ||
				(len(filter.GetRuntimeStatus()) > 0 && !slices.Contains(filter.GetRuntimeStatus(), inst.status)) ||
				!inRange(inst.createdAt, filter.GetCreatedTimeFrom(), filter.GetCreatedTimeTo()) {
				continue
			}
			matches = append(matches, inst)
		}
		for _, inst := range matches {
			if s.instances[inst.id] == inst {
				deleted += s.purgeLocked(inst, req.GetRecursive())
			}
		}
	default:
		return nil, status.Error(codes.InvalidArgument, "a purge request must specify an instance ID, batch, or filter")
	}
	if deleted > 0 {
		s.notifyLocked()
	}
	return &protos.PurgeInstancesResponse{DeletedInstanceCount: int32(deleted), IsComplete: wrapperspb.Bool(true)}, nil
}

func (s *Server) purgeLocked(inst *instance, recursive bool) int {
	inst.stopTimers()
	delete(s.instances, inst.id)
	deleted := 1
	if recursive {
		for _, childID := range inst.children {
			if child, ok := s.instances[childID]; ok && isTerminal(child.status) &&
				child.parent.GetOrchestrationInstance().GetInstanceId() == inst.id {
				deleted += s.purgeLocked(child, true)
			}
		}
	}
	return deleted
}

func inRange(t time.Time, from, to *timestamppb.Timestamp) bool {
	if from != nil && t.Before(from.AsTime()) {
		return false
	}
	if to != nil && t.After(to.AsTime()) {
		return false
	}
	return true
}
