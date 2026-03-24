package mocks

import (
	"context"

	"github.com/microsoft/durabletask-go/api"
	"github.com/stretchr/testify/mock"
)

// EntityBackend is a mock that implements both backend.Backend and backend.EntityBackend.
type EntityBackend struct {
	Backend
}

type EntityBackend_Expecter struct {
	mock *mock.Mock
}

func (_m *EntityBackend) EXPECT() *EntityBackend_Expecter {
	return &EntityBackend_Expecter{mock: &_m.Mock}
}

// GetEntityMetadata provides a mock function
func (_m *EntityBackend) GetEntityMetadata(_a0 context.Context, _a1 api.EntityID, _a2 bool) (*api.EntityMetadata, error) {
	ret := _m.Called(_a0, _a1, _a2)

	if len(ret) == 0 {
		panic("no return value specified for GetEntityMetadata")
	}

	var r0 *api.EntityMetadata
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, api.EntityID, bool) (*api.EntityMetadata, error)); ok {
		return rf(_a0, _a1, _a2)
	}
	if rf, ok := ret.Get(0).(func(context.Context, api.EntityID, bool) *api.EntityMetadata); ok {
		r0 = rf(_a0, _a1, _a2)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*api.EntityMetadata)
		}
	}
	if rf, ok := ret.Get(1).(func(context.Context, api.EntityID, bool) error); ok {
		r1 = rf(_a0, _a1, _a2)
	} else {
		r1 = ret.Error(1)
	}
	return r0, r1
}

func (_e *EntityBackend_Expecter) GetEntityMetadata(_a0 interface{}, _a1 interface{}, _a2 interface{}) *EntityBackend_GetEntityMetadata_Call {
	return &EntityBackend_GetEntityMetadata_Call{Call: _e.mock.On("GetEntityMetadata", _a0, _a1, _a2)}
}

type EntityBackend_GetEntityMetadata_Call struct {
	*mock.Call
}

func (_c *EntityBackend_GetEntityMetadata_Call) Return(_a0 *api.EntityMetadata, _a1 error) *EntityBackend_GetEntityMetadata_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

// QueryEntities provides a mock function
func (_m *EntityBackend) QueryEntities(_a0 context.Context, _a1 api.EntityQuery) (*api.EntityQueryResults, error) {
	ret := _m.Called(_a0, _a1)

	if len(ret) == 0 {
		panic("no return value specified for QueryEntities")
	}

	var r0 *api.EntityQueryResults
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, api.EntityQuery) (*api.EntityQueryResults, error)); ok {
		return rf(_a0, _a1)
	}
	if rf, ok := ret.Get(0).(func(context.Context, api.EntityQuery) *api.EntityQueryResults); ok {
		r0 = rf(_a0, _a1)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*api.EntityQueryResults)
		}
	}
	if rf, ok := ret.Get(1).(func(context.Context, api.EntityQuery) error); ok {
		r1 = rf(_a0, _a1)
	} else {
		r1 = ret.Error(1)
	}
	return r0, r1
}

func (_e *EntityBackend_Expecter) QueryEntities(_a0 interface{}, _a1 interface{}) *EntityBackend_QueryEntities_Call {
	return &EntityBackend_QueryEntities_Call{Call: _e.mock.On("QueryEntities", _a0, _a1)}
}

type EntityBackend_QueryEntities_Call struct {
	*mock.Call
}

func (_c *EntityBackend_QueryEntities_Call) Return(_a0 *api.EntityQueryResults, _a1 error) *EntityBackend_QueryEntities_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

// CleanEntityStorage provides a mock function
func (_m *EntityBackend) CleanEntityStorage(_a0 context.Context, _a1 api.CleanEntityStorageRequest) (*api.CleanEntityStorageResult, error) {
	ret := _m.Called(_a0, _a1)

	if len(ret) == 0 {
		panic("no return value specified for CleanEntityStorage")
	}

	var r0 *api.CleanEntityStorageResult
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, api.CleanEntityStorageRequest) (*api.CleanEntityStorageResult, error)); ok {
		return rf(_a0, _a1)
	}
	if rf, ok := ret.Get(0).(func(context.Context, api.CleanEntityStorageRequest) *api.CleanEntityStorageResult); ok {
		r0 = rf(_a0, _a1)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*api.CleanEntityStorageResult)
		}
	}
	if rf, ok := ret.Get(1).(func(context.Context, api.CleanEntityStorageRequest) error); ok {
		r1 = rf(_a0, _a1)
	} else {
		r1 = ret.Error(1)
	}
	return r0, r1
}

func (_e *EntityBackend_Expecter) CleanEntityStorage(_a0 interface{}, _a1 interface{}) *EntityBackend_CleanEntityStorage_Call {
	return &EntityBackend_CleanEntityStorage_Call{Call: _e.mock.On("CleanEntityStorage", _a0, _a1)}
}

type EntityBackend_CleanEntityStorage_Call struct {
	*mock.Call
}

func (_c *EntityBackend_CleanEntityStorage_Call) Return(_a0 *api.CleanEntityStorageResult, _a1 error) *EntityBackend_CleanEntityStorage_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}
