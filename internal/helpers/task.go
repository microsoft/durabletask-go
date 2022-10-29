package helpers

import (
	"reflect"
	"runtime"
	"strings"
)

func GetTaskFunctionName(f any) string {
	if name, ok := f.(string); ok {
		return name
	} else {
		// this gets the full module path (github.com/org/module/package.function)
		name = runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name()
		startIndex := strings.LastIndexByte(name, '.')
		if startIndex > 0 {
			name = name[startIndex+1:]
		}
		return name
	}
}
