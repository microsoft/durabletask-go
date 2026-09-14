package main

import (
	"github.com/microsoft/durabletask-go/cmd/orchestratorvet/analysis/orchestratorgo"
	"golang.org/x/tools/go/analysis/unitchecker"
)

func main() {
	unitchecker.Main(orchestratorgo.Analyzer)
}
