package main

import (
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/microsoft/durabletask-go/task"
)

// HelloCities is an orchestrator function that generates a "hello" message for several cities.
func HelloCities(ctx *task.OrchestrationContext) (any, error) {
	var helloTokyo string
	if err := ctx.CallActivity(SayHello, "Tokyo").Await(&helloTokyo); err != nil {
		return nil, err
	}
	var helloLondon string
	if err := ctx.CallActivity(SayHello, "London").Await(&helloLondon); err != nil {
		return nil, err
	}
	var helloSeattle string
	if err := ctx.CallActivity(SayHello, "Seattle").Await(&helloSeattle); err != nil {
		return nil, err
	}
	return []string{helloTokyo, helloLondon, helloSeattle}, nil
}

// SayHello is an activity function that takes a city name as an input and returns "Hello, {city}!"
func SayHello(ctx task.ActivityContext) (any, error) {
	var input string
	if err := ctx.GetInput(&input); err != nil {
		return "", err
	}
	return fmt.Sprintf("Hello, %s!", input), nil
}

func main() {
	listenAddr := ":8080"
	if val, ok := os.LookupEnv("FUNCTIONS_CUSTOMHANDLER_PORT"); ok {
		listenAddr = ":" + val
	}

	http.HandleFunc("/HelloCities", MapOrchestrator(HelloCities))
	http.HandleFunc("/SayHello", MapActivity(SayHello))

	log.Printf("Listening for function invocations on %s\n", listenAddr)
	log.Fatal(http.ListenAndServe(listenAddr, nil))
}
