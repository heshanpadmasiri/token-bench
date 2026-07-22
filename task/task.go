// Package task provide the abstraction on top of each task used for the benchmark
package task

import (
	"token-bench/task/internal/claimcheck"
	"token-bench/task/internal/contentbasedrouting"
	"token-bench/task/internal/contentenricher"
	"token-bench/task/internal/deadletterchannel"
	"token-bench/task/internal/scattergather"
)

type (
	Task struct {
		Name                 string
		Resources            Resources
		Endpoints            []Endpoint
		RequirementsTemplate string
		InitFn               func(res AllocResources) TaskHandle
	}
	TaskHandle interface {
		Setup() (func() error, error) // Use Setup for setting things such as mock backend servers
		Prompt() (string, error)      // This is the initial prompt we give the coding harness to start working on
		// Use this to provide feedback to harness. Every time harness indicate completion last message we call
		// this method and if it is non empty we feed it back to harness
		Feedback() (string, error)
		// When we no longer get feedback we call validation. Use this to run the hidden test set. This is always a pass or fail
		Validation() (bool, error)
	}
	// Resources represents static resources that should be allocated by the benchmark for the given task
	Resources struct {
		NPorts int
	}
	// AllocResources represents static resources that are already allocated by the harness
	AllocResources struct {
		Ports []int
	}
)

// NewClaimCheck initializes the Claim Check task.
func NewClaimCheck() Task {
	return Task{
		Name:      "claim-check",
		Resources: Resources{NPorts: 5},
		Endpoints: []Endpoint{
			{Method: "GET", Path: "/health", PortIndex: 0},
			{Method: "POST", Path: "/messages", PortIndex: 0},
			{Method: "GET", Path: "/health", PortIndex: 1},
			{Method: "POST", Path: "/messages", PortIndex: 1},
		},
		RequirementsTemplate: claimcheck.PromptTemplate(),
		InitFn: func(resources AllocResources) TaskHandle {
			return claimcheck.New(resources.Ports)
		},
	}
}

// NewContentBasedRouter initializes the Content-Based Router task.
func NewContentBasedRouter() Task {
	return Task{
		Name:                 "content-based-router",
		Resources:            Resources{NPorts: 3},
		Endpoints:            []Endpoint{{Method: "GET", Path: "/health"}, {Method: "POST", Path: "/messages"}},
		RequirementsTemplate: contentbasedrouting.PromptTemplate(),
		InitFn: func(resources AllocResources) TaskHandle {
			return contentbasedrouting.New(resources.Ports)
		},
	}
}

// NewContentEnricher initializes the Content Enricher task.
func NewContentEnricher() Task {
	return Task{
		Name:                 "content-enricher",
		Resources:            Resources{NPorts: 3},
		Endpoints:            []Endpoint{{Method: "GET", Path: "/health"}, {Method: "POST", Path: "/messages"}},
		RequirementsTemplate: contentenricher.PromptTemplate(),
		InitFn: func(resources AllocResources) TaskHandle {
			return contentenricher.New(resources.Ports)
		},
	}
}

// NewDeadLetterChannel initializes the Dead Letter Channel task.
func NewDeadLetterChannel() Task {
	return Task{
		Name:                 "dead-letter-channel",
		Resources:            Resources{NPorts: 2},
		Endpoints:            []Endpoint{{Method: "GET", Path: "/health"}},
		RequirementsTemplate: deadletterchannel.PromptTemplate(),
		InitFn: func(resources AllocResources) TaskHandle {
			return deadletterchannel.New(resources.Ports)
		},
	}
}

// NewScatterGather initializes the Scatter-Gather task.
func NewScatterGather() Task {
	return Task{
		Name:                 "scatter-gather",
		Resources:            Resources{NPorts: 4},
		Endpoints:            []Endpoint{{Method: "GET", Path: "/health"}, {Method: "POST", Path: "/quotes"}},
		RequirementsTemplate: scattergather.PromptTemplate(),
		InitFn: func(resources AllocResources) TaskHandle {
			return scattergather.New(resources.Ports)
		},
	}
}

func (a AllocResources) Cleanup() {
	// TODO: implement this
}
