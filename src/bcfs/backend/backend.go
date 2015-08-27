package backend

import (
	"errors"
	"fmt"
)

// This is the interface that will interact with the actual backend.
type Graph interface {
	GetNodes(sid string, nids []string) (*Nodes, error)
	GetEdges(sid string, nids []string) (*Edges, error)

	GetConnection() (*Connection, error)

	PutNodes() error
	PutEdges() error

	PostNodes() error
	PostEdges() error

	DeleteNodes() error
	DeleteEdges() error

	GetPath(sid string, nids []string) (*Path, error)

	Ping() error
}

// all drivers will have this type of function that will be registered to be used in creating a new driver
type DriverInitializer func(*Config) (Graph, error)

var registry = make(map[string]DriverInitializer)

// Adds a driver to the registered backedns.  This driver is not useable until it is pulled with GetBackend
func RegisterBackend(name string, i DriverInitializer) {
	registry[name] = i
}

// Pulls a driver from the registered drivers and initializes it with the config information from the config.
func GetBackend(cfg *Config) (Graph, error) {
	// pull the driver out of the registered backends
	factory, ok := registry[cfg.StringKey("name")]
	if !ok {
		return nil, errors.New(
			fmt.Sprintf("A backend with the name \"%s\" has not been registered", cfg.StringKey("name")),
		)
	}

	// setup the driver with all the connections it will need to be useful.
	graph, err := factory(cfg)
	if err != nil {
		return nil, errors.New(
			fmt.Sprintf("Failed to initialize with error message %s", err.Error()),
		)
	}

	return graph, err
}
