package backend

import (
	"errors"
	"fmt"
)

// This is the interface that will interact with the actual backend.
type Graph interface {
	// Given a list of node ids and a source id, return all the nodes requested or error
	// This is useful for getting all nodes under a parent.
	GetNodes(sid string, nids []string) (*Nodes, error)

	//	// Return all the edges between sets of nodes
	//	GetEdges(sid string, nids []string) (Edge, error)
	//
	//	// Gets a connection for the connection pool
	//	GetConnection() (Connection, error)
	//
	//	// Creates nodes
	//	PutNodes() error
	//	PutEdges() error
	//
	//	DeleteNodes() error
	//	DeleteEdges() error
	//
	//	GetPath(sid string, nids []string) (Path, error)

	Ping() error
}

var registry = make(map[string]func(*Config) (Graph, error))

// Adds a driver to the registered backedns.  This driver is not useable until it is pulled with GetBackend
func RegisterBackend(name string, i func(*Config) (Graph, error)) {
	registry[name] = i
}

// Pulls a driver from the registered drivers and initializes it with the config information from the config.
func GetBackend(cfg *Config) (Graph, error) {
	// pull the driver out of the registered backends
	factory, ok := registry[(*cfg)["name"].(string)]
	if !ok {
		return nil, errors.New(
			fmt.Sprintf("A backend with the name \"%s\" has not been registered", (*cfg)["name"].(string)),
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
