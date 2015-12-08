package backend

import "fmt"

// Graph is the interface that all drivers must implement
type Graph interface {
	// Gets
	GetNodes(*Nodes) error
	GetInEdges(*Edges) error
	GetOutEdges(*Edges) error

	// Creates
	CreateNodes(*Nodes) error
	CreateEdges(*Edges) error

	// Alters
	AlterNodes(*Nodes) error
	//	AlterEdges(*Edges) error
	//
	//	DeleteNodes(*Nodes) error
	//	DeleteEdges(*Edges) error
	//
	//	GetPath(*Nodes) (*Path, error)
	//
	//	GetConnection() (*Connection, error)
	//	Ping() error
}

// DriverInitializer is a signature that all drivers must have to register it's backen with the FS
type DriverInitializer func(*Config) (Graph, error)

var registry = make(map[string]DriverInitializer)

// RegisterBackend is the function from a driver to register that driver with the FS
func RegisterBackend(name string, i DriverInitializer) {
	registry[name] = i
}

// GetBackend returns a driver based on a config
func GetBackend(cfg *Config) (Graph, error) {
	// pull the driver out of the registered backends
	factory, ok := registry[cfg.StringKey("name")]
	if !ok {
		return nil, fmt.Errorf(
			fmt.Sprintf("A backend with the name \"%s\" has not been registered", cfg.StringKey("name")),
		)
	}

	// setup the driver with all the connections it will need to be useful.
	graph, err := factory(cfg)
	if err != nil {
		return nil, fmt.Errorf(
			fmt.Sprintf("Failed to initialize with error message %s", err.Error()),
		)
	}

	return graph, err
}
