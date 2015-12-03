package ddb

import (
	"log"

	"github.com/sir-wiggles/bcfs/backend"
)

var (
	PACKAGE_NAME = "ddb"
)

func init() {
	log.Printf("Registering %s as a backend", PACKAGE_NAME)
	backend.RegisterBackend(PACKAGE_NAME, newDriver)
}

type Driver struct{}

func newDriver(c *backend.Config) (backend.Graph, error) {
	return &Driver{}, nil
}

func (d *Driver) Ping() string {
	return "From " + PACKAGE_NAME
}

// Given a list of node ids return all the nodes and their properties
func (d *Driver) GetNodes(*backend.Nodes) (*backend.Nodes, error) {
	return nil, nil
}
