package ddb

import (
	log "github.com/Sirupsen/logrus"
	"github.com/sir-wiggles/bcfs/backend"
)

var (
	PACKAGE_NAME = "neo"
)

type Driver struct{}

func init() {
	log.Infof("Registering %s as a backend", PACKAGE_NAME)
	backend.RegisterBackend(PACKAGE_NAME, &Driver{})
}

func (d *Driver) Ping() string {
	return "From " + PACKAGE_NAME
}

// Given a list of node ids return all the nodes and their properties
func (d *Driver) GetNodes(sid string, nids []string) (*backend.Nodes, error) {
	return nil, nil
}
