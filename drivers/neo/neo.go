package neo

import (
	"program/backend"

	log "github.com/Sirupsen/logrus"
	"database/sql"
)

type Driver struct{
	Connection *sql.DB
}

func NewDriver(conn *sql.DB) *Driver {
	return &Driver{
		Connection: conn,
	}
}

func init() {
	log.Infof("Registering %s as a backend", PACKAGE_NAME)
	backend.RegisterBackend(PACKAGE_NAME, &Driver{})
}

func (d *Driver) Ping() error {
	err := d.Connection.Ping()
	return err
}

// Given a list of node ids return all the nodes and their properties
func (d *Driver) GetNodes(sid string, nids []string) (*backend.Nodes, error) {
	return nil, nil
}
