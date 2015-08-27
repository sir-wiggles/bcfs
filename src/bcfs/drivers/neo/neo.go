package neo

import (
	"bcfs/backend"
	"database/sql"
	"fmt"
	log "github.com/Sirupsen/logrus"
	_ "gopkg.in/cq.v1"
)

var (
    PACKAGE_NAME = "neo"
)

// will register this package as a knows backend
func init() {
	log.Infof("Registering %s as a backend", PACKAGE_NAME)
	backend.RegisterBackend(PACKAGE_NAME, newDriver)
}

type Driver struct {
	Connection *sql.DB
}

// creates a new driver with the unique set of config options specified in the config file
func newDriver(c *backend.Config) (backend.Graph, error) {

	url := fmt.Sprintf(
		"http://%s:%s@%s:%d",
		c.StringKey("user"),
		c.StringKey("password"),
		c.StringKey("host"),
		c.IntKey("port"),
	)

	db, err := sql.Open("neo4j-cypher", url)
	return &Driver{
		Connection: db,
	}, err
}

// tests the DB connection
func (d *Driver) Ping() error {
	return d.Connection.Ping()
}

func (d *Driver) GetConnection() (*backend.Connection, error) {
	return nil, nil
}

// Given a list of node ids return all the nodes and their properties
func (d *Driver) GetNodes(sid string, nids []string) (*backend.Nodes, error) {
	return nil, nil
}

func (d *Driver) GetEdges(sid string, nids []string) (*backend.Edges, error) {
	return nil, nil
}

func (d *Driver) PutNodes() error {
	return nil
}

func (d *Driver) PutEdges() error {
	return nil
}

func (d *Driver) PostNodes() error {
	return nil
}

func (d *Driver) PostEdges() error {
	return nil
}

func (d *Driver) DeleteNodes() error {
	return nil
}

func (d *Driver) DeleteEdges() error {
	return nil
}

func (d *Driver) GetPath(sid string, nids []string) (*backend.Path, error) {
	return nil, nil
}
