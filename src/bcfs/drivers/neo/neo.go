package neo

import (
	"bcfs/backend"
	"database/sql"
	"fmt"
	log "github.com/Sirupsen/logrus"
	_ "gopkg.in/cq.v1"
	"strings"
)

// Constants for the package
var (
	PACKAGE_NAME = "neo"
)

// will register this package as a knows backend
func init() {
	log.Infof("Registering %s as a backend", PACKAGE_NAME)
	backend.RegisterBackend(PACKAGE_NAME, newDriver)
}

type Driver struct {
	Connection  *sql.DB
	Transaction *sql.Tx
	sid         string
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

// -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
// Given a list of node ids return all the nodes and their properties
func (d *Driver) GetNodes(nodes *backend.Nodes) (*backend.Nodes, error) {

	nids := make([]string, len(nodes))
	i := 0
	for k, _ := range nodes {
		nids[i] = fmt.Sprintf("(n_%d:`%s` {nid: '%s'})", i, d.sid, k)
		i += 1
	}
	pattern := strings.Join(nids, ", ")
	statement := fmt.Sprintf("MATCH %s RETURN *;", pattern)

	stmt, err := d.Transaction.Prepare(statement)
	if err != nil {
		return nil, err
	}

	rows, err := stmt.Query()
	if err != nil {
		return nil, err
	}

	holster := make([]interface{}, len(nids))
	i = 0
	for _, v := range nodes {
		holster[i] = &v
		i += 1
	}

	for rows.Next() {
		err := rows.Scan(holster...)
		if err != nil {
			log.Error(err.Error())
			return nil, err
		}
	}
	return nil, nil
}

func (d *Driver) GetEdges(edges *backend.Edges) (*backend.Edges, error) {
	return nil, nil
}

// -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
func (d *Driver) CreateNodes(nodes *backend.Nodes) error {
	return nil
}

func (d *Driver) CreateEdges(edges *backend.Edges) error {
	return nil
}

// -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
func (d *Driver) AlterNodes(nodes *backend.Nodes) error {
	return nil
}

func (d *Driver) AlterEdges(edges *backend.Edges) error {
	return nil
}

// -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
func (d *Driver) DeleteNodes(nodes *backend.Nodes) error {
	return nil
}

func (d *Driver) DeleteEdges(edges *backend.Edges) error {
	return nil
}

// -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
func (d *Driver) GetPath(nodes *backend.Nodes) (*backend.Path, error) {
	return nil, nil
}

func (d *Driver) GetConnection() (*backend.Connection, error) {
	return nil, nil
}

// Test the connection to the DB. Returns error if failed to communicate. Usually due to a connection error.
func (d *Driver) Ping() error {
	return d.Connection.Ping()
}
