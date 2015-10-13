package neo

import (
	"bcfs/backend"
	"fmt"
	log "github.com/Sirupsen/logrus"
//	"strings"

	"github.com/jmcvetta/neoism"
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
	Connection  *neoism.Database
	Transaction *neoism.Tx
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

//	db, err := sql.Open("neo4j-cypher", url)
	db, err := neoism.Connect(url)
	return &Driver{
		Connection: db,
	}, err
}

// Given a map of node ids return all the nodes and their properties
func (d *Driver) GetNodes(nodes *backend.Nodes) (*backend.Nodes, error) {

	// unpackage the nodes extracting their ids
	stmts := make([]*neoism.CypherQuery, len(*nodes))
	for k, _ := range *nodes {
		res := []struct {
			nodes map[string]interface{} `json:"n"`
		}{}

		cq := neoism.CypherQuery{
			Statement: fmt.Sprintf("MATCH (n:`%s` {nid: '%s'}) RETURN n", d.sid, k),
			Result: &res,
		}
		stmts = append(stmts, &cq)
	}

	tx, err := d.Connection.Begin(stmts)
	if err != nil {
		log.Debug("1", err.Error())
		return nil, err
	}

	log.Debug(stmts)

	err = tx.Commit()
	if err != nil {
		log.Debug(err.Error())
		return nil, err
	}
	log.Debug(stmts)
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
	return nil
}
