package neo

import (
	"fmt"

	log "github.com/Sirupsen/logrus"
	"github.com/jmcvetta/neoism"
	"github.com/sir-wiggles/bcfs/backend"
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

// Driver struct to house all relative information about DB connections
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

type getNodeResponse struct {
	Data map[string]interface{} `json:"n"`
}

// GetNodes returns nodes from the backend storage given their IDs
func (d *Driver) GetNodes(nodes *backend.Nodes) (*backend.Nodes, error) {

	statements := make([]*neoism.CypherQuery, 0, len(*nodes))
	responses := make([]*[]getNodeResponse, 0, len(*nodes))

	for nid := range *nodes {
		// Each statment will get a res struct to house the returned node from Neo
		r := &[]getNodeResponse{}
		q := &neoism.CypherQuery{
			// we need the back ticks for the label because some may start with a number
			// and cypher requires that we back tick those.
			Statement: fmt.Sprintf("MATCH (n:`%s` {nid:'%s'}) RETURN n;", d.sid, nid),
			Result:    r,
		}

		statements = append(statements, q)
		responses = append(responses, r)

		d.Connection.Cypher(q)
		log.Debug(q)
	}

	tx, err := d.Connection.Begin(statements)
	if err != nil {
		log.Debugf("Begin Tx error: %s", err.Error())
		return nil, err
	}

	err = tx.Commit()
	if err != nil {
		log.Debugf("Commit Tx error: %s", err.Error())
		return nil, err
	}

	// Translate the nodes into a valid backend node
	bn := make(backend.Nodes, len(*nodes))
	for _, r := range responses {
		resp := (*r)[0].Data["data"].(map[string]interface{})
		bn[resp["nid"].(string)] = resp
	}

	return &bn, nil
}

func (d *Driver) GetEdges(edges *backend.Edges) (*backend.Edges, error) {
	return nil, nil
}

func (d *Driver) CreateNodes(nodes *backend.Nodes) error {
	return nil
}

func (d *Driver) CreateEdges(edges *backend.Edges) error {
	return nil
}

func (d *Driver) AlterNodes(nodes *backend.Nodes) error {
	return nil
}

func (d *Driver) AlterEdges(edges *backend.Edges) error {
	return nil
}

func (d *Driver) DeleteNodes(nodes *backend.Nodes) error {

	statements := make([]*neoism.CypherQuery, 0, len(*nodes))

	for nid := range *nodes {

		q := &neoism.CypherQuery{
			// we need the back ticks for the label because some may start with a number
			// and cypher requires that we back tick those.
			Statement: fmt.Sprintf("MATCH (n:`%s` {nid:'%s'}) DELETE n;", d.sid, nid),
		}

		statements = append(statements, q)
		log.Debug(q)
	}

	tx, err := d.Connection.Begin(statements)
	if err != nil {
		log.Debugf("Begin Tx error: %s", err.Error())
		return err
	}
	err = tx.Commit()
	if err != nil {
		log.Debugf("Commit Tx error: %s", err.Error())
		return err
	}

	return nil
}

func (d *Driver) DeleteEdges(edges *backend.Edges) error {
	return nil
}

func (d *Driver) GetPath(nodes *backend.Nodes) (*backend.Path, error) {
	return nil, nil
}

func (d *Driver) GetConnection() (*backend.Connection, error) {
	return nil, nil
}

// Ping tests the connection to the database
func (d *Driver) Ping() error {
	return nil
}