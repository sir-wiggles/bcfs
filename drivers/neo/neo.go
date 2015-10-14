package neo

import (
	"bytes"
	"fmt"

	log "github.com/Sirupsen/logrus"
	"github.com/jmcvetta/neoism"
	"github.com/sir-wiggles/bcfs/backend"
)

// Constants for the package
var (
	PackageName = "neo"
)

// will register this package as a knows backend
func init() {
	log.Infof("Registering %s as a backend", PackageName)
	backend.RegisterBackend(PackageName, newDriver)
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

type neoResponse struct {
	Data    map[string]interface{} `json:"n"`
	Created bool                   `json:"created"`
}

// GetNodes returns nodes from the backend storage given their IDs
func (d *Driver) GetNodes(nodes *backend.Nodes) (*backend.Nodes, error) {

	statements := make([]*neoism.CypherQuery, 0, len(*nodes))
	responses := make([]*[]neoResponse, 0, len(*nodes))

	for nid := range *nodes {
		// Each statment will get a res struct to house the returned node from Neo
		r := &[]neoResponse{}
		q := &neoism.CypherQuery{
			// we need the back ticks for the label because some may start with a number
			// and cypher requires that we back tick those.
			Statement: fmt.Sprintf("MATCH (n:`%s` {nid:'%s'}) RETURN n;", d.sid, nid),
			Result:    r,
		}

		statements = append(statements, q)
		responses = append(responses, r)

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
		resp := (*r)[0].Data
		if resp == nil {
			continue
		}
		bn[resp["nid"].(string)] = resp
	}

	return &bn, nil
}

func (d *Driver) GetEdges(edges *backend.Edges) (*backend.Edges, error) {
	return nil, nil
}

func (d *Driver) CreateNodes(nodes *backend.Nodes) (*backend.Nodes, error) {

	statements := make([]*neoism.CypherQuery, 0, len(*nodes))
	responses := make([]*[]neoResponse, 0, len(*nodes))
	createQuery := `MERGE (n:` + "`%s`" + ` %s)
	ON CREATE SET n.__created__ = true
	WITH n, n.__created__ as created
	REMOVE n.__created__
	RETURN n, created;`
	for _, properties := range *nodes {

		buffer := bytes.NewBufferString("{")
		for k, v := range properties {
			switch v.(type) {
			case int:
				buffer.WriteString(fmt.Sprintf("%s:%d", k, v))
			case string:
				buffer.WriteString(fmt.Sprintf("%s:'%s'", k, v))
			}
			buffer.WriteString(",")
		}
		buffer.Truncate(buffer.Len() - 1)
		buffer.WriteString("}")

		// Each statment will get a res struct to house the returned node from Neo
		r := &[]neoResponse{}
		q := &neoism.CypherQuery{
			// we need the back ticks for the label because some may start with a number
			// and cypher requires that we back tick those.
			Statement: fmt.Sprintf(createQuery, d.sid, buffer.String()),
			Result:    r,
		}

		statements = append(statements, q)
		responses = append(responses, r)
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
		resp := (*r)[0].Data
		if resp == nil {
			continue
		}
		bn[resp["nid"].(string)] = resp
	}

	return &bn, nil
}

func (d *Driver) CreateEdges(edges *backend.Edges) error {
	return nil
}

func (d *Driver) AlterNodes(nodes *backend.Nodes) (backend.Nodes, error) {
	statements := make([]*neoism.CypherQuery, 0, len(*nodes))
	responses := make([]*[]neoResponse, 0, len(*nodes))
	createQuery := `MATCH (n:` + "`%s`" + ` %s)
	SET %s
	RETURN n, created;`
	for _, properties := range *nodes {

		buffer := bytes.NewBufferString("")
		for k, v := range properties {
			switch v.(type) {
			case int:
				buffer.WriteString(fmt.Sprintf("n.%s=%d,", k, v))
			case string:
				buffer.WriteString(fmt.Sprintf("n.%s='%s',", k, v))
			}
			buffer.WriteString(",")
		}
		buffer.Truncate(buffer.Len() - 1)

		// Each statment will get a res struct to house the returned node from Neo
		r := &[]neoResponse{}
		q := &neoism.CypherQuery{
			// we need the back ticks for the label because some may start with a number
			// and cypher requires that we back tick those.
			Statement: fmt.Sprintf(createQuery, d.sid, buffer.String()),
			Result:    r,
		}

		statements = append(statements, q)
		responses = append(responses, r)
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
		resp := (*r)[0].Data
		if resp == nil {
			continue
		}
		bn[resp["nid"].(string)] = resp
	}

	return &bn, nil
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
