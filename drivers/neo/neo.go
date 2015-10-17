package neo

import (
	"bytes"
	"fmt"
	"strconv"

	log "github.com/Sirupsen/logrus"
	"github.com/jmcvetta/neoism"
	"github.com/sir-wiggles/bcfs/backend"
)

// Constants for the package
var (
	PackageName = "neo"
)

// will register this package as a know backend
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
		if len(*r) == 0 {
			continue
		}
		resp := (*r)[0].Data
		if resp == nil {
			continue
		}
		bn[resp["nid"].(string)] = resp
	}

	return &bn, nil
}

// CreateNodes will create a node in the graph and return the newly created node
// If the node already exists, then the existing node will remain unchanged
func (d *Driver) CreateNodes(nodes *backend.Nodes) (*backend.Nodes, error) {

	statements := make([]*neoism.CypherQuery, 0, len(*nodes))
	responses := make([]*[]neoResponse, 0, len(*nodes))
	createQuery := `MERGE (n:` + "`%s`" + ` {nid:'%s'})
	ON CREATE SET n.__created__ = true, %s
	WITH n, n.__created__ as created
	REMOVE n.__created__
	RETURN n, created;`
	for nid, properties := range *nodes {
		buffer := bytes.NewBufferString("")
		for k, v := range properties {
			// nid is the index and should never be altered
			if k == "nid" {
				nid = v.(string)
				continue
			}
			switch v.(type) {
			case int, int32, int64, float32, float64:
				buffer.WriteString(fmt.Sprintf("n.%s=%d", k, v))
			case string:
				buffer.WriteString(fmt.Sprintf("n.%s='%s'", k, v))
			}
			buffer.WriteString(",")
		}
		buffer.Truncate(buffer.Len() - 1)
		buffer.WriteString("")

		// Each statment will get a res struct to house the returned node from Neo
		r := &[]neoResponse{}
		q := &neoism.CypherQuery{
			// we need the back ticks for the label because some may start with a number
			// and cypher requires that we back tick those.
			Statement: fmt.Sprintf(createQuery, d.sid, nid, buffer.String()),
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

// AlterNodes will update the specified node with the parameters given.  If
// no node was found then nothing will happen and a null node will be returned.
func (d *Driver) AlterNodes(nodes *backend.Nodes) (*backend.Nodes, error) {
	statements := make([]*neoism.CypherQuery, 0, len(*nodes))
	responses := make([]*[]neoResponse, 0, len(*nodes))
	createQuery := `MATCH (n:` + "`%s`" + ` {nid:'%s'})
	SET %s
	RETURN n;`
	for nid, properties := range *nodes {
		buffer := bytes.NewBufferString("")
		for k, v := range properties {
			if k == "nid" {
				nid = v.(string)
				continue
			}
			switch v.(type) {
			case int, int32, int64, float32, float64:
				buffer.WriteString(fmt.Sprintf("n.%s=%d", k, v))
			case string:
				buffer.WriteString(fmt.Sprintf("n.%s='%s'", k, v))
			}
			buffer.WriteString(",")
		}
		buffer.Truncate(buffer.Len() - 1)

		// Each statment will get a res struct to house the returned node from Neo
		r := &[]neoResponse{}
		q := &neoism.CypherQuery{
			// we need the back ticks for the label because some may start with a number
			// and cypher requires that we back tick those.
			Statement: fmt.Sprintf(createQuery, d.sid, nid, buffer.String()),
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

// DeleteNodes will delete the given nodes from the graph. All relationships
// must be deleted before nodes can be deleted.
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

// GetInEdges returns all edges that are pointing to a nid
func (d *Driver) GetInEdges(edges *backend.Edges) (*backend.Edges, error) {

	statements := make([]*neoism.CypherQuery, 0, len(*edges))
	responses := make([]*[]neoResponse, 0, len(*edges))

	for nid := range *edges {

		r := &[]neoResponse{}
		q := &neoism.CypherQuery{
			Statement: fmt.Sprintf("MATCH ()-[n:ROOT*]->(m:`%s` {nid:'%s'}) RETURN n;", d.sid, nid),
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
	be := make(backend.Edges, len(*edges))
	for i, r := range responses {
		resp := (*r)[0].Data
		if resp == nil {
			continue
		}
		temp := strconv.Itoa(i)
		be[temp] = resp
	}
	return &be, nil
}

// GetOutEdges returns all edges that are originating from a nid
func (d *Driver) GetOutEdges(edges *backend.Edges) (*backend.Edges, error) {
	return nil, nil
}

// GetSingleEdge returns one edge that is between two nids
func (d *Driver) GetSingleEdge(edges *backend.Edges) (*backend.Edges, error) {
	return nil, nil
}

// CreateEdges creates edges with properties
func (d *Driver) CreateEdges(edges *backend.Edges) (*backend.Edges, error) {
	return nil, nil
}

// AlterEdges changes properties on edges with the given properties
func (d *Driver) AlterEdges(edges *backend.Edges) (*backend.Edges, error) {
	return nil, nil
}

// DeleteEdges removes edges from the graph
func (d *Driver) DeleteEdges(edges *backend.Edges) error {
	return nil
}

// GetPath returns a path with it's edeges given a series of nids
func (d *Driver) GetPath(nodes *backend.Nodes) (*backend.Path, error) {
	return nil, nil
}

// GetConnection gets a new connection to the db
func (d *Driver) GetConnection() (*backend.Connection, error) {
	return nil, nil
}

// Ping tests the connection to the database
func (d *Driver) Ping() error {
	return nil
}
