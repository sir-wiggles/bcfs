package neo

import (
	"testing"

	log "github.com/Sirupsen/logrus"
	"github.com/jmcvetta/neoism"
	"github.com/sir-wiggles/bcfs/backend"
)

var url string = "http://neo4j:asdf@localhost:7474/"

func init() {
	log.SetLevel(log.DebugLevel)
}

func addNodesToGraph(db *neoism.Database) error {
	// A unique constraint on a node's nid

	constraint := &neoism.CypherQuery{
		Statement: "CREATE CONSTRAINT ON (node:Node) ASSERT node.nid IS UNIQUE",
	}
	err := db.Cypher(constraint)
	if err != nil {
		log.Debug(err.Error())
		return err
	}

	statements := make([]*neoism.CypherQuery, 0, 4)
	for i, nid := range []string{"abc", "123", "bar", "foo"} {
		stmt := &neoism.CypherQuery{
			Statement: "MERGE (:`test-source-id`:Node {nid:{nid}, date_created:{dc}})",
			Parameters: neoism.Props{
				"nid": nid,
				"dc":  i,
			},
		}
		statements = append(statements, stmt)
	}

	tx, err := db.Begin(statements)
	if err != nil {
		log.Debug(err.Error())
		return err
	}

	err = tx.Commit()
	if err != nil {
		log.Debug(err.Error())
		return err
	}
	return nil
}

func Test_GetNodes(t *testing.T) {

	db, err := neoism.Connect(url)
	if err != nil {
		t.Error("here", err.Error())
	}
	err = addNodesToGraph(db)
	if err != nil {
		t.Errorf(err.Error())
	}
	driver := Driver{
		Connection: db,
		sid:        "test-source-id",
	}

	nodesToGet := backend.Nodes{
		"abc": nil,
		"123": nil,
		"bar": nil,
		"foo": nil,
	}

	nodes, err := driver.GetNodes(&nodesToGet)
	if err != nil {
		t.Errorf(err.Error())
	}

	t.Logf("%#v", nodes)
	if len(*nodes) != len(nodesToGet) {
		t.Errorf("Expected %d nodes got %d", len(nodesToGet), len(*nodes))
		t.Fail()
	}
}

func Test_DeleteNodes(t *testing.T) {

	db, err := neoism.Connect(url)
	if err != nil {
		t.Error(err.Error())
	}

	err = addNodesToGraph(db)
	if err != nil {
		t.Errorf(err.Error())
	}
	driver := Driver{
		Connection: db,
		sid:        "test-source-id",
	}

	nodesToDel := backend.Nodes{
		"abc": nil,
		"123": nil,
		"bar": nil,
		"foo": nil,
	}

	err = driver.DeleteNodes(&nodesToDel)
	if err != nil {
		t.Error(err.Error())
	}
}
