package neo

import (
	"testing"
	"bcfs/backend"
	log "github.com/Sirupsen/logrus"

	"github.com/jmcvetta/neoism"
)

var url string = "http://neo4j:asdf@localhost:7474/"

func init(){
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
	for nid, i := range []string{"abc", "123", "bar", "foo"} {
		stmt := &neoism.CypherQuery{
			Statement: "MERGE (:`test-source-id`:Node {nid:{nid}, date_created:{dc}})",
			Parameters: neoism.Props{
				"nid": nid,
				"dc": i,
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
		sid: "test-source-id",
	}

	nodes_to_get := backend.Nodes{
		"abc": nil,
		"123": nil,
		"bar": nil,
		"foo": nil,
	}

	nodes, err := driver.GetNodes(&nodes_to_get)
	if err != nil {
		t.Errorf(err.Error())
	}
	t.Log(nodes)

}