package neo

import (
	"testing"

	log "github.com/Sirupsen/logrus"
	"github.com/jmcvetta/neoism"
	"github.com/sir-wiggles/bcfs/backend"
)

var url = "http://neo4j:asdf@localhost:7474/"

func init() {
	log.SetLevel(log.DebugLevel)
}

func addNodesToGraph(db *neoism.Database) error {
	// A unique constraint on a node's nid

	constraint := &neoism.CypherQuery{
		Statement: "CREATE CONSTRAINT ON (node:`test-source-id`) ASSERT node.nid IS UNIQUE",
	}
	err := db.Cypher(constraint)
	constraint = &neoism.CypherQuery{
		Statement: "CREATE CONSTRAINT ON (node:Node) ASSERT node.nid IS UNIQUE",
	}
	err = db.Cypher(constraint)
	if err != nil {
		log.Debug(err.Error())
		return err
	}

	delr := &neoism.CypherQuery{
		Statement: "MATCH ()-[r]-() DELETE r;",
	}

	deln := &neoism.CypherQuery{
		Statement: "MATCH (n) DELETE n;",
	}

	fs := &neoism.CypherQuery{
		Statement: `CREATE
			(root:test-source-id {nid: 'root'}),
			(d1:test-source-id {nid: 'd1'}),
			(d2:test-source-id {nid: 'd2'}),
			(d3:test-source-id {nid: 'd3'}),
			(f01:test-source-id {nid:'f0.1'}),
			(f11:test-source-id {nid:'f1.1'}),
			(f21:test-source-id {nid:'f2.1'}),
			root-[:ROOT]->f01,
			root-[:ROOT]->d1,
			root-[:ROOT]->d2,
			d1-[:ROOT]->f11,
			d2-[:ROOT]->f21,
			d2-[:ROOT]->d3
			RETURN root;`,
	}

	cleanup := make([]*neoism.CypherQuery, 0, 3)
	cleanup = append(cleanup, delr)
	cleanup = append(cleanup, deln)
	cleanup = append(cleanup, fs)
	tx, err := db.Begin(cleanup)
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

func Test_CreateNodes(t *testing.T) {

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

	oneNode := backend.Nodes{
		"a": backend.Properties{
			"nid":          "a",
			"date_created": 1,
		},
	}

	twoNode := backend.Nodes{
		"b": backend.Properties{
			"nid":          "b",
			"date_created": 2,
		},
		"c": backend.Properties{
			"nid":          "c",
			"date_created": 3,
		},
	}

	oneManyNode := backend.Nodes{
		"d": backend.Properties{
			"nid":          "d",
			"date_created": 4,
			"prop1":        "p1",
			"prop2":        1000,
			"prop3":        "p3",
		},
	}

	existingNode := backend.Nodes{
		"a": backend.Properties{
			"nid":          "a",
			"date_created": 1,
		},
	}

	resp, err := driver.CreateNodes(&oneNode)
	if err != nil {
		t.Errorf("Failed to create oneNode %s", err.Error())
	}
	if len(*resp) != 1 {
		t.Errorf("Expected %d got %d nodes", len(oneNode), len(*resp))
	}

	resp, err = driver.CreateNodes(&twoNode)
	if len(*resp) != 2 {
		t.Errorf("Expected %d got %d nodes", len(twoNode), len(*resp))
	}

	resp, err = driver.CreateNodes(&oneManyNode)
	if len(*resp) != 1 {
		t.Errorf("Expected %d got %d nodes", len(oneManyNode), len(*resp))
	}
	if len((*resp)["d"]) != 5 {
		t.Errorf("Expected %d got %d properties", len(oneManyNode["d"]), len((*resp)["d"]))
	}

	_ = "breakpoint"
	resp, err = driver.CreateNodes(&existingNode)
	if err != nil {
		t.Errorf("%s", err.Error())
	}
}
