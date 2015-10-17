package neo

import (
	"testing"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/jmcvetta/neoism"
	"github.com/sir-wiggles/bcfs/backend"
)

var url = "http://neo4j:asdf@localhost:7474/"

func init() {
	log.SetLevel(log.DebugLevel)
}

func setup() (*neoism.Database, error) {

	db, err := neoism.Connect(url)
	if err != nil {
		return nil, err
	}
	constraint2 := &neoism.CypherQuery{
		Statement: "CREATE CONSTRAINT ON (node:Node) ASSERT node.nid IS UNIQUE",
	}

	delr := &neoism.CypherQuery{
		Statement: "MATCH ()-[r]-() DELETE r;",
	}

	deln := &neoism.CypherQuery{
		Statement: "MATCH (n) DELETE n;",
	}

	fs := &neoism.CypherQuery{
		Statement: `CREATE
			(root:Node {nid: 'root'}),
			(d1:Node {nid: 'd1'}),
			(d2:Node {nid: 'd2'}),
			(d3:Node {nid: 'd3'}),
			(f01:Node {nid:'f0.1'}),
			(f11:Node {nid:'f1.1'}),
			(f21:Node {nid:'f2.1'}),
			root-[:ROOT]->f01,
			root-[:ROOT]->d1,
			root-[:ROOT]->d2,
			d1-[:ROOT]->f11,
			d2-[:ROOT]->f21,
			d2-[:ROOT]->d3
			RETURN root;`,
	}

	err = db.Cypher(constraint2)
	err = db.Cypher(delr)
	err = db.Cypher(deln)
	err = db.Cypher(fs)
	if err != nil {
		log.Debug(err.Error())
		return nil, err
	}
	return db, nil
}

func Test_GetNodes(t *testing.T) {

	db, err := setup()
	if err != nil {
		t.Errorf(err.Error())
		t.FailNow()
	}

	tests := map[string]map[string]interface{}{
		"One node query": map[string]interface{}{
			"query": backend.Nodes{
				"f0.1": nil,
			},
			"check":  1,
			"expect": "One node query. One node should be returned.",
			"driver": Driver{
				Connection: db,
				sid:        "Node",
			},
		},
		"Two node query": map[string]interface{}{
			"query": backend.Nodes{
				"f0.1": nil,
				"f1.1": nil,
			},
			"check":  2,
			"expect": "Two node query. Two nodes should be returned.",
			"driver": Driver{
				Connection: db,
				sid:        "Node",
			},
		},
		"One non existent node query": map[string]interface{}{
			"query": backend.Nodes{
				"fnil": nil,
			},
			"check":  0,
			"expect": "One node query where node doesn't exist. Nothing should be returned.",
			"driver": Driver{
				Connection: db,
				sid:        "Node",
			},
		},
		"Two node query one non existant": map[string]interface{}{
			"query": backend.Nodes{
				"f0.1": nil,
				"fnil": nil,
			},
			"check":  1,
			"expect": "Two node query where one node doesn't exist. Should only have one node returned",
			"driver": Driver{
				Connection: db,
				sid:        "Node",
			},
		},
		"Invalid source id": map[string]interface{}{
			"query": backend.Nodes{
				"f0.1": nil,
			},
			"check":  0,
			"expect": "Invalid source id given, no nodes should be returned.",
			"driver": Driver{
				Connection: db,
				sid:        "invalid",
			},
		},
	}

	for testName, test := range tests {
		driver := test["driver"].(Driver)
		query := test["query"].(backend.Nodes)
		check := test["check"].(int)
		nodes, err := driver.GetNodes(&query)
		if err != nil {
			t.Errorf("%s %s", testName, err.Error())
			continue
		}
		if len(*nodes) != check {
			t.Errorf("%s", test["expect"].(string))
		}
	}
}

func Test_DeleteNodes(t *testing.T) {

	db, err := setup()

	res := []struct {
		Count int `json:"count"`
	}{}
	err = db.Cypher(&neoism.CypherQuery{
		Statement: "MATCH (n) RETURN count(n) as count;",
		Result:    &res,
	})

	if err != nil {
		t.Errorf("Failed to get initial count of all nodes in DB %s", err.Error())
		t.FailNow()
	}
	dbNodeCount := res[0].Count

	tests := map[string]map[string]interface{}{
		"delete one node": {
			"query": backend.Nodes{
				"d1": nil,
			},
			"delta":    -1,
			"expected": "Expected one node to be deleted.",
			"driver": Driver{
				Connection: db,
				sid:        "Node",
			},
		},
		"delete two nodes": {
			"query": backend.Nodes{
				"f0.1": nil,
				"f1.1": nil,
			},
			"delta":    -2,
			"expected": "Expected two nodes to be deleted.",
			"driver": Driver{
				Connection: db,
				sid:        "Node",
			},
		},
		"delete one node non existant": {
			"query": backend.Nodes{
				"fnil": nil,
			},
			"delta":    0,
			"expected": "Node doesn't exist, shouldn't be deleted.",
			"driver": Driver{
				Connection: db,
				sid:        "Node",
			},
		},
		"delete two nodes one non existant": {
			"query": backend.Nodes{
				"fnil": nil,
				"f2.1": nil,
			},
			"delta":    -1,
			"expected": "Should only delete one node.",
			"driver": Driver{
				Connection: db,
				sid:        "Node",
			},
		},
	}

	// with neo you can't delete a node that sill has relationships.
	err = db.Cypher(&neoism.CypherQuery{
		Statement: "MATCH ()-[r]-() DELETE r;",
		Result:    &res,
	})

	for testName, test := range tests {
		query := test["query"].(backend.Nodes)
		delta := test["delta"].(int)
		driver := test["driver"].(Driver)
		err = driver.DeleteNodes(&query)
		if err != nil {
			t.Errorf("%s %s", testName, err.Error())
			continue
		}

		res := []struct {
			Count int `json:"count"`
		}{}
		err = db.Cypher(&neoism.CypherQuery{
			Statement: "MATCH (n) RETURN count(n) as count;",
			Result:    &res,
		})
		if dbNodeCount+delta != res[0].Count {
			t.Errorf("%s", test["expected"].(string))
			t.FailNow()
		}
		dbNodeCount = res[0].Count
	}
}

func Test_CreateNodes(t *testing.T) {

	db, err := setup()
	if err != nil {
		t.Error(err.Error())
		t.FailNow()
	}

	tests := map[string]map[string]interface{}{
		"create one node": {
			"query": backend.Nodes{
				"fc.1": backend.Properties{
					"date_created": time.Now().Unix(),
				},
			},
			"expected": "Create one node with two properties",
			"driver": Driver{
				Connection: db,
				sid:        "Node",
			},
		},
		"create two nodes": {
			"query": backend.Nodes{
				"fc.1": backend.Properties{
					"date_created": time.Now().Unix(),
				},
				"fc.2": backend.Properties{
					"date_created": time.Now().Unix(),
				},
			},
			"expected": "Create two nodes with two properties each",
			"driver": Driver{
				Connection: db,
				sid:        "Node",
			},
		},
		"create existing node": {
			"query": backend.Nodes{
				"f1.1": backend.Properties{
					"date_created": time.Now().Unix(),
				},
			},
			"expected": "Create one node with two properties",
			"driver": Driver{
				Connection: db,
				sid:        "Node",
			},
		},
	}

	for testName, test := range tests {
		query := test["query"].(backend.Nodes)
		driver := test["driver"].(Driver)
		nodes, err := driver.CreateNodes(&query)
		if err != nil {
			t.Errorf("%s %s", testName, err.Error())
			continue
		}
		if len(*nodes) != len(query) {
			t.Errorf(test["expected"].(string))
		}
	}
}

func Test_AlterNodes(t *testing.T) {

	db, err := setup()
	if err != nil {
		t.Error(err.Error())
		t.FailNow()
	}

	tests := map[string]map[string]interface{}{
		"alter one node": {
			"query": backend.Nodes{
				"f0.1": backend.Properties{
					"nid":          "f0.1",
					"date_created": time.Now().Unix(),
					"type":         "file",
				},
			},
			"expected": "Create one node with two properties",
			"driver": Driver{
				Connection: db,
				sid:        "Node",
			},
		},
		"alter two nodes": {
			"query": backend.Nodes{
				"f1.1": backend.Properties{
					"date_created": time.Now().Unix(),
					"type":         "file",
				},
				"f2.1": backend.Properties{
					"date_created": time.Now().Unix(),
					"type":         "file",
				},
			},
			"expected": "Create two nodes with two properties each",
			"driver": Driver{
				Connection: db,
				sid:        "Node",
			},
		},
	}

	for testName, test := range tests {
		query := test["query"].(backend.Nodes)
		driver := test["driver"].(Driver)
		nodes, err := driver.AlterNodes(&query)
		if err != nil {
			t.Errorf("%s %s", testName, err.Error())
			continue
		}
		if len(*nodes) != len(query) {
			t.Errorf(test["expected"].(string))
		}
	}
}

func Test_GetEdges(t *testing.T) {

	db, err := setup()
	if err != nil {
		t.Errorf(err.Error())
		t.FailNow()
	}

	tests := map[string]map[string]interface{}{
		"zero edges": map[string]interface{}{
			"query": backend.Edges{
				"root": nil,
			},
			"check":  0,
			"expect": "no edges should be returned.",
			"driver": Driver{
				Connection: db,
				sid:        "Node",
			},
		},
		"one edge": map[string]interface{}{
			"query": backend.Edges{
				"d1": nil,
			},
			"check":  1,
			"expect": "One edge should be returned.",
			"driver": Driver{
				Connection: db,
				sid:        "Node",
			},
		},
	}

	for testName, test := range tests {
		driver := test["driver"].(Driver)
		query := test["query"].(backend.Edges)
		check := test["check"].(int)
		edges, err := driver.GetInEdges(&query)
		if edges == nil {
			t.Error("response should not be nil")
			continue
		}
		if err != nil {
			t.Errorf("%s %s", testName, err.Error())
			continue
		}
		if len(*edges) != check {
			t.Errorf("%s", test["expect"].(string))
		}
	}
}
