package ddb

import (
	"encoding/hex"
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"testing/quick"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/davecgh/go-spew/spew"
	"github.com/sir-wiggles/bcfs/backend"
)

var (
	// Parameters for DDB Local
	LOCAL_ENDPOINT      = aws.String("http://localhost:8000")
	LOCAL_REGION        = aws.String("us-west-2")
	LOCAL_MAX_RETRIES   = aws.Int(1)
	LOCAL_KEY           = "key"
	LOCAL_SECRET        = "secret"
	LOCAL_SESSION_TOKEN = ""
	LOCAL_TIMEOUT       = 5
)

func newSid() string {
	var temp [3]byte
	t := reflect.TypeOf(temp)
	r := rand.New(rand.NewSource(int64(rand.Int())))
	v, _ := quick.Value(t, r)
	a := v.Interface().([3]byte)
	return hex.EncodeToString(a[:3])
}

func Test_GetNodes(t *testing.T) {

	env := setup(t)

	type testCase struct {
		nodesToAdd [][]string
		input      *backend.Nodes
		output     *backend.Nodes
		driver     *Driver
		err        error
	}

	var sid = newSid()
	var sid2 = newSid()
	var sid3 = newSid()
	var sid4 = newSid()
	var sid5 = newSid()

	nodes150 := make([][]string, 0, 150)
	input150 := &backend.Nodes{SOURCE_ID: &backend.Properties{"nid": sid3}}
	output150 := &backend.Nodes{SOURCE_ID: &backend.Properties{"nid": sid3}}
	for i := 0; i < 150; i++ {
		nid := fmt.Sprintf("%d", i+1)
		nodes150 = append(nodes150, []string{sid3, nid})
		(*input150)[nid] = &backend.Properties{}
		(*output150)[nid] = &backend.Properties{
			"sid_nid": fmt.Sprintf("%s:%s", sid3, nid),
			"nid":     nid,
			"string":  "test",
			"number":  "0",
			"bool":    "1",
		}

	}

	tests := map[string]*testCase{
		"get one node": &testCase{
			nodesToAdd: [][]string{
				{sid, "1"}, {sid, "2"}, {sid, "3"},
			},
			input: &backend.Nodes{
				SOURCE_ID: &backend.Properties{"nid": sid},
				"1":       &backend.Properties{},
			},
			output: &backend.Nodes{
				SOURCE_ID: &backend.Properties{"nid": sid},
				"1": &backend.Properties{
					"sid_nid": fmt.Sprintf("%s:%s", sid, "1"),
					"nid":     "1",
					"string":  "test",
					"number":  "0",
					"bool":    "1",
				},
			},
			driver: &Driver{
				Connection:    env.db,
				NodeTableName: *NODE_TABLE_NAME,
				SourceID:      sid,
			},
			err: nil,
		},
		"get two nodes": &testCase{
			nodesToAdd: [][]string{
				{sid2, "1"}, {sid2, "2"}, {sid2, "3"},
			},
			input: &backend.Nodes{
				SOURCE_ID: &backend.Properties{"nid": sid2},
				"1":       &backend.Properties{},
				"2":       &backend.Properties{},
			},
			output: &backend.Nodes{
				SOURCE_ID: &backend.Properties{"nid": sid2},
				"1": &backend.Properties{
					"sid_nid": fmt.Sprintf("%s:%s", sid2, "1"),
					"nid":     "1",
					"string":  "test",
					"number":  "0",
					"bool":    "1",
				},
				"2": &backend.Properties{
					"sid_nid": fmt.Sprintf("%s:%s", sid2, "2"),
					"nid":     "2",
					"string":  "test",
					"number":  "0",
					"bool":    "1",
				},
			},
			driver: &Driver{
				Connection:    env.db,
				NodeTableName: *NODE_TABLE_NAME,
				SourceID:      sid2,
			},
			err: nil,
		},
		"get with paging": &testCase{
			nodesToAdd: nodes150,
			input:      input150,
			output:     output150,
			driver: &Driver{
				Connection:    env.db,
				NodeTableName: *NODE_TABLE_NAME,
				SourceID:      sid3,
			},
			err: nil,
		},
		"get missing node": &testCase{
			nodesToAdd: [][]string{
				{sid4, "1"}, {sid4, "2"}, {sid4, "3"},
			},
			input: &backend.Nodes{
				SOURCE_ID: &backend.Properties{"nid": sid4},
				"4":       &backend.Properties{},
			},
			output: &backend.Nodes{
				SOURCE_ID: &backend.Properties{"nid": sid4},
				"4":       &backend.Properties{},
			},
			driver: &Driver{
				Connection:    env.db,
				NodeTableName: *NODE_TABLE_NAME,
				SourceID:      sid4,
			},
			err: nil,
		},
		"get mix have and missing nodes": &testCase{
			nodesToAdd: [][]string{
				{sid5, "1"}, {sid5, "2"}, {sid5, "3"},
			},
			input: &backend.Nodes{
				SOURCE_ID: &backend.Properties{"nid": sid5},
				"1":       &backend.Properties{},
				"4":       &backend.Properties{},
			},
			output: &backend.Nodes{
				SOURCE_ID: &backend.Properties{"nid": sid5},
				"1": &backend.Properties{
					"sid_nid": fmt.Sprintf("%s:%s", sid5, "1"),
					"nid":     "1",
					"string":  "test",
					"number":  "0",
					"bool":    "1",
				},
				"4": &backend.Properties{},
			},
			driver: &Driver{
				Connection:    env.db,
				NodeTableName: *NODE_TABLE_NAME,
				SourceID:      sid5,
			},
			err: nil,
		},
	}

	for testDescription, testCase := range tests {
		env.addNodesToDB(testCase.nodesToAdd)

		resp, err := testCase.driver.GetNodes(testCase.input)
		if !reflect.DeepEqual(resp, testCase.output) {
			t.Errorf("%s %s\n", testDescription, spew.Sprintf("expected\n%#+v\ngot\n%#+v", testCase.output, resp))
		}
		if err != testCase.err {
			t.Errorf("%s error missmatch: %s\n", testDescription, err.Error())
		}
	}
}
