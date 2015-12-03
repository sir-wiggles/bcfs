package ddb

import (
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
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

	// Edge table parameters
	EDGE_TABLE_NAME  = aws.String("fs-edge")
	EDGE_HASH        = aws.String("sid_from")
	EDGE_RANGE       = aws.String("sid_to")
	EDGE_ATTR_NAME   = aws.String("name")
	EDGE_LSI_NAME    = aws.String("name-index")
	EDGE_GSI_REVERSE = aws.String("sid_to-sid_from-index")

	// Node table parameters
	NODE_TABLE_NAME     = aws.String("fs-node")
	NODE_HASH           = aws.String("sid_nid")
	NODE_RANGE          = aws.String("nid")
	NODE_ATTR_BLOCKLIST = aws.String("blocklist_id")
	NODE_GSI_BLOCKLIST  = aws.String("sid_nid-blocklist_id-index")
)

func Test_GetNodes(t *testing.T) {

	env := setup(t)
	sid := "__sid__"

	type testCase struct {
		nodesToAdd [][]string
		input      *backend.Nodes
		output     *backend.Nodes
		driver     *Driver
		err        error
	}

	tests := map[string]*testCase{
		"get one node": &testCase{
			nodesToAdd: [][]string{
				{sid, "1"}, {sid, "2"}, {sid, "3"},
				{sid, "4"}, {sid, "5"}, {sid, "6"},
				{sid, "7"}, {sid, "8"}, {sid, "9"},
				{sid, "0"},
			},
			input: &backend.Nodes{
				SOURCE_ID: &backend.Properties{
					"nid": sid,
				},
				"1": &backend.Properties{
					"nid": "1",
				},
			},
			output: &backend.Nodes{
				"1": &backend.Properties{
					"sid":    sid,
					"nid":    "1",
					"string": "test",
					"number": "0",
					"bool":   "true",
				},
			},
			driver: &Driver{
				Connection:    env.db,
				NodeTableName: *NODE_TABLE_NAME,
			},
			err: nil,
		},
	}

	for testDescription, testCase := range tests {
		env.addNodesToDB(testCase.nodesToAdd)

		resp, err := testCase.driver.GetNodes(testCase.input)
		if err != testCase.err {
			t.Errorf("%s error missmatch: %s", testDescription, err.Error())
			continue
		}
		if reflect.DeepEqual(resp, testCase.output) {
			t.Errorf("%s expected\n%s\ngot\n%s", testCase.output, resp)
		}
	}
}
