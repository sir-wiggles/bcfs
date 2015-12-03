package ddb

import (
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/sir-wiggles/bcfs/backend"
)

var (
	PACKAGE_NAME = "ddb"

	SOURCE_ID = "source_id"
	NODE_ID   = "nid"
)

func init() {
	log.Printf("Registering %s as a backend", PACKAGE_NAME)
	backend.RegisterBackend(PACKAGE_NAME, newDriver)
}

type Driver struct {
	Connection    *dynamodb.DynamoDB
	NodeTableName string
}

func newDriver(c *backend.Config) (backend.Graph, error) {
	return &Driver{}, nil
}

func (d *Driver) Ping() string {
	return "From " + PACKAGE_NAME
}

// Given a list of node ids return all the nodes and their properties
func (d *Driver) GetNodes(nodes *backend.Nodes) (*backend.Nodes, error) {

	keys := make([]map[string]*dynamodb.AttributeValue, 0, len(*nodes))
	sn, ok := (*nodes)[SOURCE_ID]
	if !ok {
		return nil, fmt.Errorf("driver: missing source id in request")
	}
	sid, err := sn.StringKey(NODE_ID)
	if err != nil {
		return nil, fmt.Errorf("driver: %s", err.Error())
	}

	for nid, _ := range *nodes {
		if nid == SOURCE_ID {
			continue
		}
		hash := aws.String(fmt.Sprintf("%s:%s", sid, nid))
		key := map[string]*dynamodb.AttributeValue{
			*NODE_HASH:  &dynamodb.AttributeValue{S: hash},
			*NODE_RANGE: &dynamodb.AttributeValue{S: aws.String(nid)},
		}
		keys = append(keys, key)
	}
	fmt.Println(keys)

	items, err := d.batchGet(keys)
	if err != nil {

	}
	fmt.Println(items)

	return nil, nil
}

func (d *Driver) batchGet(keys []map[string]*dynamodb.AttributeValue) ([]map[string]*dynamodb.AttributeValue, error) {

	items := make([]map[string]*dynamodb.AttributeValue, 0, len(keys))
	for {

		resp, err := d.Connection.BatchGetItem(&dynamodb.BatchGetItemInput{
			RequestItems: map[string]*dynamodb.KeysAndAttributes{
				d.NodeTableName: &dynamodb.KeysAndAttributes{
					Keys: keys,
				},
			},
		})
		fmt.Println(resp)
		if err != nil {
			log.Printf("batchGet: %s", err.Error())
			continue
		}
		if _, ok := resp.UnprocessedKeys[d.NodeTableName]; !ok {
			break
		}
		keys = resp.UnprocessedKeys[d.NodeTableName].Keys
	}
	return items, nil
}
