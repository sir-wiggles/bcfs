package ddb

import (
	"fmt"
	"log"
	"reflect"

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

// Given a list of node ids return all the nodes and their properties
func (d *Driver) GetNodes(nodes *backend.Nodes) (*backend.Nodes, error) {

	sn, ok := (*nodes)[SOURCE_ID]
	if !ok {
		return nil, fmt.Errorf("driver: missing source id in request")
	}
	sid, err := sn.StringKey(NODE_ID)
	if err != nil {
		return nil, fmt.Errorf("driver: %s", err.Error())
	}

	keys := make([]map[string]*dynamodb.AttributeValue, 0, 100)
	items := make([]map[string]*dynamodb.AttributeValue, 0, len(*nodes))
	subSet := make([]map[string]*dynamodb.AttributeValue, 0, 100)
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
		if len(keys) == 100 {
			subSet, err = d.batchGet(keys)
			if err != nil {
				return nil, err
			}
			keys = make([]map[string]*dynamodb.AttributeValue, 0, 100)
			items = append(items, subSet...)
		}
	}
	if len(keys) > 0 {
		subSet, err = d.batchGet(keys)
	}
	if err != nil {
		return nil, err
	}
	items = append(items, subSet...)

	for _, item := range items {
		node := nodes.GetNodeByID(*item["nid"].S)
		for key, value := range item {
			field := getFieldOfInterest(value)
			switch field {
			case "S":
				node.SetString(key, *value.S)
			case "B":
				node.SetBinary(key, value.B)
			case "N":
				node.SetNumber(key, *value.N)
			// this should be ok given we only use the above three fields
			// boto puts bools up to dynamo as numbers :D
			case "BOOL", "BS", "L", "M", "NS", "NULL", "SS":
				return nil, fmt.Errorf("dynamodb type %s is not implemented", field)
			case "":
				return nil, fmt.Errorf("no field found for %s", node)
			}
		}
	}
	return nodes, nil
}

func getFieldOfInterest(item *dynamodb.AttributeValue) string {
	v := reflect.ValueOf(item).Elem()
	t := v.Type()
	n := t.NumField()

	for i := 0; i < n; i++ {
		f := v.Field(i)
		switch f.Kind() {
		case reflect.Ptr:
			if f.IsNil() {
				continue
			}
		case reflect.Struct:
			continue
		case reflect.Slice, reflect.Map:
			if reflect.ValueOf(f.Interface()).Len() == 0 {
				continue
			}
		case reflect.Bool:
			if f.IsNil() {
				continue
			}
		}
		return reflect.Indirect(reflect.ValueOf(item)).Type().Field(i).Name
	}
	return ""
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
		if err != nil {
			log.Printf("batchGet: %s", err.Error())
			continue
		}
		items = append(items, resp.Responses[d.NodeTableName]...)

		// If we have no unprocessed items then we're good
		if _, ok := resp.UnprocessedKeys[d.NodeTableName]; !ok {
			break
		}

		// handle the unprocessed items
		keys = resp.UnprocessedKeys[d.NodeTableName].Keys
	}
	return items, nil
}
