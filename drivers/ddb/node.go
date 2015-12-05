package ddb

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/sir-wiggles/bcfs/backend"
)

// Given a list of node ids return all the nodes and their properties
func (d *Driver) GetNodes(nodes *backend.Nodes) error {

	var err error
	var sid = d.SourceID
	keys := make([]map[string]*dynamodb.AttributeValue, 0, 100)
	items := make([]map[string]*dynamodb.AttributeValue, 0, len(*nodes))
	subSet := make([]map[string]*dynamodb.AttributeValue, 0, 100)
	for nid, _ := range *nodes {
		hash := aws.String(fmt.Sprintf("%s:%s", sid, nid))
		key := map[string]*dynamodb.AttributeValue{
			*NODE_HASH:  &dynamodb.AttributeValue{S: hash},
			*NODE_RANGE: &dynamodb.AttributeValue{S: aws.String(nid)},
		}
		keys = append(keys, key)
		if len(keys) == 100 {
			subSet, err = d.batchGet(d.NodeTableName, keys)
			if err != nil {
				return err
			}
			keys = make([]map[string]*dynamodb.AttributeValue, 0, 100)
			items = append(items, subSet...)
		}
	}
	if len(keys) > 0 {
		subSet, err = d.batchGet(d.NodeTableName, keys)
	}
	if err != nil {
		return err
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
				return fmt.Errorf("dynamodb type %s is not implemented", field)
			case "":
				return fmt.Errorf("no field found for %s", node)
			}
		}
	}
	return nil
}

func (d *Driver) CreateNodes(nodes *backend.Nodes) error {

	items := make([]*dynamodb.WriteRequest, 0, len(*nodes))
	for nid, properties := range *nodes {

		item := make(map[string]*dynamodb.AttributeValue, 0, len(properties))
		for key, property := range properties {
			switch property.Type {
			case backend.StringProperty:
				value := property.Value.(string)
				item[key] = &dynamodb.AttributeValue{S: aws.String(value)}
			case backend.NumberProperty:
				value := property.Value.(int)
				item[key] = &dynamodb.AttributeValue{N: aws.String(value)}
			case backend.BinaryProperty:
				value := property.Value.(int)
				item[key] = &dynamodb.AttributeValue{N: aws.String(value)}
			}
		}

		write := dynamodb.WriteRequest{PutRequest: &dynamodb.PutRequest{Item: item}}
		items = append(items, write)

	}
	return nil
}

func (d *Driver) batchWrite(items []map[string]*dynamodb.AttributeValue) error {

	input := &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]*dynamodb.WriteRequest{
			d.NodeTableName: items,
		},
	}
}
