package ddb

import (
	"fmt"
	"log"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/sir-wiggles/bcfs/backend"
)

func (d *Driver) GetInEdges(edges *backend.Edges) error {
	var err error
	var sid = d.SourceID
	items := make([]map[string]*dynamodb.AttributeValue, 0, len(*edges))
	for id, tos := range *edges {
		hash := aws.String(fmt.Sprintf("%s:%s", sid, id))
		key := &dynamodb.QueryInput{
			TableName: aws.String(d.EdgeTableName),
			IndexName: EDGE_GSI_REVERSE,
			ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
				*EDGE_RANGE: aws.String(hash),
			},
			KeyConditionExpression: aws.String(fmt.Sprintf("%s = %s", *EDGE_RANGE, id)),
		}
		resp := d.query(key)
		items = append(items, resp...)
	}
	for _, item := range items {
		sid_fid := *item[*EDGE_HASH].S
		sid_tid := *item[*EDGE_RANGE].S
		fid := strings.Split(sid_fid, ":")[1]
		tid := strings.Split(sid_tid, ":")[1]
		edge := edges.GetEdgeByID(tid, fid)
		for key, value := range item {
			field := getFieldOfInterest(value)
			switch field {
			case "S":
				edge.SetString(key, *value.S)
			case "B":
				edge.SetBinary(key, value.B)
			case "N":
				edge.SetNumber(key, *value.N)
			case "BOOL", "BS", "L", "M", "NS", "NULL", "SS":
				return fmt.Errorf("dynamodb type %s is not implemented", field)
			case "":
				return fmt.Errorf("no field found for %s", edge)
			}
		}
	}
	return nil
}

func (d *Driver) query(key *dynamodb.QueryInput) []map[string]*dynamodb.AttributeValue {
	items := make([]map[string]*dynamodb.AttributeValue, 100)
	for {
		resp, err := d.Connection.Query(key)
		if err != nil {
			log.Println(err.Error())
			continue
		}
		items = append(items, resp.Items...)
		if resp.LastEvaluatedKey == nil {
			break
		}
		key.ExclusiveStartKey = resp.LastEvaluatedKey
	}
	return resp.Items
}

// GetOutEdges will get all the edges extending from a parent node and going to its children.
// This will utilize batch as much as possible
func (d *Driver) GetOutEdges(edges *backend.Edges) error {

	var err error
	var sid = d.SourceID
	keys := make([]map[string]*dynamodb.AttributeValue, 0, 100)
	items := make([]map[string]*dynamodb.AttributeValue, 0, len(*edges))
	subSet := make([]map[string]*dynamodb.AttributeValue, 0, 100)
	for fid, tos := range *edges {
		hash := aws.String(fmt.Sprintf("%s:%s", sid, fid))
		for tid, _ := range tos {
			rang := aws.String(fmt.Sprintf("%s:%s", sid, tid))
			key := map[string]*dynamodb.AttributeValue{
				*EDGE_HASH: &dynamodb.AttributeValue{S: hash},
			}
			keys = append(keys, key)
			if len(keys) == 100 {
				subSet, err = d.batchGet(d.EdgeTableName, keys)
				if err != nil {
					return err
				}
				keys = make([]map[string]*dynamodb.AttributeValue, 0, 100)
				items = append(items, subSet...)
			}
		}
	}
	if len(keys) > 0 {
		subSet, err = d.batchGet(d.EdgeTableName, keys)
	}
	if err != nil {
		return err
	}
	items = append(items, subSet...)

	for _, item := range items {
		sid_fid := *item["sid_from"].S
		sid_tid := *item["sid_to"].S
		fid := strings.Split(sid_fid, ":")[1]
		tid := strings.Split(sid_tid, ":")[1]
		edge := edges.GetEdgeByID(fid, tid)
		for key, value := range item {
			field := getFieldOfInterest(value)
			switch field {
			case "S":
				edge.SetString(key, *value.S)
			case "B":
				edge.SetBinary(key, value.B)
			case "N":
				edge.SetNumber(key, *value.N)
			case "BOOL", "BS", "L", "M", "NS", "NULL", "SS":
				return fmt.Errorf("dynamodb type %s is not implemented", field)
			case "":
				return fmt.Errorf("no field found for %s", edge)
			}
		}
	}
	return nil
}
func (d *Driver) CreateEdges(edges *backend.Edges) error {

	items := make([]*dynamodb.WriteRequest, 0, 25)
	groups := make([]map[string][]*dynamodb.BatchWriteItemInput, 0, 1)
	for nid, properties := range *edges {

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
		items = append(items, dynamodb.WriteRequest{PutRequest: &dynamodb.PutRequest{Item: item}})
		if len(items) == 25 {
			groups = append(groups, map[string]*dynamodb.BatchWriteItemInput{d.NodeTableName: items})
			items = make([]*dynamodb.WriteRequest, 0, 25)
		}
	}
	if len(items) > 0 {
		groups = append(groups, items)
	}
	err := d.batchWrite(groups)
	return err
}
