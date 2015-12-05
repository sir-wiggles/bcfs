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
			IndexName: aws.String("sid_to-sid_from-index"),
			ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
				*EDGE_RANGE: aws.String(hash),
			},
			KeyConditionExpression: aws.String(fmt.Sprintf("%s = %s", *EDGE_RANGE, id)),
		}
		resp := d.query(key)
		items = append(items, resp...)
	}
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
				*EDGE_HASH:  &dynamodb.AttributeValue{S: hash},
				*EDGE_RANGE: &dynamodb.AttributeValue{S: rang},
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
