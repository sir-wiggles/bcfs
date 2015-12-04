package ddb

import (
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/sir-wiggles/bcfs/backend"
)

func (d *Driver) GetInEdges(sid string, edges *backend.Edges) error {
}

func (d *Driver) GetOutEdges(sid string, edges *backend.Edges) error {

	var err error
	keys := make([]map[string]*dynamodb.AttributeValue, 0, 100)
	items := make([]map[string]*dynamodb.AttributeValue, 0, len(*edges))
	subSet := make([]map[string]*dynamodb.AttributeValue, 0, 100)
	for fid, tos := range *edges {
		hash := aws.String(fmt.Sprintf("%s:%s", sid, fid))
		for tid, _ := range *tos {
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
		fid := strings.Split(fid, ":")[1]
		tid := strings.Split(tid, ":")[1]
		edge := edges.GetEdgeByID(fid, nid)
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
				return fmt.Errorf("no field found for %s", node)
			}
		}
	}
	return nil
}
