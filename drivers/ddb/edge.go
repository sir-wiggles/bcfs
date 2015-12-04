package ddb

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/sir-wiggles/bcfs/backend"
)

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
	return err
}
