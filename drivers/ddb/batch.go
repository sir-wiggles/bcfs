package ddb

import (
	"log"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func (d *Driver) batchGet(table string, keys []map[string]*dynamodb.AttributeValue) ([]map[string]*dynamodb.AttributeValue, error) {

	items := make([]map[string]*dynamodb.AttributeValue, 0, len(keys))
	for {

		resp, err := d.Connection.BatchGetItem(&dynamodb.BatchGetItemInput{
			RequestItems: map[string]*dynamodb.KeysAndAttributes{
				table: &dynamodb.KeysAndAttributes{
					Keys: keys,
				},
			},
		})
		if err != nil {
			log.Printf("batchGet: %s", err.Error())
			continue
		}
		items = append(items, resp.Responses[table]...)

		// If we have no unprocessed items then we're good
		if _, ok := resp.UnprocessedKeys[table]; !ok {
			break
		}

		// handle the unprocessed items
		keys = resp.UnprocessedKeys[table].Keys
	}
	return items, nil
}

func (d *Driver) batchWrite(groups []map[string]*dynamodb.BatchWriteItemInput) error {
	for _, group := range groups {
		for {
			resp, err := d.send(d.Connection.BatchWriteItemRequest, group)
			if err != nil {
				return err.(error)
			}
			output := resp.(*dynamodb.BatchWriteItemOutput)
			if output.UnprocessedItems == nil {
				break
			}
			group = output.UnprocessedItems
		}
	}
	return nil
}
