package ddb

import (
	"log"
	"reflect"

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
	EdgeTableName string
}

func newDriver(c *backend.Config) (backend.Graph, error) {
	return &Driver{}, nil
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
