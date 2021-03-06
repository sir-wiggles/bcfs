package ddb

import (
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

func init() {
	log.Printf("Registering %s as a backend", PACKAGE_NAME)
	backend.RegisterBackend(PACKAGE_NAME, newDriver)
}

type Driver struct {
	Connection    *dynamodb.DynamoDB
	SourceID      string
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

func (d Driver) send(sender func(interface{}) (interface{}, interface{}), request interface{}) (interface{}, interface{}) {
	for {
		resp, err := sender(request)
		if err != nil {
			// TODO: handle various ddb error codes
			return nil, err
		}
		return resp, nil
	}
}
