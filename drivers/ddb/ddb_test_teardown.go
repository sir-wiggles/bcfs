package ddb

import (
	"testing"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func teardownTables(t *testing.T, db *dynamodb.DynamoDB) {

	resp, err := db.ListTables(&dynamodb.ListTablesInput{})
	if err != nil {
		t.Fatalf("teardown tables list: %s", err.Error())
	}
	for _, table := range resp.TableNames {
		_, err = db.DeleteTable(&dynamodb.DeleteTableInput{
			TableName: table,
		})
		if err != nil {
			t.Logf("teardown table %s: %s", *table, err.Error())
		}
	}
	if err != nil {
		t.FailNow()
	}
}
