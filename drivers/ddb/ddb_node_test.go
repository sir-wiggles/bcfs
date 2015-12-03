package ddb

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func createNodeTable(t *testing.T, db *dynamodb.DynamoDB) {

	_, err := db.CreateTable(&dynamodb.CreateTableInput{
		TableName: NODE_TABLE_NAME,
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			&dynamodb.AttributeDefinition{
				AttributeName: NODE_HASH,
				AttributeType: aws.String("S"),
			},
			&dynamodb.AttributeDefinition{
				AttributeName: NODE_RANGE,
				AttributeType: aws.String("S"),
			},
			&dynamodb.AttributeDefinition{
				AttributeName: NODE_ATTR_BLOCKLIST,
				AttributeType: aws.String("S"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			&dynamodb.KeySchemaElement{
				AttributeName: NODE_HASH,
				KeyType:       aws.String("HASH"),
			},
			&dynamodb.KeySchemaElement{
				AttributeName: NODE_RANGE,
				KeyType:       aws.String("RANGE"),
			},
		},
		GlobalSecondaryIndexes: []*dynamodb.GlobalSecondaryIndex{
			&dynamodb.GlobalSecondaryIndex{
				IndexName: NODE_GSI_BLOCKLIST,
				KeySchema: []*dynamodb.KeySchemaElement{
					&dynamodb.KeySchemaElement{
						AttributeName: NODE_HASH,
						KeyType:       aws.String("HASH"),
					},
					&dynamodb.KeySchemaElement{
						AttributeName: NODE_ATTR_BLOCKLIST,
						KeyType:       aws.String("RANGE"),
					},
				},
				Projection: &dynamodb.Projection{
					ProjectionType: aws.String(dynamodb.ProjectionTypeAll),
				},
				ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
					ReadCapacityUnits:  aws.Int64(10),
					WriteCapacityUnits: aws.Int64(10),
				},
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(10),
			WriteCapacityUnits: aws.Int64(10),
		},
	})
	if err != nil {
		t.Fatalf("create node table: %s", err.Error())
	}
}

func (e *env) addNodesToDB(items [][]string) {

	for _, item := range items {
		hash := fmt.Sprintf("%s:%s", item[0], item[1])
		_, err := e.db.PutItem(&dynamodb.PutItemInput{
			TableName: NODE_TABLE_NAME,
			Item: map[string]*dynamodb.AttributeValue{
				*NODE_HASH:  &dynamodb.AttributeValue{S: aws.String(hash)},
				*NODE_RANGE: &dynamodb.AttributeValue{S: aws.String(item[1])},
				"string":    &dynamodb.AttributeValue{S: aws.String("test")},
				"number":    &dynamodb.AttributeValue{N: aws.String("0")},
				"bool":      &dynamodb.AttributeValue{N: aws.String("1")},
			},
		})
		if err != nil {
			e.t.Fatalf("create node: %s", err.Error())
		}
	}
}
