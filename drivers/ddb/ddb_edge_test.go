package ddb

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func createEdgeTable(t *testing.T, db *dynamodb.DynamoDB) {

	_, err := db.CreateTable(&dynamodb.CreateTableInput{
		TableName: EDGE_TABLE_NAME,
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			&dynamodb.AttributeDefinition{
				AttributeName: EDGE_HASH,
				AttributeType: aws.String("S"),
			},
			&dynamodb.AttributeDefinition{
				AttributeName: EDGE_RANGE,
				AttributeType: aws.String("S"),
			},
			&dynamodb.AttributeDefinition{
				AttributeName: EDGE_ATTR_NAME,
				AttributeType: aws.String("S"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			&dynamodb.KeySchemaElement{
				AttributeName: EDGE_HASH,
				KeyType:       aws.String("HASH"),
			},
			&dynamodb.KeySchemaElement{
				AttributeName: EDGE_RANGE,
				KeyType:       aws.String("RANGE"),
			},
		},
		LocalSecondaryIndexes: []*dynamodb.LocalSecondaryIndex{
			&dynamodb.LocalSecondaryIndex{
				IndexName: EDGE_LSI_NAME,
				KeySchema: []*dynamodb.KeySchemaElement{
					&dynamodb.KeySchemaElement{
						AttributeName: EDGE_HASH,
						KeyType:       aws.String("HASH"),
					},
					&dynamodb.KeySchemaElement{
						AttributeName: EDGE_ATTR_NAME,
						KeyType:       aws.String("RANGE"),
					},
				},
				Projection: &dynamodb.Projection{
					ProjectionType: aws.String(dynamodb.ProjectionTypeInclude),
					NonKeyAttributes: []*string{
						EDGE_HASH,
						EDGE_RANGE,
						EDGE_ATTR_NAME,
					},
				},
			},
		},
		GlobalSecondaryIndexes: []*dynamodb.GlobalSecondaryIndex{
			&dynamodb.GlobalSecondaryIndex{
				IndexName: EDGE_GSI_REVERSE,
				KeySchema: []*dynamodb.KeySchemaElement{
					&dynamodb.KeySchemaElement{
						AttributeName: EDGE_RANGE,
						KeyType:       aws.String("HASH"),
					},
					&dynamodb.KeySchemaElement{
						AttributeName: EDGE_HASH,
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
		t.Fatalf("create edge table: %s", err.Error())
	}
}

func (e *env) addEdgesToDB(items [][]string) {

	for _, item := range items {
		_, err := e.db.PutItem(&dynamodb.PutItemInput{
			TableName: EDGE_TABLE_NAME,
			Item: map[string]*dynamodb.AttributeValue{
				*EDGE_HASH:  &dynamodb.AttributeValue{S: aws.String(item[0])},
				*EDGE_RANGE: &dynamodb.AttributeValue{S: aws.String(item[1])},
				"name":      &dynamodb.AttributeValue{S: aws.String("foo")},
			},
		})
		if err != nil {
			e.t.Fatalf("create edge: %s", err.Error())
		}
	}
}
