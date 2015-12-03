package ddb

import (
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type env struct {
	db *dynamodb.DynamoDB
	t  *testing.T
}

func setup(t *testing.T) *env {
	db := getDynamodbConnection(t)
	teardownTables(t, db)
	setupTables(t, db)
	return &env{
		db: db,
		t:  t,
	}
}

func setupTables(t *testing.T, db *dynamodb.DynamoDB) {
	createEdgeTable(t, db)
	createNodeTable(t, db)
}

func getDynamodbConnection(t *testing.T) *dynamodb.DynamoDB {
	db := dynamodb.New(&aws.Config{
		Endpoint:   LOCAL_ENDPOINT,
		Region:     LOCAL_REGION,
		MaxRetries: LOCAL_MAX_RETRIES,
		Credentials: credentials.NewStaticCredentials(
			LOCAL_KEY,
			LOCAL_SECRET,
			LOCAL_SESSION_TOKEN,
		),
		HTTPClient: &http.Client{
			Timeout: time.Duration(LOCAL_TIMEOUT) * time.Second,
		},
	})

	_, err := db.ListTables(&dynamodb.ListTablesInput{})
	if err != nil {
		t.Logf("dynamodb connection: %s", err.Error())
		t.FailNow()
	}
	return db
}
