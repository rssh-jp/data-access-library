package dynamodb

import (
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type DynamoDB struct {
	DynamoDB *dynamodb.DynamoDB
}

func New(cfgs ...*aws.Config) (*DynamoDB, error) {
	sess, err := session.NewSession(cfgs...)
	if err != nil {
		return nil, err
	}
	return &DynamoDB{
		DynamoDB: dynamodb.New(sess),
	}, nil
}

func (d *DynamoDB) CreateTable2(tableName string) error {
	res, err := d.DynamoDB.CreateTable(&dynamodb.CreateTableInput{
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("key"),
				AttributeType: aws.String("S"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("key"),
				KeyType:       aws.String("HASH"),
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		},
		TableName: aws.String(tableName),
	})
	log.Println("+++", res, err)
	if err != nil {
		return err
	}

	return nil
}

func (d *DynamoDB) ListTable() error {
	res, err := d.DynamoDB.ListTables(&dynamodb.ListTablesInput{})
	if err != nil {
		return err
	}

	for _, tableName := range res.TableNames {
		res, err := d.DynamoDB.DescribeTable(&dynamodb.DescribeTableInput{
			TableName: tableName,
		})

		log.Println("---------", res, err)
	}

	return nil
}

func (d *DynamoDB) Get2(tableName, key string) (string, error) {
	res, err := d.DynamoDB.GetItem(&dynamodb.GetItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"key": {
				S: aws.String(key),
			},
		},
		TableName: aws.String(tableName),
	})
	if err != nil {
		return "", err
	}

	if item, ok := res.Item["value"]; !ok {
		return "", nil
	} else {
		return *item.S, nil
	}
}

func (d *DynamoDB) Set(tableName, key, value string) error {
	res, err := d.DynamoDB.PutItem(&dynamodb.PutItemInput{
		Item: map[string]*dynamodb.AttributeValue{
			"key": {
				S: aws.String(key),
			},
			"value": {
				S: aws.String(value),
			},
		},
		TableName: aws.String(tableName),
	})
	if err != nil {
		return err
	}

	log.Println("res:", res)

	return nil
}

func (d *DynamoDB) CreateTable() error {
	_, err := d.DynamoDB.CreateTable(&dynamodb.CreateTableInput{
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String("Artist"),
				AttributeType: aws.String("S"),
			},
			{
				AttributeName: aws.String("SongTitle"),
				AttributeType: aws.String("S"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String("Artist"),
				KeyType:       aws.String("HASH"),
			},
			{
				AttributeName: aws.String("SongTitle"),
				KeyType:       aws.String("RANGE"),
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		},
		TableName: aws.String("Music"),
	})
	if err != nil {
		return err
	}

	return nil
}

func (d *DynamoDB) PutItem() error {
	_, err := d.DynamoDB.PutItem(&dynamodb.PutItemInput{
		Item: map[string]*dynamodb.AttributeValue{
			"AlbumTitle": {
				S: aws.String("Somewhat Famous"),
			},
			"Artist": {
				S: aws.String("No One You Know"),
			},
			"SongTitle": {
				S: aws.String("Call Me Today"),
			},
		},
		ReturnConsumedCapacity: aws.String("TOTAL"),
		TableName:              aws.String("Music"),
	})
	if err != nil {
		return err
	}

	return nil
}

func (d *DynamoDB) Get(key string) interface{} {
	_, err := d.DynamoDB.GetItem(&dynamodb.GetItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"Artist": {
				S: aws.String("No One You Know"),
			},
			"SongTitle": {
				S: aws.String("Call Me Today"),
			},
		},
		TableName: aws.String("Music"),
	})
	if err != nil {
		return err
	}

	return ""
}
