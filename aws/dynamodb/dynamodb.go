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

func (d *DynamoDB) CreateTable() error {
	log.Println("+++++++++++++", 1)
	res, err := d.DynamoDB.CreateTable(&dynamodb.CreateTableInput{
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
	log.Println("+++++++++++++", 2)
	if err != nil {
		return err
	}
	log.Println("+++++++++++++", 3)

	log.Println(res, err)

	return nil
}

func (d *DynamoDB) PutItem() error {
	log.Println("----------", 1)
	res, err := d.DynamoDB.PutItem(&dynamodb.PutItemInput{
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
	log.Println("----------", 2)
	if err != nil {
		return err
	}
	log.Println("----------", 3)

	log.Println(res, err)

	return nil
}

func (d *DynamoDB) Get(key string) interface{} {
	log.Println("key", key, key, key)

	log.Println(d.DynamoDB)

	res, err := d.DynamoDB.GetItem(&dynamodb.GetItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"Artist": {
				S: aws.String("Acme Band"),
			},
			"SongTitle": {
				S: aws.String("Happy Day"),
			},
		},
		TableName: aws.String("Music"),
	})

	log.Println(res, err)

	return ""
}
