package dynamodb

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type DynamoDB struct {
	DynamoDB         *dynamodb.DynamoDB
	DefaultTableName string
	DefaultKeyName   string
	DefaultValueName string
}

func New(cfgs ...*aws.Config) (*DynamoDB, error) {
	sess, err := session.NewSession(cfgs...)
	if err != nil {
		return nil, err
	}
	d := &DynamoDB{
		DynamoDB:         dynamodb.New(sess),
		DefaultTableName: "default_table",
		DefaultKeyName:   "key",
		DefaultValueName: "value",
	}

	return d, nil
}

func (d *DynamoDB) CreateDefaultTable() error {
	return d.CreateTable(d.DefaultTableName, d.DefaultKeyName)
}

func (d *DynamoDB) CreateTable(tableName, keyName string) error {
	_, err := d.DynamoDB.CreateTable(&dynamodb.CreateTableInput{
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String(keyName),
				AttributeType: aws.String("S"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String(keyName),
				KeyType:       aws.String("HASH"),
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		},
		TableName: aws.String(tableName),
	})
	if err != nil {
		return err
	}

	return nil
}

func (d *DynamoDB) TableNames() ([]string, error) {
	res, err := d.DynamoDB.ListTables(&dynamodb.ListTablesInput{})
	if err != nil {
		return nil, err
	}

	ret := make([]string, 0, len(res.TableNames))

	for _, tableName := range res.TableNames {
		ret = append(ret, *tableName)
	}

	return ret, nil
}

func (d *DynamoDB) Get(key string) (string, error) {
	res, err := d.DynamoDB.GetItem(&dynamodb.GetItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			d.DefaultKeyName: {
				S: aws.String(key),
			},
		},
		TableName: aws.String(d.DefaultTableName),
	})
	if err != nil {
		return "", err
	}

	if item, ok := res.Item[d.DefaultValueName]; !ok {
		return "", nil
	} else {
		return *item.S, nil
	}
}

func (d *DynamoDB) Set(key, value string) error {
	_, err := d.DynamoDB.PutItem(&dynamodb.PutItemInput{
		Item: map[string]*dynamodb.AttributeValue{
			d.DefaultKeyName: {
				S: aws.String(key),
			},
			d.DefaultValueName: {
				S: aws.String(value),
			},
		},
		TableName: aws.String(d.DefaultTableName),
	})
	if err != nil {
		return err
	}

	return nil
}
