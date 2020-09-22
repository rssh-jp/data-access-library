package main

import (
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/rssh-jp/data-access-library/aws/dynamodb"
)

func main() {
	log.Println("START")
	defer log.Println("END")

	log.Println("state:", 1)
	log.Println("state:", 2)
	log.Println("state:", 7)

	cfg := aws.NewConfig()
	cfg.WithEndpoint("http://dynamodb:8000")
	//cfg.WithRegion("ap-northeast-1")
	cfg.WithRegion("us-west-2")
	cfg.WithCredentials(credentials.NewStaticCredentials("dummy", "dummy", "dummy"))
	d, err := dynamodb.New(cfg)
	if err != nil {
		log.Fatal(err)
	}

	d.CreateTable2("nicetable")
	d.ListTable()
	d.Set("nicetable", "test_key", "test_value")
	res, err := d.Get2("nicetable", "test_key")
	if err != nil {
		log.Fatal(err)
	}

	d.CreateTable()
	log.Println("#################", res)

	d.PutItem()

	log.Println(d.Get("nice"))
}
