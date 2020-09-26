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

	cfg := aws.NewConfig()
	cfg.WithEndpoint("http://dynamodb:8000")
	cfg.WithRegion("ap-northeast-1")
	//cfg.WithRegion("us-west-2")
	cfg.WithCredentials(credentials.NewStaticCredentials("dummy", "dummy", "dummy"))
	d, err := dynamodb.New(cfg)
	if err != nil {
		log.Fatal(err)
	}

	err = d.CreateDefaultTable()
	if err != nil {
		log.Println("err", err)
	}

	err = d.Set("test_key", "test_value")
	if err != nil {
		log.Println("err", err)
	}

	res, err := d.Get("test_key")
	if err != nil {
		log.Fatal(err)
	}

	log.Println(res)
}
