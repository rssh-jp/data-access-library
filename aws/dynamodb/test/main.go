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
	cfg.WithEndpoint("http://localhost:8000")
	cfg.WithRegion("ap-northeast-1")
	cfg.WithCredentials(credentials.NewStaticCredentials("dummy", "dummy", "dummy"))
	d, err := dynamodb.New(cfg)
	if err != nil {
		log.Fatal(err)
	}

	d.CreateTable()

	d.PutItem()

	log.Println(d.Get("nice"))
}
