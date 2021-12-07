package main

import (
	"context"
	"log"

	"github.com/rssh-jp/data-access-library/gcp/bigquery"
)

func main() {
	const projectID = "infra-falcon-262905"
	const query = "select name, age from `test_dataset.test_table`"

	ctx := context.Background()

	bq, err := bigquery.New(projectID)
	if err != nil {
		log.Fatal(err)
	}

	cols, contents, err := bq.Query(ctx, query)
	if err != nil {
		log.Fatal(err)
	}

	log.Println(cols, contents)
}
