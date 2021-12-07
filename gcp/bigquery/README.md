# BigQuery
https://godoc.org/cloud.google.com/go/bigquery

# Usage
## Query
Query returns column name array, content list, error.
```
package main

import (
	"context"
	"log"

	"github.com/rssh-jp/data-access-library/gcp/bigquery"
)

func main() {
	const projectID = "own-project-id"
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
```

## Execute
Execute query and return error.
```
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

	err = bq.Execute(ctx, query)
	if err != nil {
		log.Fatal(err)
	}
}
```

## ExecuteAsync
Execute query asynchronous and returns query job id, error.
```
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

	jobID, err := bq.ExecuteAsync(ctx, query)
	if err != nil {
		log.Fatal(err)
	}

	log.Println(jobID)
}
```
