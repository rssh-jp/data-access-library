package bigquery

import (
	"testing"
)

const (
	projectID   = "test-project1"
	selectQuery = "select name, age from `test_dataset.test_table`"
)

func TestSuccess(t *testing.T) {
	bq, err := New(projectID)
	if err != nil {
		t.Fatal(err)
	}

	cols, contents, err := bq.Query(selectQuery)
	if err != nil {
		t.Fatal(err)
	}

	t.Log(cols, contents)
}
