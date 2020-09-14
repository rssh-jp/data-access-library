package bigquery

import (
	"context"
	"testing"
)

const (
	projectID    = "infra-falcon-262905"
	subProjectID = "test-project01-289306"
	selectQuery  = "select name, age from `test_dataset.test_table`"
)

func TestSuccess(t *testing.T) {
	var mainBQ, subBQ *BigQuery
	var err error

	t.Run("New main project", func(t *testing.T) {
		mainBQ, err = New(projectID)
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("New sub project", func(t *testing.T) {
		subBQ, err = New(subProjectID)
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("Query", func(t *testing.T) {
		cols, contents, err := mainBQ.Query(context.Background(), selectQuery)
		if err != nil {
			t.Fatal(err)
		}

		t.Log(cols, contents)
	})

	t.Run("QueryCopyCreateIfNeededWriteTruncate", func(t *testing.T) {
		err = mainBQ.Execute(context.Background(), selectQuery, QueryOptionDstTable(subBQ, "test_dataset", "test_table2"))
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("QueryCopyCreateIfNeededWriteAppend", func(t *testing.T) {
		err = mainBQ.Execute(context.Background(), selectQuery, QueryOptionDstTable(subBQ, "test_dataset", "test_table2"), QueryOptionWriteAppend())
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("QueryCopyCreateIfNeededWriteEmpty", func(t *testing.T) {
		err = mainBQ.Execute(context.Background(), selectQuery, QueryOptionDstTable(subBQ, "test_dataset", "test_table2"), QueryOptionWriteEmpty())
		if err == nil {
			t.Error("Bug. Table already contains data. But returns not error")
		}
	})

	t.Run("QueryCopyCreateNeverWriteTruncate", func(t *testing.T) {
		err = mainBQ.Execute(context.Background(), selectQuery, QueryOptionDstTable(subBQ, "test_dataset", "test_table2"), QueryOptionCreateNever())
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("QueryCopyCreateNeverWriteAppend", func(t *testing.T) {
		err = mainBQ.Execute(context.Background(), selectQuery, QueryOptionDstTable(subBQ, "test_dataset", "test_table2"), QueryOptionCreateNever(), QueryOptionWriteAppend())
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("QueryCopyCreateNeverWriteEmpty", func(t *testing.T) {
		err = mainBQ.Execute(context.Background(), selectQuery, QueryOptionDstTable(subBQ, "test_dataset", "test_table2"), QueryOptionCreateNever(), QueryOptionWriteEmpty())
		if err == nil {
			t.Error("Bug. Table already contains data. But returns not error")
		}
	})

}
