package bigquery

import (
	"context"
	"reflect"
	"testing"

	"cloud.google.com/go/bigquery"
)

const (
	projectID    = "test-project"
	subProjectID = "test-project"
)

func TestSuccess(t *testing.T) {
	const selectQuery = "select name, age from `test_dataset.test_table`"

	var mainBQ, subBQ *BigQuery
	var err error

	t.Run("New", func(t *testing.T) {
		t.Run("Main project", func(t *testing.T) {
			mainBQ, err = New(projectID)
			if err != nil {
				t.Fatal(err)
			}
		})
		t.Run("Sub project", func(t *testing.T) {
			subBQ, err = New(subProjectID)
			if err != nil {
				t.Fatal(err)
			}
		})
	})

	t.Run("Query", func(t *testing.T) {
		t.Run("Vanilla", func(t *testing.T) {
			cols, contents, err := mainBQ.Query(context.Background(), selectQuery)
			if err != nil {
				t.Fatal(err)
			}

			expectCols := []string{
				"name",
				"age",
			}
			expectContents := [][]string{
				[]string{"aaa", "20"},
				[]string{"ccc", "15"},
				[]string{"bbb", "32"},
			}

			if !reflect.DeepEqual(expectCols, cols) {
				t.Errorf("Could not match columns.\nexpect: %v\nactual: %v", expectCols, cols)
			}

			if !reflect.DeepEqual(expectContents, contents) {
				t.Errorf("Could not match contents.\nexpect: %v\nactual: %v", expectContents, contents)
			}
		})
		t.Run("Dry run", func(t *testing.T) {
			js := new(bigquery.JobStatistics)
			_, _, err := mainBQ.Query(context.Background(), selectQuery, QueryOptionSetJobStatisticsReference(js), QueryOptionIsDryRun())
			if err != nil {
				t.Fatal(err)
			}

			const expectTotalBytesProcessed = 39

			if js.TotalBytesProcessed != expectTotalBytesProcessed {
				t.Errorf("Could not match TotalBytesProcessed.\nexpect: %d\nactual: %d", js.TotalBytesProcessed, expectTotalBytesProcessed)
			}
		})
	})

	t.Run("Execute", func(t *testing.T) {
		t.Run("Vanilla", func(t *testing.T) {
			err := mainBQ.Execute(context.Background(), selectQuery)
			if err != nil {
				t.Fatal(err)
			}
		})
		t.Run("Dry run", func(t *testing.T) {
			js := new(bigquery.JobStatistics)
			err := mainBQ.Execute(context.Background(), selectQuery, QueryOptionSetJobStatisticsReference(js), QueryOptionIsDryRun())
			if err != nil {
				t.Fatal(err)
			}

			const expectTotalBytesProcessed = 39

			if js.TotalBytesProcessed != expectTotalBytesProcessed {
				t.Errorf("Could not match TotalBytesProcessed.\nexpect: %d\nactual: %d", js.TotalBytesProcessed, expectTotalBytesProcessed)
			}
		})
		t.Run("Copy", func(t *testing.T) {
			copyOptions1 := []QueryOption{
				QueryOptionDstTable(subBQ, "test_dataset", "test_table2"),
			}
			t.Run("CreateIfNeeded", func(t *testing.T) {
				copyOptions2 := append(copyOptions1, QueryOptionCreateIfNeeded())
				t.Run("WriteTruncate", func(t *testing.T) {
					err = mainBQ.Execute(context.Background(), selectQuery, append(copyOptions2, QueryOptionWriteTruncate())...)
					if err != nil {
						t.Fatal(err)
					}
				})
				t.Run("WriteAppend", func(t *testing.T) {
					err = mainBQ.Execute(context.Background(), selectQuery, append(copyOptions2, QueryOptionWriteAppend())...)
					if err != nil {
						t.Fatal(err)
					}
				})
				t.Run("WriteEmpty", func(t *testing.T) {
					err = mainBQ.Execute(context.Background(), selectQuery, append(copyOptions2, QueryOptionWriteEmpty())...)
					if err == nil {
						t.Error("Bug. Table already contains data. But returns not error")
					}
				})
			})
			t.Run("CreateNever", func(t *testing.T) {
				copyOptions2 := append(copyOptions1, QueryOptionCreateNever())
				t.Run("WriteTruncate", func(t *testing.T) {
					err = mainBQ.Execute(context.Background(), selectQuery, append(copyOptions2, QueryOptionWriteTruncate())...)
					if err != nil {
						t.Fatal(err)
					}
				})
				t.Run("WriteAppend", func(t *testing.T) {
					err = mainBQ.Execute(context.Background(), selectQuery, append(copyOptions2, QueryOptionWriteAppend())...)
					if err != nil {
						t.Fatal(err)
					}
				})
				t.Run("WriteEmpty", func(t *testing.T) {
					err = mainBQ.Execute(context.Background(), selectQuery, append(copyOptions2, QueryOptionWriteEmpty())...)
					if err == nil {
						t.Error("Bug. Table already contains data. But returns not error")
					}
				})
			})
		})
	})
}
