package bigquery

import (
	"context"
	"log"
	"reflect"
	"testing"

	"cloud.google.com/go/bigquery"
)

const (
	projectID    = "infra-falcon-262905"
	subProjectID = "test-project01-289306"
)

func TestMain(m *testing.M) {
	err := preprocess()
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		err := postprocess()
		if err != nil {
			log.Fatal(err)
		}
	}()

	m.Run()
}
func preprocess() error {
	mainBQ, err := New(projectID)
	if err != nil {
		return err
	}

	subBQ, err := New(subProjectID)
	if err != nil {
		return err
	}

	log.Println("CREATE DATASET main")
	err = mainBQ.Client.Dataset("test_dataset2").Create(context.Background(), &bigquery.DatasetMetadata{})
	if err != nil {
		return err
	}

	log.Println("CREATE DATASET sub")
	err = subBQ.Client.Dataset("test_dataset2").Create(context.Background(), &bigquery.DatasetMetadata{})
	if err != nil {
		return err
	}

	log.Println("CREATE TABLE")
	err = mainBQ.Client.Dataset("test_dataset2").Table("test_table").Create(context.Background(), &bigquery.TableMetadata{
		Schema: bigquery.Schema{
			&bigquery.FieldSchema{
				Name:     "id",
				Required: true,
				Type:     bigquery.IntegerFieldType,
			},
			&bigquery.FieldSchema{
				Name: "name",
				Type: bigquery.StringFieldType,
			},
			&bigquery.FieldSchema{
				Name: "age",
				Type: bigquery.IntegerFieldType,
			},
		},
	})
	if err != nil {
		return err
	}

	return nil
}
func postprocess() error {
	mainBQ, err := New(projectID)
	if err != nil {
		return err
	}

	subBQ, err := New(subProjectID)
	if err != nil {
		return err
	}

	log.Println("DELETE DATASET main")
	err = mainBQ.Client.Dataset("test_dataset2").DeleteWithContents(context.Background())
	if err != nil {
		return err
	}

	log.Println("DELETE DATASET sub")
	err = subBQ.Client.Dataset("test_dataset2").DeleteWithContents(context.Background())
	if err != nil {
		return err
	}

	return nil
}

func TestInsert(t *testing.T) {
	bq, err := New(projectID)
	if err != nil {
		t.Fatal(err)
	}

	query := "INSERT INTO `test_dataset2.test_table` (id, name, age) values (1, 'aa', 32), (2, 'bb', 25), (3, 'cc', 13)"

	_, err = bq.Client.Query(query).Read(context.Background())
	if err != nil {
		t.Fatal(err)
	}
}

func TestSuccess(t *testing.T) {
	const selectQuery = "select id, name, age from `test_dataset2.test_table` order by id"

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
				"id",
				"name",
				"age",
			}
			expectContents := [][]string{
				[]string{"1", "aa", "32"},
				[]string{"2", "bb", "25"},
				[]string{"3", "cc", "13"},
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

			const expectTotalBytesProcessed = 60

			if js.TotalBytesProcessed != expectTotalBytesProcessed {
				t.Errorf("Could not match TotalBytesProcessed.\nexpect: %d\nactual: %d", expectTotalBytesProcessed, js.TotalBytesProcessed)
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

			const expectTotalBytesProcessed = 60

			if js.TotalBytesProcessed != expectTotalBytesProcessed {
				t.Errorf("Could not match TotalBytesProcessed.\nexpect: %d\nactual: %d", expectTotalBytesProcessed, js.TotalBytesProcessed)
			}
		})
		t.Run("Copy", func(t *testing.T) {
			t.Run("Same project", func(t *testing.T) {

				t.Run("CreateIfNeeded", func(t *testing.T) {
					const confirmQuery = "select id, name, age from `test_dataset2.test_table2` order by id"

					copyOptions1 := []QueryOption{
						QueryOptionDstTable(mainBQ, "test_dataset2", "test_table2"),
					}

					copyOptions2 := append(copyOptions1, QueryOptionCreateIfNeeded())

					t.Run("WriteAppend", func(t *testing.T) {
						err = mainBQ.Execute(context.Background(), selectQuery, append(copyOptions2, QueryOptionWriteAppend())...)
						if err != nil {
							t.Fatal(err)
						}

						cols, contents, err := mainBQ.Query(context.Background(), confirmQuery)
						if err != nil {
							t.Fatal(err)
						}

						expectCols := []string{
							"id",
							"name",
							"age",
						}
						expectContents := [][]string{
							[]string{"1", "aa", "32"},
							[]string{"2", "bb", "25"},
							[]string{"3", "cc", "13"},
						}

						if !reflect.DeepEqual(expectCols, cols) {
							t.Errorf("Could not match columns.\nexpect: %v\nactual: %v", expectCols, cols)
						}

						if !reflect.DeepEqual(expectContents, contents) {
							t.Errorf("Could not match contents.\nexpect: %v\nactual: %v", expectContents, contents)
						}
					})
					t.Run("WriteTruncate", func(t *testing.T) {
						err = mainBQ.Execute(context.Background(), selectQuery, append(copyOptions2, QueryOptionWriteTruncate())...)
						if err != nil {
							t.Fatal(err)
						}

						cols, contents, err := mainBQ.Query(context.Background(), confirmQuery)
						if err != nil {
							t.Fatal(err)
						}

						expectCols := []string{
							"id",
							"name",
							"age",
						}
						expectContents := [][]string{
							[]string{"1", "aa", "32"},
							[]string{"2", "bb", "25"},
							[]string{"3", "cc", "13"},
						}

						if !reflect.DeepEqual(expectCols, cols) {
							t.Errorf("Could not match columns.\nexpect: %v\nactual: %v", expectCols, cols)
						}

						if !reflect.DeepEqual(expectContents, contents) {
							t.Errorf("Could not match contents.\nexpect: %v\nactual: %v", expectContents, contents)
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
					const confirmQuery = "select id, name, age from `test_dataset2.test_table3` order by id"

					copyOptions1 := []QueryOption{
						QueryOptionDstTable(mainBQ, "test_dataset2", "test_table3"),
					}

					copyOptions2 := append(copyOptions1, QueryOptionCreateNever())
					t.Run("WriteAppend", func(t *testing.T) {
						err = mainBQ.Execute(context.Background(), selectQuery, append(copyOptions2, QueryOptionWriteAppend())...)
						if err == nil {
							t.Error("Bug. Table never create. But returns not error")
						}
					})
					t.Run("WriteTruncate", func(t *testing.T) {
						err = mainBQ.Execute(context.Background(), selectQuery, append(copyOptions2, QueryOptionWriteTruncate())...)
						if err == nil {
							t.Error("Bug. Table never create. But returns not error")
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
			t.Run("Another project", func(t *testing.T) {
				t.Run("CreateIfNeeded", func(t *testing.T) {
					const confirmQuery = "select id, name, age from `test_dataset2.test_table2` order by id"

					copyOptions1 := []QueryOption{
						QueryOptionDstTable(subBQ, "test_dataset2", "test_table2"),
					}

					copyOptions2 := append(copyOptions1, QueryOptionCreateIfNeeded())

					t.Run("WriteAppend", func(t *testing.T) {
						err = mainBQ.Execute(context.Background(), selectQuery, append(copyOptions2, QueryOptionWriteAppend())...)
						if err != nil {
							t.Fatal(err)
						}

						cols, contents, err := mainBQ.Query(context.Background(), confirmQuery)
						if err != nil {
							t.Fatal(err)
						}

						expectCols := []string{
							"id",
							"name",
							"age",
						}
						expectContents := [][]string{
							[]string{"1", "aa", "32"},
							[]string{"2", "bb", "25"},
							[]string{"3", "cc", "13"},
						}

						if !reflect.DeepEqual(expectCols, cols) {
							t.Errorf("Could not match columns.\nexpect: %v\nactual: %v", expectCols, cols)
						}

						if !reflect.DeepEqual(expectContents, contents) {
							t.Errorf("Could not match contents.\nexpect: %v\nactual: %v", expectContents, contents)
						}
					})
					t.Run("WriteTruncate", func(t *testing.T) {
						err = mainBQ.Execute(context.Background(), selectQuery, append(copyOptions2, QueryOptionWriteTruncate())...)
						if err != nil {
							t.Fatal(err)
						}

						cols, contents, err := mainBQ.Query(context.Background(), confirmQuery)
						if err != nil {
							t.Fatal(err)
						}

						expectCols := []string{
							"id",
							"name",
							"age",
						}
						expectContents := [][]string{
							[]string{"1", "aa", "32"},
							[]string{"2", "bb", "25"},
							[]string{"3", "cc", "13"},
						}

						if !reflect.DeepEqual(expectCols, cols) {
							t.Errorf("Could not match columns.\nexpect: %v\nactual: %v", expectCols, cols)
						}

						if !reflect.DeepEqual(expectContents, contents) {
							t.Errorf("Could not match contents.\nexpect: %v\nactual: %v", expectContents, contents)
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
					const confirmQuery = "select id, name, age from `test_dataset2.test_table2` order by id"

					copyOptions1 := []QueryOption{
						QueryOptionDstTable(subBQ, "test_dataset2", "test_table3"),
					}

					copyOptions2 := append(copyOptions1, QueryOptionCreateNever())
					t.Run("WriteAppend", func(t *testing.T) {
						err = mainBQ.Execute(context.Background(), selectQuery, append(copyOptions2, QueryOptionWriteAppend())...)
						if err == nil {
							t.Error("Bug. Table never create. But returns not error")
						}
					})
					t.Run("WriteTruncate", func(t *testing.T) {
						err = mainBQ.Execute(context.Background(), selectQuery, append(copyOptions2, QueryOptionWriteTruncate())...)
						if err == nil {
							t.Error("Bug. Table never create. But returns not error")
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
	})
}
