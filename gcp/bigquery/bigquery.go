package bigquery

import (
	"context"
	"fmt"
	"strconv"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

type BigQuery struct {
	Client *bigquery.Client
}

func New(projectID string, opts ...option.ClientOption) (*BigQuery, error) {
	c, err := bigquery.NewClient(context.Background(), projectID, opts...)
	if err != nil {
		return nil, err
	}

	return &BigQuery{
		Client: c,
	}, nil
}

func (bq *BigQuery) Query(query string) (columns []string, contents [][]string, err error) {
	q := bq.Client.Query(query)

	it, err := q.Read(context.Background())
	if err != nil {
		return nil, nil, err
	}

	columns = make([]string, 0, len(it.Schema))
	for _, item := range it.Schema {
		columns = append(columns, item.Name)
	}

	contents = make([][]string, 0, int(it.TotalRows))
	for {
		var row []bigquery.Value
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, nil, err
		}

		content := make([]string, 0, len(it.Schema))
		for index, _ := range row {
			content = append(content, parseToString(it.Schema[index].Type, row[index]))
		}

		contents = append(contents, content)
	}

	return columns, contents, nil
}

func parseToString(fieldtype bigquery.FieldType, src interface{}) string {
	switch fieldtype {
	case bigquery.StringFieldType:
		return src.(string)
	case bigquery.BytesFieldType:
	case bigquery.IntegerFieldType:
		return strconv.FormatInt(src.(int64), 10)
	case bigquery.FloatFieldType:
		return strconv.FormatFloat(src.(float64), 'E', -1, 64)
	case bigquery.BooleanFieldType:
	case bigquery.TimestampFieldType:
	case bigquery.RecordFieldType:
	case bigquery.DateFieldType:
	case bigquery.TimeFieldType:
	case bigquery.DateTimeFieldType:
	case bigquery.NumericFieldType:
	case bigquery.GeographyFieldType:
	}
	return fmt.Sprintf("%v", src)
}
