package bigquery

import (
	"context"
	"fmt"
	"strconv"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

// BigQuery is BigQuery operation structure
type BigQuery struct {
	// Through Client property when wanna use base bigquery function
	Client *bigquery.Client
}

// New return BigQuery instance
func New(projectID string, opts ...option.ClientOption) (*BigQuery, error) {
	c, err := bigquery.NewClient(context.Background(), projectID, opts...)
	if err != nil {
		return nil, err
	}

	return &BigQuery{
		Client: c,
	}, nil
}

// Execute is execute query. returns error
func (bq *BigQuery) Execute(ctx context.Context, query string, queryOpts ...queryOption) error {
	q, err := bq.createQuery(query, queryOpts...)
	if err != nil {
		return err
	}

	_, err = q.Read(ctx)
	if err != nil {
		return err
	}

	return nil
}

// Query is execute query. returns columns, contents, error
func (bq *BigQuery) Query(ctx context.Context, query string, queryOpts ...queryOption) (columns []string, contents [][]string, err error) {
	q, err := bq.createQuery(query, queryOpts...)
	if err != nil {
		return nil, nil, err
	}

	it, err := q.Read(ctx)
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
		for index := range row {
			content = append(content, parseToString(it.Schema[index].Type, row[index]))
		}

		contents = append(contents, content)
	}

	return columns, contents, nil
}

func (bq *BigQuery) createQuery(query string, queryOpts ...queryOption) (*bigquery.Query, error) {
	qc := newQueryConfig()

	for _, opt := range queryOpts {
		err := opt(qc)
		if err != nil {
			return nil, err
		}
	}

	q := bq.Client.Query(query)

	if qc.dstTable != nil {
		q.QueryConfig.Dst = qc.dstTable
	}

	if qc.isLegacy {
		q.UseLegacySQL = true
	} else {
		q.UseStandardSQL = true
	}

	q.CreateDisposition = qc.createDisposition
	q.WriteDisposition = qc.writeDisposition

	return q, nil
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
