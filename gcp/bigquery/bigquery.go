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

// Query is execute query.  returns columns, contents, error
func (bq *BigQuery) Query(ctx context.Context, query string) (columns []string, contents [][]string, err error) {
	q := bq.Client.Query(query)

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

// QueryCopyCreateIfNeededWriteTruncate is execute query and copy another table. Create IfNeeded, Write Truncate
func (bq *BigQuery) QueryCopyCreateIfNeededWriteTruncate(ctx context.Context, query string, dstBQ *BigQuery, dstDatasetID, dstTableID string) error {
	return bq.queryCopy(ctx, query, dstBQ, dstDatasetID, dstTableID, bigquery.CreateIfNeeded, bigquery.WriteTruncate)
}

// QueryCopyCreateIfNeededWriteAppend is execute query and copy another table. Create IfNeeded, Write Append
func (bq *BigQuery) QueryCopyCreateIfNeededWriteAppend(ctx context.Context, query string, dstBQ *BigQuery, dstDatasetID, dstTableID string) error {
	return bq.queryCopy(ctx, query, dstBQ, dstDatasetID, dstTableID, bigquery.CreateIfNeeded, bigquery.WriteAppend)
}

// QueryCopyCreateIfNeededWriteEmpty is execute query and copy another table. Create IfNeeded, Write Empty
func (bq *BigQuery) QueryCopyCreateIfNeededWriteEmpty(ctx context.Context, query string, dstBQ *BigQuery, dstDatasetID, dstTableID string) error {
	return bq.queryCopy(ctx, query, dstBQ, dstDatasetID, dstTableID, bigquery.CreateIfNeeded, bigquery.WriteEmpty)
}

// QueryCopyCreateNeverWriteTruncate is execute query and copy another table. Create Never, Write Truncate
func (bq *BigQuery) QueryCopyCreateNeverWriteTruncate(ctx context.Context, query string, dstBQ *BigQuery, dstDatasetID, dstTableID string) error {
	return bq.queryCopy(ctx, query, dstBQ, dstDatasetID, dstTableID, bigquery.CreateNever, bigquery.WriteTruncate)
}

// QueryCopyCreateNeverWriteAppend is execute query and copy another table. Create Never, Write Append
func (bq *BigQuery) QueryCopyCreateNeverWriteAppend(ctx context.Context, query string, dstBQ *BigQuery, dstDatasetID, dstTableID string) error {
	return bq.queryCopy(ctx, query, dstBQ, dstDatasetID, dstTableID, bigquery.CreateNever, bigquery.WriteAppend)
}

// QueryCopyCreateNeverWriteEmpty is execute query and copy another table. Create Never, Write Empty
func (bq *BigQuery) QueryCopyCreateNeverWriteEmpty(ctx context.Context, query string, dstBQ *BigQuery, dstDatasetID, dstTableID string) error {
	return bq.queryCopy(ctx, query, dstBQ, dstDatasetID, dstTableID, bigquery.CreateNever, bigquery.WriteEmpty)
}

func (bq *BigQuery) queryCopy(ctx context.Context, query string, dstBQ *BigQuery, dstDatasetID, dstTableID string, createDisposition bigquery.TableCreateDisposition, writeDisposition bigquery.TableWriteDisposition) error {
	q := bq.Client.Query(query)

	q.QueryConfig.Dst = dstBQ.Client.Dataset(dstDatasetID).Table(dstTableID)
	q.CreateDisposition = createDisposition
	q.WriteDisposition = writeDisposition

	_, err := q.Read(ctx)
	if err != nil {
		return err
	}

	return nil
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
