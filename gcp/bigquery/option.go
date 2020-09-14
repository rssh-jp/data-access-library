package bigquery

import (
	"context"

	"cloud.google.com/go/bigquery"
)

type queryConfig struct {
	isLegacy          bool
	createDisposition bigquery.TableCreateDisposition
	writeDisposition  bigquery.TableWriteDisposition
	dstTable          *bigquery.Table
}

func newQueryConfig() *queryConfig {
	return &queryConfig{
		isLegacy:          false,
		createDisposition: bigquery.CreateIfNeeded,
		writeDisposition:  bigquery.WriteTruncate,
		dstTable:          nil,
	}
}

type queryOption func(*queryConfig) error

// QueryOptionIsLegacy returns queryOption instance with legacy sql enabled
func QueryOptionIsLegacy(isLegacy bool) func(c *queryConfig) error {
	return func(c *queryConfig) error {
		c.isLegacy = isLegacy
		return nil
	}
}

// QueryOptionCreateIfNeeded returns queryOption instance with CreateIfNeeded specified for createDisposition
func QueryOptionCreateIfNeeded() func(c *queryConfig) error {
	return func(c *queryConfig) error {
		c.createDisposition = bigquery.CreateIfNeeded
		return nil
	}
}

// QueryOptionCreateNever returns queryOption instance with CreateNever specified for createDisposition
func QueryOptionCreateNever() func(c *queryConfig) error {
	return func(c *queryConfig) error {
		c.createDisposition = bigquery.CreateNever
		return nil
	}
}

// QueryOptionWriteTruncate returns queryOption instance with WriteTruncate specified for writeDisposition
func QueryOptionWriteTruncate() func(c *queryConfig) error {
	return func(c *queryConfig) error {
		c.writeDisposition = bigquery.WriteTruncate
		return nil
	}
}

// QueryOptionWriteAppend returns queryOption instance with WriteAppend specified for writeDisposition
func QueryOptionWriteAppend() func(c *queryConfig) error {
	return func(c *queryConfig) error {
		c.writeDisposition = bigquery.WriteAppend
		return nil
	}
}

// QueryOptionWriteEmpty returns queryOption instance with WriteEmpty specified for writeDisposition
func QueryOptionWriteEmpty() func(c *queryConfig) error {
	return func(c *queryConfig) error {
		c.writeDisposition = bigquery.WriteEmpty
		return nil
	}
}

// QueryOptionDstTable returns queryOption instance with destination table
func QueryOptionDstTable(bq *BigQuery, datasetID, tableID string) func(c *queryConfig) error {
	return func(c *queryConfig) error {
		ctx := context.Background()
		dataset := bq.Client.Dataset(datasetID)
		if _, err := dataset.Metadata(ctx); err != nil {
			return err
		}

		c.dstTable = dataset.Table(tableID)

		return nil
	}
}
