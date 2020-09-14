package bigquery

import (
	"context"

	"cloud.google.com/go/bigquery"
)

type queryConfig struct {
	isLegacy          bool
	isDryRun          bool
	createDisposition bigquery.TableCreateDisposition
	writeDisposition  bigquery.TableWriteDisposition
	dstTable          *bigquery.Table
	jobStatistics     *bigquery.JobStatistics
}

func newQueryConfig() *queryConfig {
	return &queryConfig{
		isLegacy:          false,
		isDryRun:          false,
		createDisposition: bigquery.CreateIfNeeded,
		writeDisposition:  bigquery.WriteTruncate,
		dstTable:          nil,
		jobStatistics:     nil,
	}
}

func createQueryConfig(queryOpts ...queryOption) (*queryConfig, error) {
	qc := newQueryConfig()

	for _, opt := range queryOpts {
		err := opt(qc)
		if err != nil {
			return nil, err
		}
	}

	return qc, nil
}

type queryOption func(*queryConfig) error

// QueryOptionIsLegacy returns queryOption instance with legacy sql enabled
func QueryOptionIsLegacy() func(c *queryConfig) error {
	return func(c *queryConfig) error {
		c.isLegacy = true
		return nil
	}
}

// QueryOptionIsDryRun returns queryOption instance with dryrun enabled
func QueryOptionIsDryRun() func(c *queryConfig) error {
	return func(c *queryConfig) error {
		c.isDryRun = true
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

// QueryOptionSetJobStatisticsReference returns queryOption instance with JobStatistics reference
func QueryOptionSetJobStatisticsReference(js *bigquery.JobStatistics) func(c *queryConfig) error {
	return func(c *queryConfig) error {
		c.jobStatistics = js
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
