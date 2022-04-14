package bigquery

import (
	"context"

	"cloud.google.com/go/bigquery"
)

type queryConfig struct {
	isLegacy          bool
	isDryRun          bool
	createDisposition *bigquery.TableCreateDisposition
	writeDisposition  *bigquery.TableWriteDisposition
	dstTable          *bigquery.Table
	jobStatistics     *bigquery.JobStatistics
}

func newQueryConfig() *queryConfig {
	return &queryConfig{
		isLegacy:          false,
		isDryRun:          false,
		createDisposition: nil,
		writeDisposition:  nil,
		dstTable:          nil,
		jobStatistics:     nil,
	}
}

func createQueryConfig(queryOpts ...QueryOption) (*queryConfig, error) {
	qc := newQueryConfig()

	for _, opt := range queryOpts {
		err := opt(qc)
		if err != nil {
			return nil, err
		}
	}

	return qc, nil
}

// QueryOption is functional option pattern queryoption
type QueryOption func(*queryConfig) error

// QueryOptionIsLegacy returns QueryOption instance with legacy sql enabled
func QueryOptionIsLegacy() func(c *queryConfig) error {
	return func(c *queryConfig) error {
		c.isLegacy = true
		return nil
	}
}

// QueryOptionIsDryRun returns QueryOption instance with dryrun enabled
func QueryOptionIsDryRun() func(c *queryConfig) error {
	return func(c *queryConfig) error {
		c.isDryRun = true
		return nil
	}
}

// QueryOptionCreateIfNeeded returns QueryOption instance with CreateIfNeeded specified for createDisposition
func QueryOptionCreateIfNeeded() func(c *queryConfig) error {
	return func(c *queryConfig) error {
		s := bigquery.CreateIfNeeded
		c.createDisposition = &s
		return nil
	}
}

// QueryOptionCreateNever returns QueryOption instance with CreateNever specified for createDisposition
func QueryOptionCreateNever() func(c *queryConfig) error {
	return func(c *queryConfig) error {
		s := bigquery.CreateNever
		c.createDisposition = &s
		return nil
	}
}

// QueryOptionWriteTruncate returns QueryOption instance with WriteTruncate specified for writeDisposition
func QueryOptionWriteTruncate() func(c *queryConfig) error {
	return func(c *queryConfig) error {
		s := bigquery.WriteTruncate
		c.writeDisposition = &s
		return nil
	}
}

// QueryOptionWriteAppend returns QueryOption instance with WriteAppend specified for writeDisposition
func QueryOptionWriteAppend() func(c *queryConfig) error {
	return func(c *queryConfig) error {
		s := bigquery.WriteAppend
		c.writeDisposition = &s
		return nil
	}
}

// QueryOptionWriteEmpty returns QueryOption instance with WriteEmpty specified for writeDisposition
func QueryOptionWriteEmpty() func(c *queryConfig) error {
	return func(c *queryConfig) error {
		s := bigquery.WriteEmpty
		c.writeDisposition = &s
		return nil
	}
}

// QueryOptionSetJobStatisticsReference returns QueryOption instance with JobStatistics reference
func QueryOptionSetJobStatisticsReference(js *bigquery.JobStatistics) func(c *queryConfig) error {
	return func(c *queryConfig) error {
		c.jobStatistics = js
		return nil
	}
}

// QueryOptionDstTable returns QueryOption instance with destination table
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
