package main

import (
	"context"
	"log"
	"time"

	"github.com/TFMV/adbcbq/bigquery_driver/bigquery"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow/go/v15/arrow/memory"
	"github.com/apache/arrow/go/v18/arrow/array"
)

// OpenClient initializes a BigQuery client using your local implementation
func OpenClient(ctx context.Context) (adbc.Database, error) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	driver := bigquery.NewDriver(mem)
	db, err := driver.NewDatabase(map[string]string{
		bigquery.OptionStringAuthCredentials: "sa.json",
		bigquery.OptionStringProjectID:       "tfmv-371720",
		bigquery.OptionStringDatasetID:       "tpch",
	})
	if err != nil {
		return nil, err
	}
	return db, nil
}

// runQuery executes a query using the BigQuery client and returns the iterator and total rows
func runQuery(ctx context.Context, db adbc.Database, query string) (array.RecordReader, int64, error) {
	cnxn, err := db.Open(ctx)
	if err != nil {
		return nil, 0, err
	}
	defer cnxn.Close()

	stmt, err := cnxn.NewStatement()
	if err != nil {
		return nil, 0, err
	}
	defer stmt.Close()

	err = stmt.SetOption(bigquery.OptionBoolQueryUseLegacySQL, "false")
	if err != nil {
		return nil, 0, err
	}

	err = stmt.SetSqlQuery(query)
	if err != nil {
		return nil, 0, err
	}

	rdr, totalRows, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		return nil, 0, err
	}

	return rdr, totalRows, nil
}

func main() {
	// Initialize the BigQuery client
	client, err := OpenClient(context.Background())
	if err != nil {
		log.Fatalf("Failed to create BigQuery client: %v", err)
	}

	// Define your query
	query := "SELECT o_shippriority, count(*) as cnt FROM tpch.orders GROUP BY o_shippriority ORDER BY o_shippriority"

	// Create a context with a timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Run the query using the BigQuery client
	arrowIterator, totalRows, err := runQuery(ctx, client, query)
	if err != nil {
		log.Fatalf("RunQuery failed: %v", err)
	}

	// Process the results
	for arrowIterator.Next() {
		record := arrowIterator.Record()
		defer record.Release()
		// Process the record
		log.Printf("Record: %v", record)
	}

	if err := arrowIterator.Err(); err != nil {
		log.Fatalf("ArrowIterator error: %v", err)
	}

	log.Printf("Total Rows: %d", totalRows)
}
