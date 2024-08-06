package main

import (
	"context"
	"log"
	"time"

	"github.com/TFMV/adbcbq/bigquery_driver/bigquery"
)

func main() {
	// Initialize the BigQuery client, replace with actual initialization code
	client, err := bigquery.OpenClient(context.Background())
	if err != nil {
		log.Fatalf("Failed to create BigQuery client: %v", err)
	}

	// Define your query
	query := "SELECT o_shippriority, count(*) as cnt FROM orders GROUP BY o_shippriority ORDER BY o_shippriority"

	// Create a context with a timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Run the query using the BigQuery client
	arrowIterator, totalRows, err := bigquery.runQuery(ctx, client, query, false)
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
