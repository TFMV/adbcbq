package main

import (
	"context"
	"fmt"
	"log"

	"github.com/TFMV/adbcbq/bigquery_driver/bigquery"
	"github.com/apache/arrow/go/v18/arrow/memory"
)

func executeBigQuery(query string) ([]string, error) {
	ctx := context.Background()
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	tmpDriver := bigquery.NewDriver(mem)

	db, err := tmpDriver.NewDatabase(map[string]string{
		bigquery.OptionStringProjectID: "tfmv-371720",
		bigquery.OptionStringDatasetID: "tpch",
	})
	if err != nil {
		log.Printf("BigQuery database creation failed: %v", err)
		return nil, err
	}
	defer db.Close()

	cnxn, err := db.Open(ctx)
	if err != nil {
		log.Printf("BigQuery connection open failed: %v", err)
		return nil, err
	}
	defer cnxn.Close()

	stmt, err := cnxn.NewStatement()
	if err != nil {
		log.Printf("BigQuery statement creation failed: %v", err)
		return nil, err
	}
	defer stmt.Close()

	err = stmt.SetSqlQuery(query)
	if err != nil {
		log.Printf("BigQuery set query failed: %v", err)
		return nil, err
	}

	result, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		log.Printf("BigQuery execute query failed: %v", err)
		return nil, err
	}
	defer result.Release()

	var results []string
	for result.Next() {
		record := result.Record()
		for i := 0; i < int(record.NumCols()); i++ {
			results = append(results, fmt.Sprintf("%v", record.Column(i)))
		}
	}

	return results, nil
}
