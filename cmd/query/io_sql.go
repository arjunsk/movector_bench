package main

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"strings"
	"time"
)

type KnnQueryOptions struct {
	DbName           string
	OrgTblName       string
	OrgTblSkName     string
	OrgTblIdName     string
	OrgTblVecIdxName string
	ProbeVal         int
	K                int
}

func buildKnnQueryTemplateWithIVFFlatPg(inputVectorVal []float32, options KnnQueryOptions) string {
	orgTblName := options.OrgTblName
	orgTblSkName := options.OrgTblSkName
	orgTblIdName := options.OrgTblIdName
	k := options.K
	inputVectorStr := "[" + strings.Trim(strings.Replace(fmt.Sprint(inputVectorVal), " ", ", ", -1), "[]") + "]"

	probeQuery := fmt.Sprintf("set ivfflat.probes=%d;\n", options.ProbeVal)
	getOriginalTblVectorQuery := fmt.Sprintf("SELECT %s-1 FROM %s ORDER BY %s <-> '%s' ASC LIMIT %d", orgTblIdName, orgTblName, orgTblSkName, inputVectorStr, k)

	return probeQuery + getOriginalTblVectorQuery
}

func buildKnnQueryTemplateWithIVFFlatMo(inputVectorVal []float32, options KnnQueryOptions) string {
	orgTblName := options.OrgTblName
	orgTblSkName := options.OrgTblSkName
	orgTblIdName := options.OrgTblIdName
	k := options.K
	inputVectorStr := "[" + strings.Trim(strings.Replace(fmt.Sprint(inputVectorVal), " ", ", ", -1), "[]") + "]"

	getOriginalTblVectorQuery := fmt.Sprintf("SELECT %s FROM %s ORDER BY l2_distance(%s,'%s') ASC LIMIT %d", orgTblIdName, orgTblName, orgTblSkName, inputVectorStr, k)

	return getOriginalTblVectorQuery
}

func getDbConnection(dbType, dbName string) (*sql.DB, error) {
	var db *sql.DB
	var err error

	switch dbType {
	case "mysql":
		dsn := fmt.Sprintf("root:111@tcp(127.0.0.1:6001)/%s", dbName)
		db, err = sql.Open("mysql", dsn)
	case "postgres":
		dsn := fmt.Sprintf("postgres://postgres:111@localhost:5432/%s?sslmode=disable", dbName)
		db, err = sql.Open("postgres", dsn)
	}

	if err != nil {
		return nil, err
	}
	return db, nil
}

func executeKnnQuery(dbType, dbName, query string) (res []int32, dur time.Duration, err error) {
	db, err := getDbConnection(dbType, dbName)
	if err != nil {
		return nil, 0, err
	}
	defer db.Close()

	beginTs := time.Now()
	rows, err := db.Query(query)
	duration := time.Since(beginTs)

	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	// Iterate through the result set and collect the results
	var results []int32
	for rows.Next() {
		var id int32
		if err := rows.Scan(&id); err != nil {
			return nil, 0, err
		}
		results = append(results, id)
	}

	// Check for errors from iterating over rows
	if err = rows.Err(); err != nil {
		return nil, 0, err
	}

	return results, duration, nil
}
