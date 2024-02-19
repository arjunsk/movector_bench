package main

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"strings"
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

var (
	db *sql.DB
)

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

	probeQuery := fmt.Sprintf("set @probe_limit=%d;\n", options.ProbeVal)
	getOriginalTblVectorQuery := fmt.Sprintf("SELECT %s FROM %s ORDER BY l2_distance(%s,'%s') ASC LIMIT %d;", orgTblIdName, orgTblName, orgTblSkName, inputVectorStr, k)

	return probeQuery + getOriginalTblVectorQuery
}

func initDb(dbType, dbName string) error {

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
		return err
	}
	return nil
}

func closeDB() {
	err := db.Close()
	if err != nil {
		panic(err)
	}
}

func executeKnnQuery(query string) (res []int32, err error) {
	rows, err := db.Query(query)

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Iterate through the result set and collect the results
	var results []int32
	for rows.Next() {
		var id int32
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		results = append(results, id)
	}

	// Check for errors from iterating over rows
	if err = rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}
