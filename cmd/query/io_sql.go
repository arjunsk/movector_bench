package main

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"strings"
	"time"
)

type KnnQueryOptions struct {
	DbName           string
	OrgTblName       string
	OrgTblSkName     string
	OrgTblIdName     string
	OrgTblPkName     string
	OrgTblVecIdxName string
	ProbeVal         int
	K                int
	Normalize        bool
}

func buildKnnQueryTemplate(inputVectorVal []float32, options KnnQueryOptions) string {
	dbName := options.DbName
	orgTblName := options.OrgTblName
	orgTblSkName := options.OrgTblSkName
	orgTblIdName := options.OrgTblIdName
	orgTblPkName := options.OrgTblPkName
	orgTblVecIdxName := options.OrgTblVecIdxName
	probeVal := options.ProbeVal
	k := options.K
	inputVectorStr := "[" + strings.Trim(strings.Replace(fmt.Sprint(inputVectorVal), " ", ", ", -1), "[]") + "]"

	idxMetadataTblName, idxCentroidsTblName, idxEntriesTblName, err := getIndexTables(dbName, orgTblVecIdxName, orgTblSkName)
	if err != nil {
		panic(err)
	}

	centroidVersion, err := getCurrentVersionFromMetadata(dbName, idxMetadataTblName)
	if err != nil {
		panic(err)
	}

	l2DistanceArg2 := ""
	if options.Normalize {
		l2DistanceArg2 = "normalize_l2(\"%s\")"
	} else {
		l2DistanceArg2 = "%s"
	}

	getCentroidsQuery := fmt.Sprintf("SELECT `__mo_index_centroid_id` FROM `%s`.`%s` WHERE `__mo_index_centroid_version`=%s ORDER BY l2_distance(`__mo_index_centroid`, "+l2DistanceArg2+" ) ASC LIMIT %d", dbName, idxCentroidsTblName, centroidVersion, inputVectorStr, probeVal)

	getEntriesPkQuery := fmt.Sprintf("SELECT DISTINCT(`__mo_index_pri_col`) FROM `%s`.`%s` WHERE `__mo_index_centroid_fk_version`=%s AND `__mo_index_centroid_fk_id` IN (%s)", dbName, idxEntriesTblName, centroidVersion, getCentroidsQuery)

	getOriginalTblVectorQuery := fmt.Sprintf("SELECT `%s` FROM `%s`.`%s` WHERE `%s` IN (%s) ORDER BY l2_distance(`%s`, \"%s\") ASC LIMIT %d", orgTblIdName, dbName, orgTblName, orgTblPkName, getEntriesPkQuery, orgTblSkName, inputVectorStr, k)

	return getOriginalTblVectorQuery
}

func getIndexTables(dbName, orgTblVecIdxName, orgTblSkName string) (idxMetadataTblName, idxCentroidsTblName, idxEntriesTblName string, err error) {
	dsn := fmt.Sprintf("root:111@tcp(127.0.0.1:6001)/%s", dbName)

	// Open database connection
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return "", "", "", err
	}
	defer db.Close()

	query := "SELECT algo_table_type, index_table_name FROM mo_catalog.mo_indexes WHERE name = ? AND column_name = ?"
	rows, err := db.Query(query, orgTblVecIdxName, orgTblSkName)
	if err != nil {
		return "", "", "", err
	}
	defer rows.Close()

	// Iterate through the result set
	for rows.Next() {
		var algoTableType, indexTableName string
		if err := rows.Scan(&algoTableType, &indexTableName); err != nil {
			return "", "", "", err
		}
		switch algoTableType {
		case "metadata":
			idxMetadataTblName = indexTableName
		case "centroids":
			idxCentroidsTblName = indexTableName
		case "entries":
			idxEntriesTblName = indexTableName
		}
	}

	// Check for errors from iterating over rows
	if err = rows.Err(); err != nil {
		return "", "", "", err
	}

	return idxMetadataTblName, idxCentroidsTblName, idxEntriesTblName, nil
}

// getCurrentVersionFromMetadata retrieves the current version from the metadata table in the database
func getCurrentVersionFromMetadata(dbName, idxMetadataTblName string) (version string, err error) {
	// Database connection string (update with your credentials and host)
	dsn := fmt.Sprintf("root:111@tcp(127.0.0.1:6001)/%s", dbName)

	// Open database connection
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return "", err
	}
	defer db.Close()

	// Prepare SQL query
	query := fmt.Sprintf("SELECT CAST(__mo_index_val AS BIGINT) FROM `%s` WHERE __mo_index_key = 'version'", idxMetadataTblName)

	// Execute the query
	var versionBigInt int64
	err = db.QueryRow(query).Scan(&versionBigInt)
	if err != nil {
		return "", err
	}

	// Convert version to string and return
	version = fmt.Sprintf("%d", versionBigInt)
	return version, nil
}

func executeKnnQuery(dbName, query string) (res []int32, dur time.Duration, err error) {
	beginTs := time.Now()
	dsn := fmt.Sprintf("root:111@tcp(127.0.0.1:6001)/%s", dbName)

	// Open database connection
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, 0, err
	}
	defer db.Close()

	// Execute the query
	rows, err := db.Query(query)
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

	return results, time.Since(beginTs), nil
}
