package main

import (
	"fmt"
	"time"
)

type config int

const (
	million128 config = iota
)

func configs(c config) (string, string, KnnQueryOptions) {

	var queryFilePath, expectedFilePath string
	var knnQueryOptions KnnQueryOptions

	switch c {
	case million128:
		queryFilePath = "/Users/arjunsunilkumar/Downloads/benchmark/1million128/sift/sift_query.fvecs"
		expectedFilePath = "/Users/arjunsunilkumar/Downloads/benchmark/1million128/sift/sift_groundtruth.ivecs"
		knnQueryOptions = KnnQueryOptions{
			DbName:           "a",
			OrgTblName:       "t3",
			OrgTblIdName:     "a",
			OrgTblSkName:     "b",
			OrgTblVecIdxName: "idx3",
			ProbeVal:         32,
			K:                100,
		}

	default:
		panic("invalid config")
	}
	return queryFilePath, expectedFilePath, knnQueryOptions
}

func main() {
	queryFilePath, expectedFilePath, knnQueryOptions := configs(million128)
	dbType := "mysql"

	vecf32List, err := readFVecsFile(queryFilePath)
	if err != nil {
		panic(err)
	}
	expectedSliceList, err := readIVecsFile(expectedFilePath)
	if err != nil {
		panic(err)
	}

	var totalDuration time.Duration
	totalRecall := float32(0)
	totalCount := float32(0)
	for i, vecf32 := range vecf32List {
		var sql string
		switch dbType {
		case "mysql":
			sql = buildKnnQueryTemplateWithIVFFlatMo(vecf32, knnQueryOptions)
		case "postgres":
			sql = buildKnnQueryTemplateWithIVFFlatPg(vecf32, knnQueryOptions)
		}

		actualIndexes, currDur, err := executeKnnQuery(dbType, knnQueryOptions.DbName, sql)
		totalDuration += currDur

		if err != nil {
			panic(err)
		}
		expectedIndexes := expectedSliceList[i]

		totalRecall += compareIndexSlice(expectedIndexes, actualIndexes)
		totalCount++

		//if i == 3 {
		//	fmt.Printf("query %v\n", sql)
		//	fmt.Printf("query %v\n", sql)
		//	fmt.Printf("exp %v\n", expectedIndexes)
		//	fmt.Printf("got %v\n", actualIndexes)
		//	fmt.Printf("\n")
		//	break
		//}

		fmt.Printf("total %v recall %v duration %v qps %v\n", totalCount, totalRecall/totalCount,
			totalDuration, totalCount/float32(totalDuration.Seconds()))
	}
}

func compareIndexSlice(expected, actual []int32) float32 {
	equalVal := float32(0)
	for i, v := range actual {
		if v == expected[i] {
			equalVal++
		}
	}
	return equalVal / float32(len(actual))
}
