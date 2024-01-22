package main

import (
	"fmt"
)

type config int

const (
	tenK config = iota
	million
)

func configs(c config) (string, string, KnnQueryOptions) {
	var queryFilePath, expectedFilePath string
	var knnQueryOptions KnnQueryOptions
	switch c {
	case tenK:
		queryFilePath = "/Users/arjunsunilkumar/Downloads/benchmark/10k/siftsmall/siftsmall_query.fvecs"
		expectedFilePath = "/Users/arjunsunilkumar/Downloads/benchmark/10k/siftsmall/siftsmall_groundtruth.ivecs"
		knnQueryOptions = KnnQueryOptions{
			DbName:           "a",
			OrgTblName:       "t1",
			OrgTblSkName:     "b",
			OrgTblIdName:     "a",
			OrgTblPkName:     "__mo_fake_pk_col",
			OrgTblVecIdxName: "idx5",
			ProbeVal:         10,
			K:                100,
			Normalize:        true,
		}
	case million:
		queryFilePath = "/Users/arjunsunilkumar/Downloads/benchmark/1million/gist/gist_query.fvecs"
		expectedFilePath = "/Users/arjunsunilkumar/Downloads/benchmark/1million/gist/gist_groundtruth.ivecs"
		knnQueryOptions = KnnQueryOptions{
			DbName:           "a",
			OrgTblName:       "t2",
			OrgTblSkName:     "b",
			OrgTblIdName:     "a",
			OrgTblPkName:     "__mo_fake_pk_col",
			OrgTblVecIdxName: "idx6",
			ProbeVal:         3,
			K:                100,
			Normalize:        true,
		}
	default:
		panic("invalid config")
	}
	return queryFilePath, expectedFilePath, knnQueryOptions
}

func main() {
	queryFilePath, expectedFilePath, knnQueryOptions := configs(tenK)

	vecf32List, err := readInputVectors(queryFilePath)
	if err != nil {
		panic(err)
	}
	expectedSliceList, err := readExpectedOutputIndexes(expectedFilePath)
	if err != nil {
		panic(err)
	}

	failures := 0
	for i, vecf32 := range vecf32List {
		//sql := buildKnnQueryTemplate(vecf32, knnQueryOptions)
		sql := buildKnnQueryTemplateWithIVFFlat(vecf32, knnQueryOptions)
		actualIndexes, _, err := executeKnnQuery("a", sql)
		if err != nil {
			panic(err)
		}
		expectedIndexes := expectedSliceList[i]

		if !compareIndexSlice(expectedIndexes, actualIndexes) {
			fmt.Printf("query %v\n", sql)
			fmt.Printf("exp %v\n", expectedIndexes)
			fmt.Printf("got %v\n", actualIndexes)
			fmt.Printf("\n")
			failures++
		}
	}
	fmt.Printf("total %v failures %v", len(vecf32List), failures)
}

func compareIndexSlice(expected, actual []int32) bool {
	if len(expected) != len(actual) {
		return false
	}
	for i, v := range expected {
		if v != actual[i] {
			return false
		}
	}
	return true
}
