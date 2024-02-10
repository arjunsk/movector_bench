package main

import "fmt"

type config int

const (
	tenK config = iota
	million128
	million1k
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
			ProbeVal:         3,
			K:                100,
			Normalize:        true,
		}
	case million1k:
		queryFilePath = "/Users/arjunsunilkumar/Downloads/benchmark/1million/gist/gist_query.fvecs"
		expectedFilePath = "/Users/arjunsunilkumar/Downloads/benchmark/1million/gist/gist_groundtruth.ivecs"
		knnQueryOptions = KnnQueryOptions{
			DbName:           "a",
			OrgTblName:       "t4",
			OrgTblSkName:     "b",
			OrgTblIdName:     "a",
			OrgTblPkName:     "__mo_fake_pk_col",
			OrgTblVecIdxName: "idx6",
			ProbeVal:         32,
			K:                100,
			Normalize:        true,

			OverrideIndexTables: true,
			MetadataTableName:   "__mo_index_secondary_018d2a69-37b7-77a1-8387-72d82c3e62d7",
			CentroidsTableName:  "__mo_index_secondary_018d2a69-37b7-7e44-a218-5c5664d1a932",
			EntriesTableName:    "__mo_index_secondary_018d2a69-37b7-7b52-8eaf-46847ebafad9",
		}
	case million128:
		queryFilePath = "/Users/arjunsunilkumar/Downloads/benchmark/1million128/sift/sift_query.fvecs"
		expectedFilePath = "/Users/arjunsunilkumar/Downloads/benchmark/1million128/sift/sift_groundtruth.ivecs"
		knnQueryOptions = KnnQueryOptions{
			DbName:           "a",
			OrgTblName:       "t3",
			OrgTblSkName:     "b",
			OrgTblIdName:     "a",
			OrgTblPkName:     "__mo_fake_pk_col",
			OrgTblVecIdxName: "idx8",
			ProbeVal:         5,
			K:                10,
			Normalize:        true,
		}

	default:
		panic("invalid config")
	}
	return queryFilePath, expectedFilePath, knnQueryOptions
}

func main() {
	queryFilePath, expectedFilePath, knnQueryOptions := configs(million128)
	dbType := "mysql"
	knnQueryOptions.DbName = "a"

	vecf32List, err := readFVecsFile(queryFilePath)
	if err != nil {
		panic(err)
	}
	expectedSliceList, err := readIVecsFile(expectedFilePath)
	if err != nil {
		panic(err)
	}

	recall := float32(0)
	count := float32(0)
	for i, vecf32 := range vecf32List {
		var sql string
		switch dbType {
		case "mysql":
			sql = buildKnnQueryTemplateWithIVFFlatMo(vecf32, knnQueryOptions)
		case "postgres":
			sql = buildKnnQueryTemplateWithIVFFlatPg(vecf32, knnQueryOptions)
		}
		//println(sql)
		actualIndexes, _, err := executeKnnQuery(dbType, knnQueryOptions.DbName, sql)
		if err != nil {
			panic(err)
		}
		expectedIndexes := expectedSliceList[i]

		recall += compareIndexSlice(expectedIndexes, actualIndexes)
		count++
		//if compareIndexSlice(expectedIndexes, actualIndexes) {
		//	fmt.Printf("query %v\n", sql)
		//	fmt.Printf("exp %v\n", expectedIndexes)
		//	fmt.Printf("got %v\n", actualIndexes)
		//	fmt.Printf("\n")
		//	failures++
		//}
		//break
		fmt.Printf("total %v recall %v \n", count, recall/count)
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
