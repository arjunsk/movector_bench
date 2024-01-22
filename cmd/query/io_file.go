package main

import (
	"encoding/binary"
	"os"
)

func readInputVectors(filename string) ([][]float32, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var vectors [][]float32
	for {
		// Read the dimension of the vector
		var dim int32
		if err := binary.Read(file, binary.LittleEndian, &dim); err != nil {
			break // EOF or other error
		}

		// Read the vector
		vec := make([]float32, dim)
		for i := range vec {
			if err := binary.Read(file, binary.LittleEndian, &vec[i]); err != nil {
				return nil, err
			}
		}

		vectors = append(vectors, vec)
	}

	return vectors, nil
}

func readExpectedOutputIndexes(filename string) ([][]int32, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var vectors [][]int32
	for {
		// Read the dimension of the vector
		var dim int32
		if err := binary.Read(file, binary.LittleEndian, &dim); err != nil {
			break // EOF or other error
		}

		// Read the vector
		vec := make([]int32, dim)
		for i := range vec {
			if err := binary.Read(file, binary.LittleEndian, &vec[i]); err != nil {
				return nil, err
			}
		}

		vectors = append(vectors, vec)
	}

	return vectors, nil
}
