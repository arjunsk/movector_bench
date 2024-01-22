package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

// readFVecsFile reads vectors from an .fvecs file
func readFVecsFile(filename string, bounds ...int) ([][]float32, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("error opening file: %w", err)
	}
	defer file.Close()

	// Read the dimension of the first vector
	var d int32
	err = binary.Read(file, binary.LittleEndian, &d)
	if err != nil {
		return nil, fmt.Errorf("error reading dimension: %w", err)
	}

	// Calculate size of each vector (including the dimension)
	vecSize := 4 * (1 + int(d)) // 4 bytes for each int32 and float32

	// Determine the bounds for reading
	a, b := 1, -1 // Default values
	if len(bounds) > 0 {
		b = bounds[0]
	}
	if len(bounds) > 1 {
		a = bounds[0]
		b = bounds[1]
	}

	// Move to the start position
	startPos := int64((a - 1) * vecSize)
	_, err = file.Seek(startPos, 0)
	if err != nil {
		return nil, fmt.Errorf("error seeking file: %w", err)
	}

	// Prepare to read the vectors
	var vectors [][]float32
	for i := a; i <= b || b == -1; i++ {
		// Read the dimension of current vector
		var currentDim int32
		err := binary.Read(file, binary.LittleEndian, &currentDim)
		if err != nil {
			if b == -1 && err == io.EOF {
				break // End of file is fine when b is not set
			}
			return nil, fmt.Errorf("error reading vector dimension: %w", err)
		}

		// Read the vector
		vec := make([]float32, currentDim)
		err = binary.Read(file, binary.LittleEndian, vec)
		if err != nil {
			return nil, fmt.Errorf("error reading vector: %w", err)
		}

		vectors = append(vectors, vec)
	}

	return vectors, nil
}

// readIVecsFile reads vectors from an .ivecs file
func readIVecsFile(filename string, bounds ...int) ([][]int32, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("error opening file: %w", err)
	}
	defer file.Close()

	// Read the dimension of the first vector
	var d int32
	err = binary.Read(file, binary.LittleEndian, &d)
	if err != nil {
		return nil, fmt.Errorf("error reading dimension: %w", err)
	}

	// Calculate size of each vector (including the dimension)
	vecSize := 4 * (d + 1) // 4 bytes for each int32

	// Determine the bounds for reading
	a, b := 1, -1 // Default values
	if len(bounds) > 0 {
		b = bounds[0]
	}
	if len(bounds) > 1 {
		a = bounds[0]
		b = bounds[1]
	}

	// Move to the start position
	startPos := int64((int32(a) - 1) * vecSize)
	_, err = file.Seek(startPos, 0)
	if err != nil {
		return nil, fmt.Errorf("error seeking file: %w", err)
	}

	// Prepare to read the vectors
	var vectors [][]int32
	for i := a; i <= b || b == -1; i++ {
		// Read the dimension of current vector
		var currentDim int32
		err := binary.Read(file, binary.LittleEndian, &currentDim)
		if err != nil {
			if b == -1 && err == io.EOF {
				break // End of file is fine when b is not set
			}
			return nil, fmt.Errorf("error reading vector dimension: %w", err)
		}

		// Read the vector
		vec := make([]int32, currentDim)
		err = binary.Read(file, binary.LittleEndian, &vec)
		if err != nil {
			return nil, fmt.Errorf("error reading vector: %w", err)
		}

		vectors = append(vectors, vec)
	}

	return vectors, nil
}
