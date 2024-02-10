package main

import (
	"encoding/hex"
	"fmt"
	"math"
	"math/rand"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

const (
	tableName          = "speedtest"
	vecLen             = 3
	numInserts         = 3000
	numVectorPerInsert = 1
)

// SpeedTest represents the structure of your table
type SpeedTest struct {
	ID         uint `gorm:"primaryKey"`
	OneKVector string
}

// ToDBBinary converts a float array to a binary representation
func ToDBBinary(value []float32) string {
	if value == nil {
		return ""
	}

	// Convert float32 array to byte array
	byteArray := make([]byte, 4*len(value))
	for i, v := range value {
		b := math.Float32bits(v)
		byteArray[i*4] = byte(b)
		byteArray[i*4+1] = byte(b >> 8)
		byteArray[i*4+2] = byte(b >> 16)
		byteArray[i*4+3] = byte(b >> 24)
	}

	return hex.EncodeToString(byteArray)
}

func main() {
	dsn := "root:111@tcp(127.0.0.1:6001)/a?charset=utf8mb4&parseTime=True&loc=Local"
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		panic("failed to connect database")
	}

	// Migrate the schema
	db.AutoMigrate(&SpeedTest{})

	start := time.Now()

	for i := 0; i < numInserts*numVectorPerInsert; i++ {
		arr := make([]float32, vecLen)
		for j := range arr {
			arr[j] = rand.Float32()
		}
		db.Create(&SpeedTest{ID: uint(i), OneKVector: ToDBBinary(arr)})
	}

	duration := time.Since(start)

	fmt.Printf("Result: vector dim=%d vectors "+
		"inserted=%d "+
		"insert/second=%f\n",
		vecLen, numInserts*numVectorPerInsert, float64(numInserts*numVectorPerInsert)/duration.Seconds())
}
