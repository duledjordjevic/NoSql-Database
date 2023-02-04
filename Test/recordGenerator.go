package tester

import (
	record "NAiSP/Structures/Record"
	"fmt"
	"math/rand"
	"time"
)

const charset = "abcdefghijklmnopqrstuvwxyz" +
	"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

var seededRand = rand.New(rand.NewSource(time.Now().UnixNano()))

func randomKey(length int, charset string) string {
	bytes := make([]byte, length)
	for i := range bytes {
		bytes[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(bytes)
}

func RandomValue(length int) *[]byte {
	bytes := make([]byte, length)
	_, err := rand.Read(bytes)
	if err != nil {
		fmt.Println("error:", err)
		return nil
	}
	if len(bytes) == 0 {
		bytes = []byte("Trajce legenda")
	}
	return &bytes
}

func RandomRecord() *record.Record {
	return record.NewRecordKeyValue(randomKey(6, charset), *RandomValue(10), 0)
}
