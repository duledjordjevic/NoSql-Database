package tester

import (
	configreader "NAiSP/Structures/ConfigReader"
	lsm "NAiSP/Structures/LSM"
	record "NAiSP/Structures/Record"
	writepath "NAiSP/Structures/WritePath"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"strings"
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
	return record.NewRecordKeyValue(randomKey(10, charset), *RandomValue(10), 0)
}

func ReadFile(filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		fmt.Println("Greska kod citanja -> Tester")
		return err
	}
	capacity := 0
	fmt.Println("	====== Pocetak fajla ======	")

	for {
		record, _ := record.ReadRecord(file)
		if record == nil {
			fmt.Println("Kapacitet -> ", capacity)
			break
		}
		capacity += int(record.GetSize())
		fmt.Println(record.String())
	}

	return nil
}

func ReadLevel(level int, directory string, config *configreader.ConfigReader) int {

	LSM := lsm.LSM{Config: config}

	counter := 0
	fileCounter := 0
	files, err := ioutil.ReadDir(directory)
	if err != nil {
		fmt.Println("Greska kod citanja direktorijuma: ", err)
		log.Fatal(err)
	}

	var fileToRead *os.File

	for _, file := range files {
		if strings.Contains(file.Name(), "data") && !strings.Contains(file.Name(), "Meta") && writepath.GetLevel(file.Name()) == level {
			filename := directory + "/" + file.Name()
			fileToRead = LSM.OpenData(filename)

			fmt.Println("Datoteka ", fileCounter, " -> ", file.Name())

			for {
				rec, _ := record.ReadRecord(fileToRead)
				if rec == nil {
					break
				}
				// fmt.Println(rec.String())
				counter++
			}
			fileCounter++
			fmt.Println("----------------------------------------")

		}
	}

	fmt.Println("Na nivou ", level, " procitano je ", counter, " record-a")
	// fmt.Println("Ocekivani broj record-a -> ")
	return counter
}
