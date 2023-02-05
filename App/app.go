package application

import (
	bloomfilter "NAiSP/Structures/Bloomfilter"
	configreader "NAiSP/Structures/ConfigReader"
	lru "NAiSP/Structures/LRUcache"
	lsm "NAiSP/Structures/LSM"
	memtable "NAiSP/Structures/Memtable"
	readpath "NAiSP/Structures/ReadPath"
	record "NAiSP/Structures/Record"
	sstable "NAiSP/Structures/Sstable"
	wal "NAiSP/Structures/WAL"
	writepath "NAiSP/Structures/WritePath"
	tester "NAiSP/Test"
	"bufio"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
)

const (
	BLOOMFILTER = "BF_"
	CMS         = "CMS_"
	HLL         = "HLL_"
	SH          = "SH_"
	USER        = "SRMND_"
	BLOOMDOWN   = 0
	BLOOMUP     = 1
	CMSEPSIOLON = 0.1
	CMSDELTA    = 0.9
	HLLMIN      = 4
	HLLMAX      = 16

	DIRECTORY = "./Data/Data"
)

type App struct {
	Config      *configreader.ConfigReader
	Memtable    *memtable.MemTable
	Bloomfilter *bloomfilter.BloomFilter
	Lru         *lru.LRUCache
	Wal         *wal.WAL
	WritePath   *writepath.WritePath
	ReadPath    *readpath.ReadPath
}

func CreateApp() *App {
	// Read configuration
	config := configreader.ConfigReader{}
	config.ReadConfig()
	filePath := "./Data/Data" + config.DataFileStructure + "/" + config.Compaction + "/"

	// Creat app
	app := App{}

	// Set atributes on app
	app.Config = &config
	var BF *bloomfilter.BloomFilter
	fileBloom, err := os.Open(filePath + "bloomfilter.gob")
	if err == nil {
		fileBloom.Close()
		BF = &bloomfilter.BloomFilter{}
		BF.Decode(filePath + "bloomfilter.gob")
	} else {
		BF = bloomfilter.NewBLoomFilter(1000, 0.1)
	}
	app.Bloomfilter = BF

	app.Memtable = memtable.CreateMemtable(float64(config.WalSize), config.MemtableTrashold, config.MemtableStructure)
	app.Wal = wal.NewWal()
	app.Lru = lru.NewLRUCache(uint(config.CacheCapacity))
	app.WritePath = &writepath.WritePath{
		Wal:         app.Wal,
		MemTable:    app.Memtable,
		BloomFilter: app.Bloomfilter,
		Config:      &config,
	}
	app.ReadPath = &readpath.ReadPath{
		MemTable:     app.Memtable,
		Lru:          app.Lru,
		BloomFilter:  app.Bloomfilter,
		ConfigReader: &config}

	return &app
}

func (app *App) End() {
	filePath := "./Data/Data" + app.Config.DataFileStructure + "/" + app.Config.Compaction + "/"
	app.WritePath.ExitFlush()
	app.Bloomfilter.Encode(filePath + "bloomfilter.gob")
	os.Exit(1)
}

func check(input string) bool {
	if strings.HasPrefix(input, BLOOMFILTER) {
		return false
	}
	if strings.HasPrefix(input, CMS) {
		return false
	}
	if strings.HasPrefix(input, HLL) {
		return false
	}
	if strings.HasPrefix(input, SH) {
		return false
	}
	if input == "" {
		return false
	}
	if strings.HasPrefix(input, "■") {
		return false
	}
	return true
}

func checkInt(input string) (int, bool) {
	number, err := strconv.Atoi(input)
	if err != nil {
		return 0, false
	}
	return number, true
}

func checkFloat(input string) (float64, bool) {
	number, err := strconv.ParseFloat(input, 64)
	if err != nil {
		return 0, false
	}
	return number, true
}

func (app *App) ReadValue(text string) string {
	var input string

	for {
		fmt.Print(text)
		n, err := fmt.Scan(&input)
		if err != nil {
			fmt.Println("Lose ste uneli komandu. Probajte ponovo.")
		} else if n == 0 || input == "" {
			fmt.Println("Lose ste uneli komandu. Probajte ponovo.")

		} else if !check(input) {

			fmt.Println("Lose ste uneli komandu. Probajte ponovo.")

		} else {
			break

		}
	}
	return input
}

func (app *App) ReadValueSimHash(text string) string {
	reader := bufio.NewReader(os.Stdin)
	fmt.Print(text)
	input, _ := reader.ReadString('\n')
	return input
}

func (app *App) Put() {
	key := app.ReadValue("Unesite kljuc koji zelite da dodate: ")
	value := tester.RandomValue(10)
	record := record.NewRecordKeyValue(key, *value, 0)
	app.WritePath.Write(record)
}

func (app *App) Get() {
	key := app.ReadValue("Unesite kljuc koji zelite da nadjete: ")
	value := app.ReadPath.Read(key)
	if value == nil {
		fmt.Println("Pretraga neuspesna. Kljuc ne postoji.")
		return
	}
	fmt.Println("Vrenost datog kluca je: ", value)
}

func (app *App) Delete() {
	key := app.ReadValue("Unesite kljuc koji zelite da izbrisete: ")
	value := tester.RandomValue(10)
	record := record.NewRecordKeyValue(key, *value, 1)
	app.WritePath.Write(record)
}

func (app *App) RangeScan() {
	filepath := "./Data/Data" + app.Config.DataFileStructure + "/" + app.Config.Compaction + "/"
	folder, err := ioutil.ReadDir(filepath + "Toc")
	if err != nil {
		fmt.Println("Greska kod citanja direktorijuma: ", err)
		log.Fatal(err)
	}

	// Biranje stranice
	fmt.Println("############## Ponudjene strane ##############")
	for i, files := range folder {
		fmt.Println(i, " - ", files.Name())
	}
	var num int
	for {
		list := app.ReadValue("Uneite broj stranice koju zelite: ")
		number, err := strconv.Atoi(list)
		if err != nil {
			fmt.Println("Uneli ste losu vrednost. Unesite ponovo.")
			continue
		} else if number < 0 || number > len(folder)-1 {
			fmt.Println("Uneli ste losu vrednost. Unesite ponovo.")
			continue
		} else {

			num = number
			break
		}
	}

	SStable := sstable.NewSStableFromTOC(filepath + "Toc/" + folder[num].Name())
	var key1 string
	var key2 string
	for {
		key1 = app.ReadValue("Unesite pocetni kljuc: ")
		key2 = app.ReadValue("Unesite krajnji kljuc: ")
		if key1 < key2 {
			break
		}
		fmt.Println("Lose se uneli kjuceve. Prvi kljuc mora biti manji od drugog.")
	}

	var size uint64
	for {
		sizeTest := app.ReadValue("Unesite broj elemenata koji zelite da dobijete: ")
		number, err := strconv.ParseUint(sizeTest, 10, 64)
		if err != nil {
			fmt.Println("Uneli ste losu vrednost. Unesite ponovo.")
			continue
		} else {

			size = number
			break
		}
	}

	var listRecords []record.Record
	if app.Config.DataFileStructure == "Multiple" {
		listRecords = SStable.SearchRangeMultiple(key1, key2, size)

	} else {
		listRecords = SStable.SearchRangeSingle(key1, key2, size)
	}
	if len(listRecords) == 0 {
		fmt.Println("Nema kljuceva u ovom opsegu")
		return
	}
	fmt.Println("Trazili ste ", size, " kljuceva, pronasli smo ", len(listRecords))
	fmt.Println("############## Pronadjeni kljucevi ##############")
	for i, record := range listRecords {
		fmt.Println(i+1, " - ", record.GetKey())
	}

}

func (app *App) List() {
	filepath := "./Data/Data" + app.Config.DataFileStructure + "/" + app.Config.Compaction + "/"
	folder, err := ioutil.ReadDir(filepath + "Toc")
	if err != nil {
		fmt.Println("Greska kod citanja direktorijuma: ", err)
		log.Fatal(err)
	}

	// Biranje stranice
	fmt.Println("############## Ponudjene strane ##############")
	for i, files := range folder {
		fmt.Println(i, " - ", files.Name())
	}
	var num int
	for {
		list := app.ReadValue("Uneite broj stranice koju zelite: ")
		number, err := strconv.Atoi(list)
		if err != nil {
			fmt.Println("Uneli ste losu vrednost. Unesite ponovo.")
			continue
		} else if number < 0 || number > len(folder)-1 {
			fmt.Println("Uneli ste losu vrednost. Unesite ponovo.")
			continue
		} else {

			num = number
			break
		}
	}
	var size uint64
	for {
		sizeTest := app.ReadValue("Unesite broj elemenata koji zelite da dobijete: ")
		number, err := strconv.ParseUint(sizeTest, 10, 64)
		if err != nil {
			fmt.Println("Uneli ste losu vrednost. Unesite ponovo.")
			continue
		} else {

			size = number
			break
		}
	}

	SStable := sstable.NewSStableFromTOC(filepath + "Toc/" + folder[num].Name())
	prefix := app.ReadValue("Unesite prefiks po kome zelite traziti: ")
	var listRecords []record.Record
	if app.Config.DataFileStructure == "Multiple" {
		listRecords = SStable.SearchPrefixMultiple(prefix, size)

	} else {
		listRecords = SStable.SearchPrefixSingle(prefix, size)
	}
	if len(listRecords) == 0 {
		fmt.Println("Nema kljuceva sa ovim prefiksom")
		return
	}
	fmt.Println("Trazili ste ", size, " kljuceva, pronasli smo ", len(listRecords))
	fmt.Println("############## Pronadjeni kljucevi ##############")
	for i, record := range listRecords {
		fmt.Println(i+1, " - ", record.GetKey())
	}
}

func (app *App) Compaction() {
	if app.Config.Compaction == "Leveled" {
		// Dodaj kad ovi napisu

	} else {
		lsm.SizeTiered(app.Config)
	}
}
