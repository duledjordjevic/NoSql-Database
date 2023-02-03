package app

import (
	menu "NAiSP/Menu"
	bloomfilter "NAiSP/Structures/Bloomfilter"
	configreader "NAiSP/Structures/ConfigReader"
	lru "NAiSP/Structures/LRUcache"
	lsm "NAiSP/Structures/LSM"
	memtable "NAiSP/Structures/Memtable"
	readpath "NAiSP/Structures/ReadPath"
	record "NAiSP/Structures/Record"
	wal "NAiSP/Structures/WAL"
	writepath "NAiSP/Structures/WritePath"
	tester "NAiSP/Test"
	"fmt"
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
	BF := bloomfilter.BloomFilter{}
	BF.Decode(filePath + "bloomfilter.gob")
	app.Bloomfilter = &BF

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
	app.Bloomfilter.Encode(filePath + "bloomfilter.gob")
}

func (app *App) Start() {
	for {
		menu.PrintMenu()
		input := menu.ReadValue("Unesite komandu: ")
		if input == "1" {
			app.Put()
		} else if input == "2" {
			app.Delete()
		} else if input == "3" {
			app.Get()
		} else if input == "4" {
			app.Compaction()
		} else if input == "5" {

		} else if input == "X" {
			break
		} else {
			fmt.Println("Lose ste uneli komandu. Probajte ponovo.")
		}

	}
	app.End()
}

func (app *App) Put() {
	key := menu.ReadValue("Unesite kljuc koji zelite da dodate: ")
	value := tester.RandomValue(10)
	record := record.NewRecordKeyValue(key, *value, 0)
	app.WritePath.Write(record)
}

func (app *App) Get() {
	key := menu.ReadValue("Unesite kljuc koji zelite da nadjete: ")
	value := app.ReadPath.Read(key)
	if value == nil {
		fmt.Println("Pretraga neuspesna. Kljuc ne postoji.")
	}
	fmt.Println("Vrenost datog kluca je: ", value)
}

func (app *App) Delete() {
	key := menu.ReadValue("Unesite kljuc koji zelite da izbrisete: ")
	value := tester.RandomValue(10)
	record := record.NewRecordKeyValue(key, *value, 1)
	app.WritePath.Write(record)
}

func (app *App) RangeScan() {
	// TODO
}
func (app *App) List() {
	// TODO
}
func (app *App) Compaction() {
	if app.Config.Compaction == "Leveled" {
		// Dodaj kad ovi napisu
		lsm.Leveled(nil)
	} else {
		lsm.SizeTiered(app.Config)
	}
}

func (app *App) SearchMenu() {
	for {
		menu.PrintSearchMenu()
		input := menu.ReadValue("Unesite komandu: ")
		if input == "1" {
			// app.Put()
		} else if input == "2" {
			// app.Delete()
		} else if input == "3" {
			// app.Get()
		} else if input == "4" {
			return
		} else if input == "X" {
			break
		} else {
			fmt.Println("Lose ste uneli komandu. Probajte ponovo.")
		}

	}
	app.End()
}

func (app *App) OtherFunctionalities() {
	for {
		menu.PrintSearchMenu()
		input := menu.ReadValue("Unesite komandu: ")
		if input == "1" {
			// app.Put()
		} else if input == "2" {
			// app.Delete()
		} else if input == "3" {
			// app.Get()
		} else if input == "4" {
			return
		} else if input == "4" {
			return
		} else if input == "X" {
			break
		} else {
			fmt.Println("Lose ste uneli komandu. Probajte ponovo.")
		}

	}
	app.End()
}
