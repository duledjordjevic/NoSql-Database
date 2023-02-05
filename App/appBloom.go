package application

import (
	record "NAiSP/Structures/Record"
	types "NAiSP/Structures/Types"
	"fmt"
)

func (app *App) AddBloom() {
	var expectedElements int
	for {
		expected := app.ReadValue("Unesite broj elemnata za koj zelite da koristite: ")
		number, err := checkInt(expected)
		if !err {
			fmt.Println("Lose ste uneli broj elemenata. Probajte Ponovo.")
		} else {
			expectedElements = number
			break
		}
	}

	var positiveRate float64
	for {
		positive := app.ReadValue("Unesite velicinu greske: ")
		number, err := checkFloat(positive)
		if !err {
			fmt.Println("Lose ste velicinu greske. Probajte Ponovo.")
		} else if number > BLOOMDOWN && number < BLOOMUP {
			fmt.Println("Velicina greske mora biti od 0 do 1.")
		} else {

			positiveRate = number
			break
		}

	}

	var key string
	for {
		keyP := app.ReadValue("Unesite kljuc po kojim ce se cuvati: ")
		keyP = BLOOMFILTER + USER + keyP
		value := app.ReadPath.Read(keyP)

		if value != nil {
			fmt.Println("Vec postoji Bloomfilter pod ovakvim imenom. Molim vas unesite novi kljuc.")
		} else {
			key = keyP
			break
		}

	}
	value := types.AddBloomFilter(expectedElements, positiveRate)
	record := record.NewRecordKeyValue(key, value, 0)
	app.WritePath.Write(record)

}
func (app *App) DeleteBloom() {

	key := app.ReadValue("Unesite kljuc po kojim se cuva: ")
	key = BLOOMFILTER + USER + key
	record := record.NewRecordKeyValue(key, []byte{0}, 1)
	app.WritePath.Write(record)

}

func (app *App) AddElementBloom() {

	key := app.ReadValue("Unesite kljuc BloomFiltera: ")
	key = BLOOMFILTER + USER + key
	value := app.ReadPath.Read(key)
	if value == nil {
		fmt.Println("Ne postoji ovaj BloomFilter.")
		return
	}

	elemnt := app.ReadValue("Unesite kljuc elementa kog zeliteda dodate: ")

	BF := types.AppendElementBloomFilter(elemnt, value)
	record := record.NewRecordKeyValue(key, BF, 0)
	app.WritePath.Write(record)
}

func (app *App) CheckElementBloom() {

	key := app.ReadValue("Unesite kljuc BloomFiltera: ")
	key = BLOOMFILTER + USER + key
	value := app.ReadPath.Read(key)
	if value == nil {
		fmt.Println("Lose ste uneli kljuc BloomFiltera ili ne postoji u bazi.")
		return
	}

	element := app.ReadValue("Unesite kljuc koji zelite da dodate: ")
	if types.CheckElementBloomFilter(element, value) {
		fmt.Println("Element je mozda u ovom BloomFilteru.")
	} else {
		fmt.Println("Element nije BloomFilterus.")
	}
}
