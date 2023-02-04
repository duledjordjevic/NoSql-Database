package application

import (
	record "NAiSP/Structures/Record"
	types "NAiSP/Structures/Types"
	"fmt"
)

func (app *App) AddHll() {

	var key string
	for {
		keyP := app.ReadValue("Unesite kljuc pod koji zelite da se cuva HLL: ")
		keyP = HLL + USER + keyP
		record := app.ReadPath.Read(keyP)
		if record == nil {
			key = keyP
			break
		}

		fmt.Println("Vec postoji ovaj HLL. Probajte Ponovo.")
	}

	var p int
	for {
		pP := app.ReadValue("Unesite P: ")
		number, err := checkInt(pP)
		if !err {
			fmt.Println("Niste uneli broj.")
		} else if HLLMIN >= number && number <= HLLMAX {
			fmt.Println("P mora biti u opsegu od 4 do 16")
		} else {
			p = number
			break
		}
	}
	value := types.AddHLL(uint8(p))
	record := record.NewRecordKeyValue(key, value, 0)
	app.WritePath.Write(record)

}

func (app *App) DeleteHLL() {
	key := app.ReadValue("Unesite kljuc HLL-a kojeg zelite izbrisati: ")
	key = HLL + USER + key
	record := record.NewRecordKeyValue(key, []byte{0}, 1)
	app.WritePath.Write(record)
}

func (app *App) AddElementHLL() {
	key := app.ReadValue("Unesite kljuc HLL-a kome zelite doddati elemnt: ")
	key = HLL + USER + key
	valueHll := app.ReadPath.Read(key)
	if valueHll == nil {
		fmt.Println("Ne postoji ovaj HLL.")
		return
	}
	element := app.ReadValue("Unesite elemnt koji zelite da dodate: ")
	value := types.AppendElementHLL(element, valueHll)
	record := record.NewRecordKeyValue(key, value, 0)
	app.WritePath.Write(record)

}

func (app *App) CheckCardHLL() {
	key := app.ReadValue("Unesite kljuc HLL-a ciju kardinalonst zelite: ")
	key = HLL + USER + key
	valueHll := app.ReadPath.Read(key)
	if valueHll == nil {
		fmt.Println("Ne postoji ovaj HLL.")
		return
	}
	value := types.CheckCardinalityHLL(valueHll)
	fmt.Println("Vrednos kardinalonosti: ", value)
}
