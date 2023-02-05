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
		if !check(keyP) {
			fmt.Println("Ne mozete koristiti ovaj kljuc.  Molim vas unesite novi kljuc.")
			continue
		}
		keyP = HLL + USER + keyP
		value := app.ReadPath.Read(keyP)
		if value != nil {
			fmt.Println("Vec postoji HLL pod ovakvim imenom. Molim vas unesite novi kljuc.")
			continue
		}
		key = keyP
		break

	}

	var p int
	for {
		pP := app.ReadValue("Unesite P: ")
		number, err := checkInt(pP)
		if !err {
			fmt.Println("Niste uneli broj.")
		} else if HLLMIN <= number && number <= HLLMAX {
			p = number
			break
		} else {
			fmt.Println("P mora biti u opsegu od 4 do 16")
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
	fmt.Println("Vrednost kardinalonosti: ", value)
}
