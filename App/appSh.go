package application

import (
	record "NAiSP/Structures/Record"
	types "NAiSP/Structures/Types"
	"fmt"
)

// ADD SimHash to base
func (app *App) AddSH() {

	var key string
	for {
		keyP := app.ReadValue("Unesite kljuc pod kojim ce se cuvati SimHash: ")
		if !check(keyP) {
			fmt.Println("Ne mozete koristiti ovaj kljuc.  Molim vas unesite novi kljuc.")
			continue
		}
		keyP = SH + USER + keyP
		value := app.ReadPath.Read(keyP)
		if value != nil {
			fmt.Println("Vec postoji SimHash pod ovakvim imenom. Molim vas unesite novi kljuc.")
			continue
		}
		key = keyP
		break

	}
	simvalue := app.ReadValueSimHash("Unesite sadrazaj za SimHash: ")
	value := types.AddSimHash(simvalue)
	record := record.NewRecordKeyValue(key, value, 0)
	app.WritePath.Write(record)

}

// Delete SimHash
func (app *App) DeleteSH() {
	key := app.ReadValue("Unesite kljuc SimHash-a kojeg zelite izbrisati: ")
	key = SH + USER + key
	record := record.NewRecordKeyValue(key, []byte{0}, 1)
	app.WritePath.Write(record)
}

// Check two list
func (app *App) CheckSH() {
	key := app.ReadValue("Unesite kljuc po kojim se cuva SimHash: ")
	key = SH + USER + key
	value := app.ReadPath.Read(key)
	if value == nil {
		fmt.Println("Ne postoji SimHash sa ovim kljucem")
		return
	}
	simvalue := app.ReadValueSimHash("Unesite sadrazaj sa kojim zelite porediti: ")
	fmt.Println("Vrednost posle poredjenja: ", types.CompareSimHash(simvalue, value))
}
