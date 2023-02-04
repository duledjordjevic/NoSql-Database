package application

import "fmt"

// ADD SimHash to base
func (app *App) AddSH() {

	var key string
	for {
		keyP := app.ReadValue("Unesite kljuc po kojim ce se cuvati: ")
		keyP = SH + USER + keyP
		value := app.ReadPath.Read(key)
		if !check(key) {
			fmt.Println("Ne mozete koristiti ovaj kljuc.  Molim vas unesite novi kljuc.")
			continue
		}
		if value != nil {
			fmt.Println("Vec postoji SimHash pod ovakvim imenom. Molim vas unesite novi kljuc.")
			continue
		}
		key = keyP
		break

	}
	for {

	}

}
