package main

import (
	application "NAiSP/App"
	menu "NAiSP/Menu"
)


func main() {
	app := application.CreateApp()
	menu.Start(app)
}
