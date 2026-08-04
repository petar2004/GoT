package main

import (
	"log"
	"net/http"
)

func main() {
	httpQ := NewHTTPQ()
	err := http.ListenAndServeTLS(":23411", "server.crt", "server.key", httpQ.Handler())
	if err != nil {
		log.Fatal(err)
	}
}
