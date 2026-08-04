package main

import (
	"log"
	"net/http"
)

func main() {
	httpQ := NewHTTPQ()
	err := http.ListenAndServeTLS(":23411", "cert.pem", "key.pem", httpQ.Handler())
	if err != nil {
		log.Fatal(err)
	}
}
