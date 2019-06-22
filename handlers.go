package main

import (
	"fmt"
	"log"
	"net/http"
)

func getVersionHanlder(w http.ResponseWriter, r *http.Request) {
	log.Println("[/version:GET] Requested api version. " + appVersion)

	w.Header().Set("Content-Type", "application/json")
	fmt.Fprint(w, fmt.Sprintf(`{"service": "%s", "version": "%s"}`, appName, appVersion))
}

func getStatusHanlder(w http.ResponseWriter, r *http.Request) {
	log.Println("[/:GET] Requested service status.")

	w.Header().Set("Content-Type", "application/json")
	fmt.Fprint(w, fmt.Sprintf(`{"service": "%s", "status":"running", version": "%s"}`, appName, appVersion))
}
