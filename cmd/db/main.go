package main

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/roman-mazur/architecture-practice-4-template/datastore"
)

var db *datastore.Db

func main() {
	var err error
	storagePath := filepath.Join(os.TempDir(), "db-data")
	err = os.MkdirAll(storagePath, 0o755)
	if err != nil {
		log.Fatalf("failed to create db storage dir: %v", err)
	}

	db, err = datastore.Open(storagePath)
	if err != nil {
		log.Fatalf("failed to open db: %v", err)
	}
	defer db.Close()

	http.HandleFunc("/db/", dbHandler)

	port := "8079"
	log.Printf("DB HTTP server listening on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func dbHandler(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/db/")
	if key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodGet:
		handleGet(key, w)
	case http.MethodPost:
		handlePost(key, w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func handleGet(key string, w http.ResponseWriter) {
	val, err := db.Get(key)
	if err != nil {
		if errors.Is(err, datastore.ErrNotFound) || errors.Is(err, datastore.ErrCorrupted) {
			http.NotFound(w, nil)
			return
		}
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	resp := map[string]string{
		"key":   key,
		"value": val,
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func handlePost(key string, w http.ResponseWriter, r *http.Request) {
	var body struct {
		Value string `json:"value"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	if err := db.Put(key, body.Value); err != nil {
		http.Error(w, "failed to store value", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
}
