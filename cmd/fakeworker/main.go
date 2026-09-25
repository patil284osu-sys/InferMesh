package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"
)

func main() {
	port := flag.String("port", "9001", "listen port")
	name := flag.String("name", "fake-1", "worker name")
	delay := flag.Duration("delay", 50*time.Millisecond, "delay per event")
	flag.Parse()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /ready", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("POST /predict", func(w http.ResponseWriter, r *http.Request) {
		var input struct {
			Image string `json:"image"`
		}
		if json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&input) != nil || input.Image == "" {
			http.Error(w, "invalid image", 400)
			return
		}
		select {
		case <-time.After(*delay):
		case <-r.Context().Done():
			return
		}
		json.NewEncoder(w).Encode(map[string]string{"label": "demo", "worker": *name})
	})
	mux.HandleFunc("POST /generate", func(w http.ResponseWriter, r *http.Request) {
		var input struct {
			Prompt    string `json:"prompt"`
			MaxTokens int    `json:"max_tokens"`
		}
		if json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&input) != nil || input.Prompt == "" || input.MaxTokens < 1 {
			http.Error(w, "invalid prompt", 400)
			return
		}
		w.Header().Set("Content-Type", "application/x-ndjson")
		for i, word := range strings.Fields(input.Prompt) {
			if i >= input.MaxTokens {
				break
			}
			select {
			case <-time.After(*delay):
			case <-r.Context().Done():
				return
			}
			fmt.Fprintf(w, "{\"token\":%q,\"worker\":%q}\n", word, *name)
			w.(http.Flusher).Flush()
		}
		fmt.Fprint(w, "{\"done\":true}\n")
		w.(http.Flusher).Flush()
	})
	log.Fatal(http.ListenAndServe("127.0.0.1:"+*port, mux))
}
