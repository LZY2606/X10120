package main

import (
	"flag"
	"log"
	"net/http"

	"incident-timeline/internal/server"
	"incident-timeline/internal/store"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:5210", "listen address")
	dataDir := flag.String("data", "./data", "directory for the append-only audit log")
	flag.Parse()

	st, err := store.Open(*dataDir)
	if err != nil {
		log.Fatalf("open store: %v", err)
	}
	defer st.Close()

	srv := &http.Server{Addr: *addr, Handler: server.New(st)}
	log.Printf("事故因果时间线 workbench listening on http://%s (data: %s)", *addr, *dataDir)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
