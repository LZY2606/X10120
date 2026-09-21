package main

import (
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"incident-timeline/internal/api"
	"incident-timeline/internal/store"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:5210", "listen address")
	dataDir := flag.String("data", "data", "directory for the append-only audit stream")
	flag.Parse()

	st, err := store.Open(*dataDir)
	if err != nil {
		log.Fatalf("open store: %v", err)
	}
	defer st.Close()

	srv := &http.Server{
		Addr:              *addr,
		Handler:           api.New(st).Mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	go func() {
		log.Printf("incident causal timeline listening on http://%s (data: %s)", *addr, *dataDir)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server: %v", err)
		}
	}()
	<-stop
	log.Println("shutting down")
	if err := srv.Close(); err != nil {
		log.Printf("close: %v", err)
	}
}
