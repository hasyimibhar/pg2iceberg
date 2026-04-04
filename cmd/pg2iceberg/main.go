package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/pg2iceberg/pg2iceberg/config"
	"github.com/pg2iceberg/pg2iceberg/pipeline"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	configPath := flag.String("config", "config.example.yaml", "path to config file")

	flag.Parse()

	if os.Getenv("PPROF") != "" {
		go func() {
			log.Println("pprof listening on :6060")
			http.ListenAndServe(":6060", nil)
		}()
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle shutdown signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		log.Printf("received %s, shutting down...", sig)
		cancel()
	}()

	err := runSingle(ctx, *configPath)
	if err != nil {
		log.Printf("fatal: %v", err)
		os.Exit(1)
	}
}

func runSingle(ctx context.Context, configPath string) error {
	cfg, err := config.Load(configPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	p, err := pipeline.BuildPipeline(ctx, "default", cfg)
	if err != nil {
		return fmt.Errorf("build pipeline: %w", err)
	}
	if err := p.Start(ctx); err != nil {
		return err
	}

	// Start a lightweight metrics server.
	metricsAddr := cfg.MetricsAddr
	if metricsAddr == "" {
		metricsAddr = ":9090"
	}
	startMetricsServer(ctx, metricsAddr, p)

	<-p.Done()
	if status, err := p.Status(); status == pipeline.StatusError && err != nil {
		return err
	}
	return nil
}

func startMetricsServer(ctx context.Context, addr string, p *pipeline.Pipeline) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		status, _ := p.Status()
		if status == pipeline.StatusError {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(p.Metrics())
	})

	srv := &http.Server{Addr: addr, Handler: mux}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(shutdownCtx)
	}()

	go func() {
		log.Printf("[metrics] listening on %s", addr)
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			log.Printf("[metrics] server error: %v", err)
		}
	}()
}

