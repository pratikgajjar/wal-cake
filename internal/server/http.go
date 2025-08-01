package server

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/rs/zerolog/log"

	"git.famapp.in/fampay-inc/wal-cake/internal/replication"
)

// Server wraps the HTTP server and its dependencies.
type Server struct {
	httpServer *http.Server
	replicator replication.PGReplicator
}

// New creates a new HTTP server for health checks.
func New(port int, replicator replication.PGReplicator) *Server {
	mux := http.NewServeMux()

	server := &Server{
		replicator: replicator,
	}

	mux.HandleFunc("/live", server.livenessHandler)
	mux.HandleFunc("/ready", server.readinessHandler)

	server.httpServer = &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}

	return server
}

// Start runs the HTTP server in a separate goroutine.
func (s *Server) Start() {
	log.Info().Msgf("starting HTTP server on %s", s.httpServer.Addr)
	go func() {
		if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal().Err(err).Msg("HTTP server failed")
		}
	}()
}

// Shutdown gracefully shuts down the HTTP server.
func (s *Server) Shutdown(ctx context.Context) error {
	log.Info().Msg("shutting down HTTP server")
	return s.httpServer.Shutdown(ctx)
}

// livenessHandler handles the liveness probe.
func (s *Server) livenessHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	fmt.Fprintln(w, "ok")
}

// readinessHandler handles the readiness probe.
func (s *Server) readinessHandler(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	if err := s.replicator.HealthCheck(ctx); err != nil {
		log.Error().Err(err).Msg("readiness check failed")
		http.Error(w, "not ready", http.StatusServiceUnavailable)
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintln(w, "ok")
}
