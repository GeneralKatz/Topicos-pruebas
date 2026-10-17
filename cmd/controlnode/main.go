// Ejecutable del ControlNode (bloque C.6 de HITO2.md).
//
// Ensambla el plano de control —registro de membresía, namespace, colocación y
// gestor de cargas— y expone dos servidores: REST para el cliente y gRPC para
// recibir los heartbeats de los DataNodes.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/thomaszambrano/DFS-Topicos-Telematica/internal/controlnode"
	"github.com/thomaszambrano/DFS-Topicos-Telematica/internal/pb"
)

// config reúne la configuración del ControlNode (C.6.1).
type config struct {
	HTTPAddr string // API REST para el cliente (RX-04)
	GRPCAddr string // RPC para los heartbeats de los DataNodes (RX-03)

	// ChunkSize es el tamaño por omisión de los chunks. Es configurable porque
	// RT-01 lo exige, y porque bajarlo permite demostrar el reparto entre los 4
	// DataNodes sin necesitar un archivo de cientos de MB (evidencia E.2.3).
	ChunkSize int64

	// ReplicationFactor es R. En el Hito 2 vale 1 (sin replicación), pero se lee
	// del entorno desde ahora porque RX-07 exige que sea configurable.
	ReplicationFactor int

	// WriteQuorum es W: cuántas confirmaciones necesita un chunk para darlo por
	// escrito (RT-06). El valor base es 2. Se acota por los destinos asignados, así
	// que con R=1 sigue bastando una.
	WriteQuorum int

	// Umbrales de clasificación de membresía (RN-13). El estado intermedio
	// SOSPECHOSO evita que un nodo que reinicia rápido dispare re-replicación.
	SuspectAfter time.Duration
	DeadAfter    time.Duration

	// LeaseTTL es la vigencia del lease sobre la clave de un objeto durante una
	// carga. Si el cliente desaparece, el objeto se libera al vencer (RN-24).
	LeaseTTL time.Duration

	HeartbeatInterval time.Duration // ritmo que el ControlNode sugiere a los nodos
}

func loadConfig() (config, error) {
	cfg := config{
		HTTPAddr: envOr("HTTP_ADDR", ":8080"),
		GRPCAddr: envOr("GRPC_ADDR", ":9090"),
	}

	var err error
	if cfg.ChunkSize, err = envInt64("CHUNK_SIZE", 64*1024*1024); err != nil {
		return cfg, err
	}
	if cfg.ChunkSize <= 0 {
		return cfg, fmt.Errorf("CHUNK_SIZE debe ser positivo, recibí %d", cfg.ChunkSize)
	}
	if cfg.ReplicationFactor, err = envInt("REPLICATION_FACTOR", 1); err != nil {
		return cfg, err
	}
	if cfg.ReplicationFactor < 1 {
		return cfg, fmt.Errorf("REPLICATION_FACTOR debe ser >= 1, recibí %d", cfg.ReplicationFactor)
	}
	if cfg.WriteQuorum, err = envInt("WRITE_QUORUM", 2); err != nil {
		return cfg, err
	}
	if cfg.WriteQuorum < 1 {
		return cfg, fmt.Errorf("WRITE_QUORUM debe ser >= 1, recibí %d", cfg.WriteQuorum)
	}
	if cfg.SuspectAfter, err = envDuration("SUSPECT_AFTER", 9*time.Second); err != nil {
		return cfg, err
	}
	if cfg.DeadAfter, err = envDuration("DEAD_AFTER", 30*time.Second); err != nil {
		return cfg, err
	}
	if cfg.LeaseTTL, err = envDuration("LEASE_TTL", 2*time.Minute); err != nil {
		return cfg, err
	}
	if cfg.HeartbeatInterval, err = envDuration("HEARTBEAT_INTERVAL", 3*time.Second); err != nil {
		return cfg, err
	}
	return cfg, nil
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("configuración inválida: %v", err)
	}
	log.Printf("controlnode | http=%s | grpc=%s | chunk=%d MB | R=%d | W=%d",
		cfg.HTTPAddr, cfg.GRPCAddr, cfg.ChunkSize/(1024*1024), cfg.ReplicationFactor, cfg.WriteQuorum)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	registry := controlnode.NewRegistry(cfg.SuspectAfter, cfg.DeadAfter)
	namespace := controlnode.NewNamespace()
	placer := controlnode.NewPlacer(registry)
	allocator := controlnode.NewAllocator(namespace, placer, registry,
		cfg.ChunkSize, cfg.ReplicationFactor, cfg.WriteQuorum, cfg.LeaseTTL)
	server := controlnode.NewServer(namespace, registry, allocator, cfg.HeartbeatInterval)

	replicator := controlnode.NewReplicator(namespace, registry, allocator)
	server.AttachReplicator(replicator)
	go replicator.Run(ctx)

	httpSrv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           server.Routes(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	grpcSrv := grpc.NewServer()
	pb.RegisterDataNodeServiceServer(grpcSrv, server)

	// Los dos servidores corren en puertos y goroutines distintos: el REST atiende al
	// cliente (RX-04) y el gRPC recibe los heartbeats de los DataNodes (RX-03).
	go func() {
		log.Printf("REST escuchando en %s", cfg.HTTPAddr)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("servidor HTTP: %v", err)
		}
	}()
	go func() {
		listener, err := net.Listen("tcp", cfg.GRPCAddr)
		if err != nil {
			log.Fatalf("escuchando gRPC en %s: %v", cfg.GRPCAddr, err)
		}
		log.Printf("gRPC escuchando en %s", cfg.GRPCAddr)
		if err := grpcSrv.Serve(listener); err != nil {
			log.Printf("servidor gRPC terminó: %v", err)
		}
	}()

	<-ctx.Done()
	log.Print("controlnode apagando")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		log.Printf("apagado forzado del HTTP: %v", err)
	}
	grpcSrv.GracefulStop()
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) (int, error) {
	n, err := envInt64(key, int64(def))
	return int(n), err
}

func envInt64(key string, def int64) (int64, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s=%q no es un entero: %w", key, v, err)
	}
	return n, nil
}

func envDuration(key string, def time.Duration) (time.Duration, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("%s=%q no es una duración (ej. 30s): %w", key, v, err)
	}
	return d, nil
}
