// Ejecutable del DataNode (bloque B.5 de HITO2.md).
//
// Lee la configuración del entorno, abre el almacén local, expone la API HTTP de
// chunks y arranca el agente de heartbeat hacia el ControlNode.
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
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/thomaszambrano/DFS-Topicos-Telematica/internal/datanode"
	"github.com/thomaszambrano/DFS-Topicos-Telematica/internal/pb"
)

// config reúne la configuración del DataNode (B.5.1). Todo se lee del entorno
// para que el mismo binario sirva en Docker y en ejecución nativa.
type config struct {
	NodeID     string // identidad del nodo dentro del clúster
	DataDir    string // directorio local donde viven los chunks
	ListenAddr string // dirección en la que escucha su API HTTP

	// AdvertisedEndpoint es la dirección que el nodo reporta al ControlNode para
	// que los clientes lo alcancen. Se separa de ListenAddr porque la dirección
	// interna del contenedor no siempre es alcanzable desde fuera (RD-12).
	AdvertisedEndpoint string

	GRPCAddr     string
	GRPCEndpoint string

	ControlAddr       string        // ControlNode al que envía los heartbeats
	Capacity          int64         // capacidad declarada en bytes
	HeartbeatInterval time.Duration // cada cuánto reporta
}

func loadConfig() (config, error) {
	cfg := config{
		NodeID:             os.Getenv("NODE_ID"),
		DataDir:            envOr("DATA_DIR", "/data"),
		ListenAddr:         envOr("LISTEN_ADDR", ":8080"),
		AdvertisedEndpoint: os.Getenv("ADVERTISED_ENDPOINT"),
		ControlAddr:        os.Getenv("CONTROL_ADDR"),
		GRPCAddr:           envOr("GRPC_ADDR", ":9091"),
		GRPCEndpoint:       os.Getenv("GRPC_ENDPOINT"),
	}

	if cfg.NodeID == "" {
		return cfg, fmt.Errorf("NODE_ID es obligatorio")
	}
	if cfg.ControlAddr == "" {
		return cfg, fmt.Errorf("CONTROL_ADDR es obligatorio")
	}
	// Si nadie declara un endpoint anunciable, el nodo anuncia su propia
	// dirección de escucha. Sirve para correr nativo, no para Docker.
	if cfg.AdvertisedEndpoint == "" {
		cfg.AdvertisedEndpoint = cfg.ListenAddr
	}

	if cfg.GRPCEndpoint == "" {
		cfg.GRPCEndpoint = cfg.GRPCAddr
		if strings.HasPrefix(cfg.GRPCEndpoint, ":") {
			cfg.GRPCEndpoint = "localhost" + cfg.GRPCEndpoint
		}
	}

	var err error
	if cfg.Capacity, err = envInt64("CAPACITY", 10*1024*1024*1024); err != nil {
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
	log.Printf("datanode %s | data=%s | listen=%s | anuncia=%s | grpc=%s (%s) | control=%s | capacidad=%d MB",
		cfg.NodeID, cfg.DataDir, cfg.ListenAddr, cfg.AdvertisedEndpoint,
		cfg.GRPCAddr, cfg.GRPCEndpoint, cfg.ControlAddr, cfg.Capacity/(1024*1024))

	// signal.NotifyContext cancela el contexto al recibir SIGINT o SIGTERM, que
	// es lo que manda `docker stop`. Todo lo que arranque debe respetar este ctx
	// para lograr el apagado ordenado que pide B.5.3.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	store, err := datanode.NewStore(cfg.DataDir, cfg.Capacity)
	if err != nil {
		log.Fatalf("abriendo el almacén: %v", err)
	}
	used, capacity, count := store.Stats()
	log.Printf("datanode %s: %d chunks en disco, %d de %d bytes ocupados",
		cfg.NodeID, count, used, capacity)

	node := datanode.New(store, cfg.NodeID, cfg.AdvertisedEndpoint)
	node.GRPCEndpoint = cfg.GRPCEndpoint

	grpcSrv := grpc.NewServer()
	pb.RegisterChunkServiceServer(grpcSrv, datanode.NewChunkServer(node))

	srv := &http.Server{
		Addr:    cfg.ListenAddr,
		Handler: node.Routes(),
		// Sin timeout de escritura: un chunk de 64 MB por un enlace lento tarda, y
		// cortarlo a mitad produciría chunks truncados. El de lectura de cabeceras
		// sí se acota, que es donde un cliente malicioso podría colgar la conexión.
		ReadHeaderTimeout: 10 * time.Second,
	}

	// El servidor HTTP y el heartbeat corren en goroutines propias; main se queda
	// esperando la señal de apagado.
	go func() {
		log.Printf("datanode %s escuchando en %s", cfg.NodeID, cfg.ListenAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("servidor HTTP: %v", err)
		}
	}()
	go func() {
		listener, err := net.Listen("tcp", cfg.GRPCAddr)
		if err != nil {
			log.Fatalf("escuchando gRPC en %s: %v", cfg.GRPCAddr, err)
		}
		log.Printf("datanode %s gRPC escuchando en %s", cfg.NodeID, cfg.GRPCAddr)
		if err := grpcSrv.Serve(listener); err != nil {
			log.Printf("servidor gRPC terminó: %v", err)
		}
	}()
	go func() {
		if err := node.StartHeartbeat(ctx, cfg.ControlAddr, cfg.HeartbeatInterval); err != nil {
			log.Printf("heartbeat terminó: %v", err)
		}
	}()

	<-ctx.Done()
	log.Printf("datanode %s apagando", cfg.NodeID)

	// Apagado ordenado: se deja de aceptar conexiones nuevas y se esperan las que
	// están en curso, para no cortar una transferencia a mitad (B.5.3).
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("apagado forzado: %v", err)
	}
	grpcSrv.Stop()
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
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
		return 0, fmt.Errorf("%s=%q no es una duración (ej. 3s): %w", key, v, err)
	}
	return d, nil
}
