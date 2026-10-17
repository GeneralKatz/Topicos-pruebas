package datanode

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/thomaszambrano/DFS-Topicos-Telematica/internal/types"
)

// DataNode es el servicio completo del plano de datos: el almacén más su API.
type DataNode struct {
	Store    *Store
	NodeID   string
	Endpoint string // endpoint anunciable (RD-12)

	GRPCEndpoint string

	// inFlight cuenta las transferencias en curso. Viaja en el heartbeat para que
	// el ControlNode pondere la carga al elegir destinos (RD-10).
	inFlight atomic.Int64

	kick chan struct{}
}

// New construye el servicio del DataNode sobre un almacén ya abierto.
func New(store *Store, nodeID, endpoint string) *DataNode {
	return &DataNode{Store: store, NodeID: nodeID, Endpoint: endpoint, kick: make(chan struct{}, 1)}
}

// Routes registra las rutas de la API del DataNode.
//
// Cliente -> DataNode es HTTP con flujo, no gRPC: es el enlace por el que viajan
// los bytes, y el cliente es Python. Los enlaces entre nodos sí son gRPC (RI-02).
func (d *DataNode) Routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /chunk/{id}", d.handlePutChunk)
	mux.HandleFunc("GET /chunk/{id}", d.handleGetChunk)
	mux.HandleFunc("HEAD /chunk/{id}", d.handleGetChunk)
	mux.HandleFunc("GET /health", d.handleHealth)
	return mux
}

// handlePutChunk recibe un chunk por flujo y lo guarda.
//
// El cuerpo se pasa directo a Store.Put, que hace io.Copy hacia el archivo. No se
// usa io.ReadAll: con chunks de 64 MB y varias subidas concurrentes, leer el cuerpo
// completo en memoria agota la RAM del contenedor (RI-04).
func (d *DataNode) handlePutChunk(w http.ResponseWriter, r *http.Request) {
	d.inFlight.Add(1)
	defer d.inFlight.Add(-1)

	id := types.ChunkID(r.PathValue("id"))
	// El cliente declara el checksum que espera; el DataNode lo verifica antes de
	// aceptar el chunk, así un chunk corrupto en tránsito nunca llega a disco.
	expected := r.Header.Get("X-Chunk-Checksum")

	started := time.Now()
	n, sum, err := d.Store.Put(id, r.Body, expected)
	if err != nil {
		log.Printf("[%s] PUT %s falló: %v", d.NodeID, id, err)
		writeError(w, err)
		return
	}
	log.Printf("[%s] PUT %s ok: %d bytes en %s", d.NodeID, id, n, time.Since(started).Round(time.Millisecond))

	writeJSON(w, http.StatusCreated, map[string]any{
		"chunk_id": id,
		"size":     n,
		"checksum": sum,
		"node_id":  d.NodeID,
	})
}

// handleGetChunk sirve un chunk por flujo, con soporte de rangos.
//
// http.ServeContent interpreta la cabecera Range y responde 206 con solo el
// fragmento pedido, sin transferir el chunk completo (RD-09, base de RT-12).
func (d *DataNode) handleGetChunk(w http.ResponseWriter, r *http.Request) {
	d.inFlight.Add(1)
	defer d.inFlight.Add(-1)

	id := types.ChunkID(r.PathValue("id"))
	f, _, sum, err := d.Store.Open(id)
	if err != nil {
		log.Printf("[%s] GET %s falló: %v", d.NodeID, id, err)
		writeError(w, err)
		return
	}
	defer f.Close()

	// El checksum viaja en la respuesta para que el cliente verifique cada chunk
	// antes de reensamblar y, si no coincide, reintente contra otra réplica (RT-13).
	w.Header().Set("X-Chunk-Checksum", sum)
	w.Header().Set("Content-Type", "application/octet-stream")
	// ServeContent deduce el tamaño del propio descriptor y atiende la cabecera Range.
	http.ServeContent(w, r, string(id), time.Time{}, f)
}

// handleHealth expone espacio, carga y número de chunks (RD-10).
func (d *DataNode) handleHealth(w http.ResponseWriter, r *http.Request) {
	used, capacity, count := d.Store.Stats()
	writeJSON(w, http.StatusOK, map[string]any{
		"node_id":   d.NodeID,
		"endpoint":  d.Endpoint,
		"used":      used,
		"capacity":  capacity,
		"free":      max(capacity-used, 0),
		"chunks":    count,
		"in_flight": d.inFlight.Load(),
	})
}

// writeError traduce los errores del almacén a códigos HTTP, distinguiendo lo
// reintentable de lo que no lo es (RI-05).
func writeError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	switch {
	case errors.Is(err, ErrNotFound):
		status = http.StatusNotFound
	case errors.Is(err, ErrBadRequest):
		status = http.StatusBadRequest
	case errors.Is(err, ErrCorrupt):
		// 422: la petición era válida, el contenido no. Reintentar con los mismos
		// bytes no va a servir, así que el cliente no debe reintentar solo.
		status = http.StatusUnprocessableEntity
	case errors.Is(err, ErrNoSpace):
		status = http.StatusInsufficientStorage
	}
	writeJSON(w, status, map[string]any{
		"error":     http.StatusText(status),
		"detail":    err.Error(),
		"retryable": status >= 500,
	})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
