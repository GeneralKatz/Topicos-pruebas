package controlnode

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/thomaszambrano/DFS-Topicos-Telematica/internal/pb"
	"github.com/thomaszambrano/DFS-Topicos-Telematica/internal/types"
)

// Server expone los dos planos del ControlNode: REST hacia el cliente y gRPC hacia
// los DataNodes.
//
// Los dos protocolos no son un capricho: cliente <-> ControlNode es una API REST bien
// definida (RX-04) y entre nodos es obligatoriamente RPC tipado (RX-03, RI-02).
type Server struct {
	pb.UnimplementedDataNodeServiceServer

	ns    *Namespace
	reg   *Registry
	alloc *Allocator
	rep   *Replicator

	heartbeatInterval time.Duration
}

// NewServer construye el servidor sobre los componentes del plano de control.
func NewServer(ns *Namespace, reg *Registry, alloc *Allocator, heartbeatInterval time.Duration) *Server {
	return &Server{ns: ns, reg: reg, alloc: alloc, heartbeatInterval: heartbeatInterval}
}

func (s *Server) AttachReplicator(rp *Replicator) {
	s.rep = rp
}

// Heartbeat atiende el reporte de un DataNode y devuelve sus órdenes (RN-12).
//
// Este es el ÚNICO punto de contacto entre el plano de control y el de datos, y lo
// inicia siempre el DataNode. El ControlNode no guarda ninguna dirección para
// llamarlo de vuelta: responde por la misma conexión que el nodo abrió.
func (s *Server) Heartbeat(ctx context.Context, report *pb.NodeReport) (*pb.CommandList, error) {
	s.reg.Update(report)
	var commands []*pb.Command
	if s.rep != nil {
		commands = s.rep.Drain(types.NodeID(report.GetNodeId()))
	}
	return &pb.CommandList{
		Commands:            commands,
		HeartbeatIntervalMs: s.heartbeatInterval.Milliseconds(),
	}, nil
}

// Routes registra la API REST versionada por prefijo de ruta (RI-06).
func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()

	// Escritura: open -> allocate (N veces) -> commit. El commit va al final y es
	// la única operación que publica algo.
	mux.HandleFunc("POST /v1/obj/open", s.handleOpenUpload)
	mux.HandleFunc("POST /v1/obj/{upload_id}/chunk", s.handleAllocateChunk)
	mux.HandleFunc("POST /v1/obj/{upload_id}/commit", s.handleCommit)
	mux.HandleFunc("DELETE /v1/obj/{upload_id}", s.handleAbort)

	// Lectura: el manifiesto es todo lo que el cliente necesita.
	mux.HandleFunc("GET /v1/obj/manifest", s.handleManifest)

	// Namespace.
	mux.HandleFunc("GET /v1/ns", s.handleList)
	mux.HandleFunc("GET /v1/ns/stat", s.handleStat)
	mux.HandleFunc("POST /v1/ns/dir", s.handleMkdir)
	mux.HandleFunc("DELETE /v1/ns", s.handleRemove)
	mux.HandleFunc("POST /v1/ns/rename", s.handleRename)

	// Administración y diagnóstico (RC-13).
	mux.HandleFunc("GET /v1/nodes", s.handleNodes)
	mux.HandleFunc("GET /v1/df", s.handleDF)
	mux.HandleFunc("PUT /v1/config/replication", s.handleSetReplication)
	mux.HandleFunc("GET /health", s.handleHealth)

	return logging(mux)
}

// logging registra cada petición con su código y duración.
func logging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		log.Printf("%s %s%s -> %d (%s)", r.Method, r.URL.Path, querySuffix(r),
			rec.status, time.Since(started).Round(time.Millisecond))
	})
}

func querySuffix(r *http.Request) string {
	if r.URL.RawQuery == "" {
		return ""
	}
	return "?" + r.URL.RawQuery
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

// userOf identifica al solicitante.
//
// LIMITACIÓN DECLARADA DEL HITO 2: la identidad viaja en una cabecera y no se
// verifica. La autenticación con Argon2id, JWT y ACL es del Hito 6 (RQ-15 a RQ-18).
// La cabecera existe desde ahora para que el campo Owner del namespace se llene con
// algo real y el modelo de propiedad no haya que retrofitearlo después.
func userOf(r *http.Request) string {
	if u := r.Header.Get("X-DFSha-User"); u != "" {
		return u
	}
	return "anonymous"
}

// ---------- Escritura ----------

type openUploadRequest struct {
	Path string `json:"path"`
	Size int64  `json:"size"`
}

func (s *Server) handleOpenUpload(w http.ResponseWriter, r *http.Request) {
	var req openUploadRequest
	if !decode(w, r, &req) {
		return
	}
	up, err := s.alloc.OpenUpload(req.Path, userOf(r), req.Size, r.Header.Get("Idempotency-Key"))
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{
		"upload_id":       up.ID,
		"version_id":      up.VersionID,
		"path":            up.Path,
		"chunk_size":      up.ChunkSize,
		"expected_chunks": up.Expected,
		"expires_at":      up.ExpiresAt,
	})
}

type allocateChunkRequest struct {
	Index    uint32 `json:"index"`
	Size     int64  `json:"size"`
	Checksum string `json:"checksum"`
}

func (s *Server) handleAllocateChunk(w http.ResponseWriter, r *http.Request) {
	var req allocateChunkRequest
	if !decode(w, r, &req) {
		return
	}
	plan, err := s.alloc.AllocateChunk(r.PathValue("upload_id"), req.Index, req.Size, req.Checksum)
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, plan)
}

type commitRequest struct {
	SHA256    string              `json:"sha256"`
	Confirmed map[string][]string `json:"confirmed"`
}

func (s *Server) handleCommit(w http.ResponseWriter, r *http.Request) {
	var req commitRequest
	if !decode(w, r, &req) {
		return
	}

	var confirmed map[types.ChunkID][]types.NodeID
	if req.Confirmed != nil {
		confirmed = make(map[types.ChunkID][]types.NodeID, len(req.Confirmed))
		for chunk, nodes := range req.Confirmed {
			for _, n := range nodes {
				confirmed[types.ChunkID(chunk)] = append(confirmed[types.ChunkID(chunk)], types.NodeID(n))
			}
		}
	}
	version, err := s.alloc.Commit(r.PathValue("upload_id"), req.SHA256, confirmed)
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"version_id": version.ID,
		"size":       version.Size,
		"sha256":     version.SHA256,
		"chunks":     len(version.Chunks),
		"created_at": version.CreatedAt,
	})
}

func (s *Server) handleAbort(w http.ResponseWriter, r *http.Request) {
	orphans, err := s.alloc.Abort(r.PathValue("upload_id"))
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"orphan_chunks": len(orphans)})
}

// ---------- Lectura ----------

func (s *Server) handleManifest(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Query().Get("path")
	if path == "" {
		fail(w, ErrBadRequest)
		return
	}
	m, err := s.alloc.Manifest(path)
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, m)
}

// ---------- Namespace ----------

// entry es la proyección de un Inode hacia el contrato REST.
//
// Es un tipo aparte y no el Inode directo porque el Inode guarda punteros al árbol y
// a la versión vigente: serializarlo tal cual arrastraría medio namespace en cada ls.
type entry struct {
	Name       string          `json:"name"`
	Kind       types.InodeKind `json:"kind"`
	Owner      string          `json:"owner"`
	Mode       uint32          `json:"mode"`
	Size       int64           `json:"size"`
	Chunks     int             `json:"chunks"`
	VersionID  types.VersionID `json:"version_id,omitempty"`
	SHA256     string          `json:"sha256,omitempty"`
	CreatedAt  time.Time       `json:"created_at"`
	ModifiedAt time.Time       `json:"modified_at"`
}

func toEntry(i *types.Inode) entry {
	e := entry{
		Name:       i.Name,
		Kind:       i.Kind,
		Owner:      i.Owner,
		Mode:       i.Mode,
		Size:       i.Size(),
		CreatedAt:  i.CreatedAt,
		ModifiedAt: i.ModifiedAt,
	}
	if i.Current != nil {
		e.Chunks = len(i.Current.Chunks)
		e.VersionID = i.Current.ID
		e.SHA256 = i.Current.SHA256
	}
	return e
}

func (s *Server) handleList(w http.ResponseWriter, r *http.Request) {
	path := queryPath(r)
	children, err := s.ns.List(path)
	if err != nil {
		fail(w, err)
		return
	}
	entries := make([]entry, 0, len(children))
	for _, c := range children {
		entries = append(entries, toEntry(c))
	}
	writeJSON(w, http.StatusOK, map[string]any{"path": path, "entries": entries})
}

// handleStat devuelve tamaño, versión vigente, número de chunks y las réplicas
// reales de cada chunk (RC-06). Es la forma de verificar en vivo cómo quedó
// distribuido un archivo entre los DataNodes.
func (s *Server) handleStat(w http.ResponseWriter, r *http.Request) {
	path := queryPath(r)
	inode, err := s.ns.Stat(path)
	if err != nil {
		fail(w, err)
		return
	}
	body := map[string]any{"path": path, "entry": toEntry(inode)}
	if inode.Current != nil {
		placement := make([]map[string]any, 0, len(inode.Current.Chunks))
		for _, c := range inode.Current.Chunks {
			placement = append(placement, map[string]any{
				"index":    c.Index,
				"chunk_id": c.ID,
				"size":     c.Size,
				"checksum": c.Checksum,
				// Las réplicas vienen del registro, no de los metadatos guardados:
				// son las que los DataNodes reportan tener AHORA (estado blando).
				"replicas": s.reg.Locations(c.ID),
			})
		}
		body["placement"] = placement
	}
	writeJSON(w, http.StatusOK, body)
}

type mkdirRequest struct {
	Path string `json:"path"`
}

func (s *Server) handleMkdir(w http.ResponseWriter, r *http.Request) {
	var req mkdirRequest
	if !decode(w, r, &req) {
		return
	}
	inode, err := s.ns.Mkdir(req.Path, userOf(r))
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, toEntry(inode))
}

func (s *Server) handleRemove(w http.ResponseWriter, r *http.Request) {
	path := queryPath(r)
	orphans, err := s.ns.Remove(path)
	if err != nil {
		fail(w, err)
		return
	}
	// Los chunks quedan marcados para recolección; el recolector es del Hito 3 (RN-07).
	writeJSON(w, http.StatusOK, map[string]any{"path": path, "orphan_chunks": len(orphans)})
}

type renameRequest struct {
	From string `json:"from"`
	To   string `json:"to"`
}

func (s *Server) handleRename(w http.ResponseWriter, r *http.Request) {
	var req renameRequest
	if !decode(w, r, &req) {
		return
	}
	if err := s.ns.Rename(req.From, req.To); err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"from": req.From, "to": req.To})
}

// ---------- Diagnóstico ----------

func (s *Server) handleNodes(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"nodes": s.reg.All()})
}

func (s *Server) handleDF(w http.ResponseWriter, r *http.Request) {
	capacity, used, alive, chunks := s.reg.Totals()
	writeJSON(w, http.StatusOK, map[string]any{
		"capacity":           capacity,
		"used":               used,
		"free":               max(capacity-used, 0),
		"alive_nodes":        alive,
		"known_chunks":       chunks,
		"chunk_size":         s.alloc.ChunkSize(),
		"replication_factor": s.alloc.ReplicationFactor(),
	})
}

type replicationRequest struct {
	ReplicationFactor int `json:"replication_factor"`
}

func (s *Server) handleSetReplication(w http.ResponseWriter, r *http.Request) {
	var req replicationRequest
	if !decode(w, r, &req) {
		return
	}
	if err := s.alloc.SetReplicationFactor(req.ReplicationFactor); err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"replication_factor": s.alloc.ReplicationFactor()})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	_, _, alive, _ := s.reg.Totals()
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "alive_nodes": alive})
}

// ---------- Utilidades ----------

func queryPath(r *http.Request) string {
	if p := r.URL.Query().Get("path"); p != "" {
		return p
	}
	return "/"
}

func decode(w http.ResponseWriter, r *http.Request, dst any) bool {
	if err := json.NewDecoder(r.Body).Decode(dst); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"error":     "Bad Request",
			"detail":    "cuerpo JSON inválido: " + err.Error(),
			"retryable": false,
		})
		return false
	}
	return true
}

// fail responde con el código y la marca de reintentable del catálogo (RI-05).
func fail(w http.ResponseWriter, err error) {
	status := HTTPStatus(err)
	writeJSON(w, status, map[string]any{
		"error":     http.StatusText(status),
		"detail":    err.Error(),
		"retryable": Retryable(err),
	})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
