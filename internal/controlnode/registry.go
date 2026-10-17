// Package controlnode implementa el plano de control: namespace, colocación,
// membresía y gestión de cargas. Los datos nunca lo atraviesan (RQ-13).
package controlnode

import (
	"sort"
	"sync"
	"time"

	"github.com/thomaszambrano/DFS-Topicos-Telematica/internal/pb"
	"github.com/thomaszambrano/DFS-Topicos-Telematica/internal/types"
)

// NodeStatus clasifica a un DataNode según cuándo reportó por última vez (RN-13).
type NodeStatus string

const (
	StatusAlive   NodeStatus = "alive"
	StatusSuspect NodeStatus = "suspect"
	StatusDead    NodeStatus = "dead"
)

// NodeState es la vista que el ControlNode tiene de un DataNode.
// Todo su contenido proviene de los heartbeats: es estado blando completo.
type NodeState struct {
	ID           types.NodeID `json:"node_id"`
	Endpoint     string       `json:"endpoint"`
	GRPCEndpoint string       `json:"grpc_endpoint"`
	Capacity     int64        `json:"capacity"`
	Used         int64        `json:"used"`
	InFlight     int64        `json:"in_flight"`
	Chunks       int          `json:"chunks"`
	Draining     bool         `json:"draining"`
	LastSeen     time.Time    `json:"last_seen"`
	Status       NodeStatus   `json:"status"`

	// inventory es el conjunto de chunks que este nodo reportó en su último
	// heartbeat. Se guarda para poder retirar sus ubicaciones antiguas cuando
	// llegue un reporte nuevo, sin recorrer el mapa global de ubicaciones.
	inventory map[types.ChunkID]struct{}
}

// Free devuelve el espacio libre declarado por el nodo.
func (n *NodeState) Free() int64 {
	if f := n.Capacity - n.Used; f > 0 {
		return f
	}
	return 0
}

// Utilization devuelve la fracción de ocupación entre 0 y 1.
func (n *NodeState) Utilization() float64 {
	if n.Capacity <= 0 {
		return 1
	}
	return float64(n.Used) / float64(n.Capacity)
}

// Registry mantiene la membresía del clúster y el mapa chunk -> ubicaciones.
//
// TODO EL CONTENIDO DE ESTE REGISTRO ES ESTADO BLANDO: nada de esto se persiste.
// Se reconstruye entero a partir de los inventarios que los DataNodes reportan,
// porque el disco de cada DataNode es el único que sabe con certeza qué tiene
// ahora mismo. Persistirlo obligaría a elegir entre creer una foto vieja —y
// entregarle a un cliente la dirección de un chunk que ya no existe— o verificarla
// preguntando, en cuyo caso guardarla no sirvió de nada. Es la misma decisión de
// GFS y de HDFS (ver D-002 en DECISIONES.md).
type Registry struct {
	mu        sync.RWMutex
	nodes     map[types.NodeID]*NodeState
	locations map[types.ChunkID]map[types.NodeID]struct{}

	// expected son ubicaciones ASUMIDAS, con fecha de caducidad: los destinos a los
	// que el ControlNode asignó un chunk y que el cliente confirmó haber escrito al
	// hacer commit, pero que todavía no han aparecido en ningún inventario.
	//
	// Existen porque sin ellas se rompe RQ-09: entre el commit y el siguiente
	// heartbeat —hasta 3 segundos— el ControlNode no sabría dónde están los chunks
	// que acaba de asignar, y una lectura inmediata después de una escritura
	// confirmada fallaría con 503.
	//
	// NO contradicen que las ubicaciones sean estado blando: son una suposición
	// optimista que caduca sola y que el inventario del nodo reemplaza en cuanto
	// llega. La autoridad sigue siendo el disco del DataNode; esto solo cubre la
	// ventana en la que todavía no ha hablado. GFS hace lo mismo: el maestro sabe a
	// qué chunkservers ordenó escribir, y los reportes periódicos lo corrigen.
	expected    map[types.ChunkID]map[types.NodeID]time.Time
	expectedTTL time.Duration

	suspectAfter time.Duration
	deadAfter    time.Duration
}

// NewRegistry crea el registro con los umbrales de clasificación de RN-13.
func NewRegistry(suspectAfter, deadAfter time.Duration) *Registry {
	return &Registry{
		nodes:     make(map[types.NodeID]*NodeState),
		locations: make(map[types.ChunkID]map[types.NodeID]struct{}),
		expected:  make(map[types.ChunkID]map[types.NodeID]time.Time),
		// Una suposición vive lo que tarda el nodo en pasar a sospechoso: si en ese
		// plazo no confirmó el chunk en su inventario, se descarta.
		expectedTTL:  suspectAfter,
		suspectAfter: suspectAfter,
		deadAfter:    deadAfter,
	}
}

// Update registra el heartbeat de un DataNode y reemplaza sus ubicaciones.
//
// El reporte trae el inventario COMPLETO, así que el reemplazo es total: primero se
// retiran todas las ubicaciones que este nodo tenía y luego se añaden las que
// acaba de declarar. Si un nodo perdió su volumen, sus chunks desaparecen del mapa
// en el siguiente heartbeat sin que nadie tenga que avisar.
func (r *Registry) Update(report *pb.NodeReport) {
	id := types.NodeID(report.GetNodeId())

	r.mu.Lock()
	defer r.mu.Unlock()

	node, known := r.nodes[id]
	if !known {
		node = &NodeState{ID: id, inventory: make(map[types.ChunkID]struct{})}
		r.nodes[id] = node
	}

	// Retirar las ubicaciones del reporte anterior.
	for chunk := range node.inventory {
		if set, ok := r.locations[chunk]; ok {
			delete(set, id)
			if len(set) == 0 {
				delete(r.locations, chunk)
			}
		}
	}

	fresh := make(map[types.ChunkID]struct{}, len(report.GetChunkIds()))
	for _, raw := range report.GetChunkIds() {
		chunk := types.ChunkID(raw)
		fresh[chunk] = struct{}{}
		if r.locations[chunk] == nil {
			r.locations[chunk] = make(map[types.NodeID]struct{})
		}
		r.locations[chunk][id] = struct{}{}
	}

	node.Endpoint = report.GetAdvertisedEndpoint()
	node.GRPCEndpoint = report.GetGrpcEndpoint()
	node.Capacity = report.GetCapacityBytes()
	node.Used = report.GetUsedBytes()
	node.InFlight = report.GetInFlight()
	node.Draining = report.GetDraining()
	node.Chunks = len(fresh)
	node.LastSeen = time.Now()
	node.Status = StatusAlive
	node.inventory = fresh

	// El inventario recién llegado es autoritativo para este nodo, así que cualquier
	// suposición pendiente sobre él deja de hacer falta: si el chunk está, ya entró
	// en locations; si no está, la suposición era falsa y debe desaparecer.
	for chunk, set := range r.expected {
		if _, assumed := set[id]; !assumed {
			continue
		}
		delete(set, id)
		if len(set) == 0 {
			delete(r.expected, chunk)
		}
	}
}

// ExpectReplicas registra que un chunk debería estar en estos nodos.
//
// Lo llama el Allocator al confirmar una versión. La suposición caduca por sí sola y
// el primer inventario que llegue de cada nodo la reemplaza por el hecho observado.
func (r *Registry) ExpectReplicas(chunk types.ChunkID, nodes []types.NodeID) {
	if len(nodes) == 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	deadline := time.Now().Add(r.expectedTTL)
	if r.expected[chunk] == nil {
		r.expected[chunk] = make(map[types.NodeID]time.Time, len(nodes))
	}
	for _, id := range nodes {
		r.expected[chunk][id] = deadline
	}
}

// holders devuelve los nodos no muertos que tienen o deberían tener un chunk.
// Une lo confirmado por inventario con lo supuesto y aún vigente.
// Quien llama debe tener el candado tomado.
func (r *Registry) holders(chunk types.ChunkID, now time.Time) []types.NodeID {
	seen := make(map[types.NodeID]struct{}, 3)
	for id := range r.locations[chunk] {
		seen[id] = struct{}{}
	}
	for id, deadline := range r.expected[chunk] {
		if now.Before(deadline) {
			seen[id] = struct{}{}
		}
	}

	out := make([]types.NodeID, 0, len(seen))
	for id := range seen {
		if n, ok := r.nodes[id]; ok && r.classify(n, now) != StatusDead {
			out = append(out, id)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// classify determina el estado de un nodo por el tiempo sin reportar (RN-13).
//
// El estado intermedio SOSPECHOSO existe para que un nodo que reinicia rápido no
// dispare re-replicación: entre los dos umbrales el ControlNode lo excluye de
// asignaciones nuevas pero todavía no da sus chunks por perdidos.
func (r *Registry) classify(n *NodeState, now time.Time) NodeStatus {
	switch silence := now.Sub(n.LastSeen); {
	case silence > r.deadAfter:
		return StatusDead
	case silence > r.suspectAfter:
		return StatusSuspect
	default:
		return StatusAlive
	}
}

// Alive devuelve los nodos que reportaron recientemente y aceptan escrituras,
// ordenados por ocupación ascendente.
func (r *Registry) Alive() []*NodeState {
	r.mu.RLock()
	defer r.mu.RUnlock()

	now := time.Now()
	out := make([]*NodeState, 0, len(r.nodes))
	for _, n := range r.nodes {
		if r.classify(n, now) != StatusAlive || n.Draining {
			continue
		}
		copy := *n
		copy.Status = StatusAlive
		out = append(out, &copy)
	}
	sortByOccupancy(out)
	return out
}

// All devuelve todos los nodos conocidos con su estado actual, para diagnóstico.
func (r *Registry) All() []*NodeState {
	r.mu.RLock()
	defer r.mu.RUnlock()

	now := time.Now()
	out := make([]*NodeState, 0, len(r.nodes))
	for _, n := range r.nodes {
		copy := *n
		copy.Status = r.classify(n, now)
		out = append(out, &copy)
	}
	sortByOccupancy(out)
	return out
}

// sortByOccupancy ordena por ocupación y desempata por identificador, para que la
// colocación sea determinista y reproducible en la demostración.
func sortByOccupancy(nodes []*NodeState) {
	sort.Slice(nodes, func(i, j int) bool {
		if ui, uj := nodes[i].Utilization(), nodes[j].Utilization(); ui != uj {
			return ui < uj
		}
		return nodes[i].ID < nodes[j].ID
	})
}

// Locations devuelve los nodos vivos que reportaron tener este chunk.
func (r *Registry) Locations(chunk types.ChunkID) []types.NodeID {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.holders(chunk, time.Now())
}

// EndpointsFor devuelve las direcciones alcanzables de las réplicas de un chunk.
//
// El cliente recibe varias para poder reintentar contra otra si una falla, sin que
// el usuario vea el error (RQ-26). Y las recibe en runtime, no de un archivo de
// configuración: es lo que hace posible RX-14 y RQ-25.
func (r *Registry) EndpointsFor(chunk types.ChunkID) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	now := time.Now()
	out := make([]string, 0, 3)
	for _, id := range r.holders(chunk, now) {
		if n, ok := r.nodes[id]; ok && n.Endpoint != "" {
			out = append(out, n.Endpoint)
		}
	}
	sort.Strings(out)
	return out
}

// Totals agrega la capacidad y la ocupación del clúster, para el comando df.
func (r *Registry) Totals() (capacity, used int64, aliveNodes, chunks int) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	now := time.Now()
	for _, n := range r.nodes {
		if r.classify(n, now) == StatusDead {
			continue
		}
		capacity += n.Capacity
		used += n.Used
		aliveNodes++
	}
	return capacity, used, aliveNodes, len(r.locations)
}

type ReplicaTask struct {
	Chunk      types.ChunkID
	Source     types.NodeID
	Target     types.NodeID
	TargetGRPC string
	Have       int
}

func (r *Registry) PlanReplication(chunks []types.ChunkID, want int, pending map[types.ChunkID]map[types.NodeID]struct{}) []ReplicaTask {
	r.mu.RLock()
	defer r.mu.RUnlock()

	now := time.Now()

	type candidate struct {
		chunk types.ChunkID
		held  map[types.NodeID]struct{}
	}
	var candidates []candidate
	for _, chunk := range chunks {
		held := make(map[types.NodeID]struct{}, want)
		for _, id := range r.holders(chunk, now) {
			held[id] = struct{}{}
		}
		for id := range pending[chunk] {
			held[id] = struct{}{}
		}
		if len(held) < want {
			candidates = append(candidates, candidate{chunk: chunk, held: held})
		}
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		if len(candidates[i].held) != len(candidates[j].held) {
			return len(candidates[i].held) < len(candidates[j].held)
		}
		return candidates[i].chunk < candidates[j].chunk
	})

	load := make(map[types.NodeID]int)
	var tasks []ReplicaTask
	for _, c := range candidates {
		var source *NodeState
		for id := range r.locations[c.chunk] {
			n, ok := r.nodes[id]
			if !ok || r.classify(n, now) != StatusAlive {
				continue
			}
			if source == nil || load[id] < load[source.ID] || (load[id] == load[source.ID] && id < source.ID) {
				source = n
			}
		}
		if source == nil {
			continue
		}

		var target *NodeState
		for id, n := range r.nodes {
			if _, has := c.held[id]; has {
				continue
			}
			if n.Draining || n.GRPCEndpoint == "" || n.Free() <= 0 || r.classify(n, now) != StatusAlive {
				continue
			}
			if target == nil || load[id] < load[target.ID] ||
				(load[id] == load[target.ID] && n.Utilization() < target.Utilization()) ||
				(load[id] == load[target.ID] && n.Utilization() == target.Utilization() && id < target.ID) {
				target = n
			}
		}
		if target == nil {
			continue
		}

		load[source.ID]++
		load[target.ID]++
		tasks = append(tasks, ReplicaTask{
			Chunk:      c.chunk,
			Source:     source.ID,
			Target:     target.ID,
			TargetGRPC: target.GRPCEndpoint,
			Have:       len(c.held),
		})
	}
	return tasks
}
