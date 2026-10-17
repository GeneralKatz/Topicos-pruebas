package controlnode

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	"github.com/thomaszambrano/DFS-Topicos-Telematica/internal/types"
)

// Target es un destino de escritura: a qué nodo y por qué dirección.
type Target struct {
	NodeID   types.NodeID `json:"node_id"`
	Endpoint string       `json:"endpoint"`
}

// ChunkPlan es la respuesta a "¿dónde escribo el chunk número N?".
type ChunkPlan struct {
	ChunkID types.ChunkID `json:"chunk_id"`
	Index   uint32        `json:"index"`
	Targets []Target      `json:"targets"`

	// Degraded indica que se asignaron menos destinos que el factor de replicación
	// pedido. La escritura se acepta igual y la reparación queda pendiente (RN-20).
	Degraded bool `json:"degraded"`
}

// Upload es una carga en construcción: una versión que todavía no se ha publicado.
//
// Existe separada de types.Version a propósito. Mientras la carga vive, el objeto
// del namespace sigue apuntando a su versión anterior, y ningún lector puede ver
// nada de esto. Es la estructura que responde la pregunta central del hito: hasta
// el Commit, un lector ve la versión anterior íntegra, o nada si no había ninguna.
type Upload struct {
	ID        string          `json:"upload_id"`
	Path      string          `json:"path"`
	Owner     string          `json:"owner"`
	VersionID types.VersionID `json:"version_id"`
	Size      int64           `json:"size"`
	ChunkSize int64           `json:"chunk_size"`
	Expected  int             `json:"expected_chunks"`
	CreatedAt time.Time       `json:"created_at"`

	// ExpiresAt es el vencimiento del lease sobre la clave del objeto. Si el cliente
	// desaparece a mitad de una carga, el lease vence y el objeto vuelve a estar
	// disponible para otro escritor (RT-18, RN-24). Sin esto, un cliente que muere
	// bloquea una ruta para siempre.
	ExpiresAt time.Time `json:"expires_at"`

	chunks map[uint32]types.ChunkMeta
}

// ManifestChunk es una entrada del manifiesto de lectura.
type ManifestChunk struct {
	ChunkID  types.ChunkID `json:"chunk_id"`
	Index    uint32        `json:"index"`
	Size     int64         `json:"size"`
	Checksum string        `json:"checksum"`

	// Endpoints son todas las réplicas vivas. El cliente elige una y, si falla,
	// reintenta con otra sin que el usuario vea el error (RC-10, RQ-26).
	Endpoints []string `json:"endpoints"`
}

// Manifest es todo lo que el cliente necesita para reconstruir un objeto.
//
// Se emite en runtime y no sale de ningún archivo de configuración: es lo que hace
// posible la transparencia de localización (RQ-24) y el descubrimiento dinámico
// (RX-14, RQ-25). Ninguna ruta del namespace codifica la identidad de un nodo.
type Manifest struct {
	Path      string          `json:"path"`
	VersionID types.VersionID `json:"version_id"`
	Size      int64           `json:"size"`
	SHA256    string          `json:"sha256"`
	ChunkSize int64           `json:"chunk_size"`
	Chunks    []ManifestChunk `json:"chunks"`
}

type lease struct {
	uploadID  string
	expiresAt time.Time
}

// Allocator gestiona las cargas y publica las versiones.
type Allocator struct {
	mu     sync.Mutex
	ns     *Namespace
	placer *Placer
	reg    *Registry

	chunkSize int64
	r         int
	w         int // quórum de escritura: confirmaciones mínimas por chunk (RT-06)
	leaseTTL  time.Duration

	uploads map[string]*Upload
	// leases da exclusión mutua por clave de objeto. Dos escrituras concurrentes
	// sobre la misma clave se resuelven por exclusión, no por fusión: el segundo
	// escritor recibe 409 explícito (RQ-11).
	leases map[string]lease
	// idempotency recuerda qué upload creó cada clave de idempotencia, para que un
	// reintento tras un timeout no produzca una versión duplicada (RT-08).
	idempotency map[string]string
}

// NewAllocator construye el gestor de cargas.
func NewAllocator(ns *Namespace, placer *Placer, reg *Registry, chunkSize int64, r, w int, leaseTTL time.Duration) *Allocator {
	return &Allocator{
		ns:          ns,
		placer:      placer,
		reg:         reg,
		chunkSize:   chunkSize,
		r:           r,
		w:           w,
		leaseTTL:    leaseTTL,
		uploads:     make(map[string]*Upload),
		leases:      make(map[string]lease),
		idempotency: make(map[string]string),
	}
}

func (a *Allocator) checkWritable() error {
	if a.r < 2 {
		return nil
	}
	if alive := len(a.reg.Alive()); alive < 2 {
		return fmt.Errorf("%w: hay %d DataNode disponible y con R=%d se exigen al menos 2 para aceptar escrituras nuevas",
			ErrNoNodes, alive, a.r)
	}
	return nil
}

// randomID genera un identificador opaco de 16 bytes en hexadecimal.
//
// Opaco a propósito: un chunk_id no debe revelar la ruta, el dueño ni el nodo donde
// vive. Si lo hiciera, el identificador filtraría información del namespace al plano
// de datos, que es justo lo que RD-01 evita.
func randomID() types.ChunkID {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		// crypto/rand no falla en la práctica; si fallara, no hay forma segura de
		// continuar generando identificadores únicos.
		panic("no hay entropía disponible: " + err.Error())
	}
	return types.ChunkID(hex.EncodeToString(b))
}

// OpenUpload inicia una carga: valida la ruta, toma el lease y reserva la versión.
//
// No escribe ningún byte ni toca a ningún DataNode. Solo decide el esquema: cuántos
// chunks va a haber y de qué tamaño. El cliente ejecuta ese plan, no lo elige (RT-03).
func (a *Allocator) OpenUpload(path, owner string, size int64, idempotencyKey string) (*Upload, error) {
	if size < 0 {
		return nil, fmt.Errorf("%w: tamaño negativo %d", ErrBadRequest, size)
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	if err := a.checkWritable(); err != nil {
		return nil, err
	}

	// Reintento de una petición ya atendida: se devuelve la misma carga en lugar de
	// abrir otra (RT-08).
	if idempotencyKey != "" {
		if id, seen := a.idempotency[idempotencyKey]; seen {
			if up, ok := a.uploads[id]; ok {
				return up, nil
			}
		}
	}

	if held, ok := a.leases[path]; ok {
		if time.Now().Before(held.expiresAt) {
			return nil, fmt.Errorf("%w: %s tiene una carga en curso (%s)", ErrConflict, path, held.uploadID)
		}
		// El lease venció: el titular desapareció y la ruta vuelve a estar libre.
		log.Printf("lease vencido sobre %s (carga %s abandonada)", path, held.uploadID)
		delete(a.uploads, held.uploadID)
		delete(a.leases, path)
	}

	// Crear la entrada del objeto valida que el padre exista y que no sea un
	// directorio. Queda con Current en nil: visible, sin contenido publicado.
	if _, err := a.ns.CreateObject(path, owner); err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	expected := 0
	if size > 0 {
		expected = int((size + a.chunkSize - 1) / a.chunkSize)
	}

	up := &Upload{
		ID:        string(randomID()),
		Path:      path,
		Owner:     owner,
		VersionID: types.VersionID(randomID()),
		Size:      size,
		ChunkSize: a.chunkSize,
		Expected:  expected,
		CreatedAt: now,
		ExpiresAt: now.Add(a.leaseTTL),
		chunks:    make(map[uint32]types.ChunkMeta),
	}
	a.uploads[up.ID] = up
	a.leases[path] = lease{uploadID: up.ID, expiresAt: up.ExpiresAt}
	if idempotencyKey != "" {
		a.idempotency[idempotencyKey] = up.ID
	}

	log.Printf("OPEN  %s | upload=%s version=%s | %d bytes en %d chunks de %d MB",
		path, up.ID, up.VersionID, size, expected, a.chunkSize/(1024*1024))
	return up, nil
}

// AllocateChunk decide dónde se escribe un chunk y registra sus metadatos.
//
// El ControlNode genera el chunk_id y elige el destino; el cliente nunca elige a qué
// DataNode escribir (RT-03). Los bytes no pasan por aquí: el ControlNode solo emite
// un plan, y el tráfico de este plano es proporcional al NÚMERO de chunks, no a su
// tamaño (RQ-13).
func (a *Allocator) AllocateChunk(uploadID string, index uint32, size int64, checksum string) (*ChunkPlan, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	up, err := a.liveUpload(uploadID)
	if err != nil {
		return nil, err
	}

	if err := a.checkWritable(); err != nil {
		return nil, err
	}

	// Reasignar el mismo índice devuelve el mismo chunk_id: un reintento del cliente
	// tras un timeout no crea un chunk huérfano (RT-08).
	if existing, done := up.chunks[index]; done {
		return a.planFor(existing, index), nil
	}

	nodes, err := a.placer.Select(index, a.r)
	if err != nil {
		return nil, err
	}

	meta := types.ChunkMeta{
		ID:       randomID(),
		Index:    index,
		Size:     size,
		Checksum: checksum,
	}
	for _, n := range nodes {
		meta.Replicas = append(meta.Replicas, n.ID)
	}
	up.chunks[index] = meta

	// Renovar el lease en cada asignación: una carga larga que sigue progresando no
	// debe perder su exclusión por vencimiento (RN-24).
	up.ExpiresAt = time.Now().UTC().Add(a.leaseTTL)
	a.leases[up.Path] = lease{uploadID: up.ID, expiresAt: up.ExpiresAt}

	plan := &ChunkPlan{ChunkID: meta.ID, Index: index, Degraded: len(nodes) < a.r}
	for _, n := range nodes {
		plan.Targets = append(plan.Targets, Target{NodeID: n.ID, Endpoint: n.Endpoint})
	}

	// Registro visible del particionamiento: el profesor lo pidió explícitamente
	// (RC-08). Muestra número de chunk, tamaño y DataNode destino.
	log.Printf("CHUNK %s | idx=%d size=%d | chunk=%s -> %v%s",
		up.Path, index, size, meta.ID, plan.Targets, degradedMark(plan.Degraded))
	return plan, nil
}

func degradedMark(degraded bool) string {
	if degraded {
		return " [DEGRADADA]"
	}
	return ""
}

func (a *Allocator) planFor(meta types.ChunkMeta, index uint32) *ChunkPlan {
	plan := &ChunkPlan{ChunkID: meta.ID, Index: index, Degraded: len(meta.Replicas) < a.r}
	for _, id := range meta.Replicas {
		plan.Targets = append(plan.Targets, Target{NodeID: id, Endpoint: a.endpointOf(id)})
	}
	return plan
}

func (a *Allocator) endpointOf(id types.NodeID) string {
	for _, n := range a.reg.All() {
		if n.ID == id {
			return n.Endpoint
		}
	}
	return ""
}

// Commit publica la versión y libera el lease.
//
// ES LA ÚLTIMA OPERACIÓN DE LA ESCRITURA Y ES ATÓMICA (RT-07). Todo lo anterior
// —chunks escritos en los DataNodes, metadatos acumulados en el Upload— es invisible
// para cualquier lector. La publicación consiste en mover un puntero bajo el candado
// del namespace, así que no existe ningún instante en el que se pueda observar una
// versión a medias (RQ-10). Si el cliente muere antes de llegar aquí, el objeto
// anterior permanece intacto y los chunks escritos quedan huérfanos (RT-09).
func (a *Allocator) Commit(uploadID, sha256 string, confirmed map[types.ChunkID][]types.NodeID) (*types.Version, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	up, err := a.liveUpload(uploadID)
	if err != nil {
		return nil, err
	}

	// La lista ordenada se arma aquí, a partir del mapa por índice, y se valida que
	// no falte ningún chunk. Un hueco significaría un archivo con un agujero.
	ordered := make([]types.ChunkMeta, 0, len(up.chunks))
	indexes := make([]uint32, 0, len(up.chunks))
	for i := range up.chunks {
		indexes = append(indexes, i)
	}
	sort.Slice(indexes, func(i, j int) bool { return indexes[i] < indexes[j] })
	for position, index := range indexes {
		if uint32(position) != index {
			return nil, fmt.Errorf("%w: falta el chunk %d de %s", ErrIncomplete, position, up.Path)
		}
		ordered = append(ordered, up.chunks[index])
	}
	if up.Expected > 0 && len(ordered) != up.Expected {
		return nil, fmt.Errorf("%w: esperaba %d chunks, recibí %d", ErrIncomplete, up.Expected, len(ordered))
	}

	var total int64
	for _, c := range ordered {
		total += c.Size
	}
	if up.Size > 0 && total != up.Size {
		return nil, fmt.Errorf("%w: los chunks suman %d bytes, se declararon %d", ErrIncomplete, total, up.Size)
	}

	// Quórum de escritura (RT-06): cada chunk debe tener al menos W confirmaciones, y
	// las réplicas que se registran son las que CONFIRMARON, no todas las asignadas.
	// Los chunks que quedan con menos de R copias son los que el replicador deberá
	// completar en segundo plano.
	underReplicated := 0
	for i, c := range ordered {
		replicas, err := a.confirmedReplicas(c, confirmed)
		if err != nil {
			return nil, err
		}
		ordered[i].Replicas = replicas
		if len(replicas) < a.r {
			underReplicated++
		}
	}
	if underReplicated > 0 {
		log.Printf("COMMIT %s | %d chunks con menos de R=%d réplicas confirmadas (re-replicación pendiente)",
			up.Path, underReplicated, a.r)
	}

	version := &types.Version{
		ID:        up.VersionID,
		Size:      total,
		SHA256:    sha256,
		CreatedAt: time.Now().UTC(),
		Chunks:    ordered,
	}

	if err := a.ns.PublishVersion(up.Path, version); err != nil {
		return nil, err
	}

	// El cliente declaró en qué nodos confirmó cada chunk (o, si no informó nada, se
	// asumen los asignados). Se informan al registro para que una lectura inmediata
	// después de esta escritura encuentre las ubicaciones, sin esperar el próximo
	// heartbeat (RQ-09). Son suposiciones con caducidad: el inventario de cada nodo
	// las confirma o las desmiente en segundos.
	for _, c := range ordered {
		a.reg.ExpectReplicas(c.ID, c.Replicas)
	}

	delete(a.uploads, up.ID)
	delete(a.leases, up.Path)

	log.Printf("COMMIT %s | version=%s | %d chunks | %d bytes | sha256=%s",
		up.Path, version.ID, len(ordered), total, sha256)
	return version, nil
}

// confirmedReplicas decide en qué nodos se registra un chunk como replicado.
//
// Si el cliente no informó confirmaciones (confirmed == nil, como el cliente del
// Hito 2) se conserva el comportamiento anterior: todos los destinos asignados. Si las
// informó, solo cuentan los nodos que además fueron asignados a ESE chunk —un cliente
// no puede declarar réplicas en nodos que el ControlNode no eligió (RT-03)— y el chunk
// debe alcanzar el quórum W. El quórum exigido se acota por los destinos asignados:
// con R=1 hay un solo destino y basta con su confirmación.
func (a *Allocator) confirmedReplicas(c types.ChunkMeta, confirmed map[types.ChunkID][]types.NodeID) ([]types.NodeID, error) {
	if confirmed == nil {
		return c.Replicas, nil
	}

	assigned := make(map[types.NodeID]struct{}, len(c.Replicas))
	for _, id := range c.Replicas {
		assigned[id] = struct{}{}
	}

	seen := make(map[types.NodeID]struct{}, len(c.Replicas))
	var out []types.NodeID
	for _, id := range confirmed[c.ID] {
		if _, ok := assigned[id]; !ok {
			continue
		}
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}

	need := a.w
	if need > len(c.Replicas) {
		need = len(c.Replicas)
	}
	if need < 1 {
		need = 1
	}
	if len(out) < need {
		return nil, fmt.Errorf("%w: el chunk %d (%s) tiene %d confirmaciones y el quórum de escritura exige %d",
			ErrIncomplete, c.Index, c.ID, len(out), need)
	}
	return out, nil
}

// Abort cancela una carga, libera el lease y devuelve los chunks a recolectar.
//
// Tras abortar, el objeto anterior permanece intacto y accesible (RT-09): nunca se
// publicó nada, así que no hay nada que revertir.
func (a *Allocator) Abort(uploadID string) ([]types.ChunkID, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	up, ok := a.uploads[uploadID]
	if !ok {
		return nil, fmt.Errorf("%w: carga %s", ErrNotFound, uploadID)
	}
	orphans := make([]types.ChunkID, 0, len(up.chunks))
	for _, c := range up.chunks {
		orphans = append(orphans, c.ID)
	}
	delete(a.uploads, uploadID)
	delete(a.leases, up.Path)

	log.Printf("ABORT %s | upload=%s | %d chunks quedan huérfanos", up.Path, uploadID, len(orphans))
	return orphans, nil
}

// liveUpload devuelve una carga vigente, o error si no existe o su lease venció.
// Quien llama debe tener el candado tomado.
func (a *Allocator) liveUpload(uploadID string) (*Upload, error) {
	up, ok := a.uploads[uploadID]
	if !ok {
		return nil, fmt.Errorf("%w: carga %s", ErrNotFound, uploadID)
	}
	if time.Now().After(up.ExpiresAt) {
		delete(a.uploads, uploadID)
		delete(a.leases, up.Path)
		return nil, fmt.Errorf("%w: el lease de %s venció", ErrConflict, uploadID)
	}
	return up, nil
}

// Manifest devuelve la lista ordenada de chunks de la versión vigente de un objeto,
// con las direcciones donde cada uno vive ahora mismo.
//
// Las direcciones se resuelven en el momento de emitir el manifiesto, no se guardan:
// vienen del registro, que a su vez las reconstruye de los heartbeats. Por eso el
// cliente puede operar después de que TODAS las direcciones de los DataNodes cambien,
// sin tocar su configuración (RQ-25).
func (a *Allocator) Manifest(path string) (*Manifest, error) {
	inode, err := a.ns.Stat(path)
	if err != nil {
		return nil, err
	}
	if inode.IsDir() {
		return nil, fmt.Errorf("%w: %s", ErrIsDir, path)
	}
	if inode.Current == nil {
		return nil, fmt.Errorf("%w: %s no tiene ninguna versión publicada", ErrNotFound, path)
	}

	v := inode.Current
	m := &Manifest{
		Path:      path,
		VersionID: v.ID,
		Size:      v.Size,
		SHA256:    v.SHA256,
		ChunkSize: a.chunkSize,
		Chunks:    make([]ManifestChunk, 0, len(v.Chunks)),
	}
	for _, c := range v.Chunks {
		endpoints := a.reg.EndpointsFor(c.ID)
		if len(endpoints) == 0 {
			return nil, fmt.Errorf("%w: ningún nodo vivo reporta el chunk %s de %s", ErrNoNodes, c.ID, path)
		}
		m.Chunks = append(m.Chunks, ManifestChunk{
			ChunkID:   c.ID,
			Index:     c.Index,
			Size:      c.Size,
			Checksum:  c.Checksum,
			Endpoints: endpoints,
		})
	}
	return m, nil
}

// ChunkSize expone el tamaño de chunk configurado.
func (a *Allocator) ChunkSize() int64 { return a.chunkSize }

// ReplicationFactor expone el factor de replicación configurado.
func (a *Allocator) ReplicationFactor() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.r
}

func (a *Allocator) SetReplicationFactor(n int) error {
	if n < 1 || n > 64 {
		return fmt.Errorf("%w: el factor de replicación debe estar entre 1 y 64, recibí %d", ErrBadRequest, n)
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	log.Printf("REPLICACIÓN | R cambia de %d a %d en caliente", a.r, n)
	a.r = n
	return nil
}
