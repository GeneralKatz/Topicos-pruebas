package controlnode

import (
	"fmt"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/thomaszambrano/DFS-Topicos-Telematica/internal/types"
)

// Namespace es el árbol de directorios y objetos del sistema.
//
// Es ESTADO DURO: nadie más en el clúster conoce esta información. Si se pierde, los
// DataNodes siguen teniendo todos los bytes y ninguno sabe a qué archivo pertenecen
// ni en qué orden van (RD-01): el dato existe y está perdido. Por eso, a diferencia
// del mapa de ubicaciones, esto es lo que hay que persistir con WAL y fsync (RN-04).
//
// LIMITACIÓN DECLARADA DEL HITO 2: el namespace vive solo en memoria. El WAL y el
// snapshot son del Hito 3 (ver P-6 en DECISIONES.md). Un reinicio del ControlNode
// pierde el namespace; los chunks en los DataNodes quedan huérfanos.
type Namespace struct {
	// mu protege el árbol completo. Un solo candado en lugar de locks por ruta
	// (RN-25) porque a esta escala la contención no es el problema y un árbol con
	// candados por nodo es una fuente inagotable de interbloqueos.
	mu   sync.RWMutex
	root *types.Inode
}

// NewNamespace crea un namespace con el directorio raíz ya existente.
func NewNamespace() *Namespace {
	now := time.Now().UTC()
	return &Namespace{
		root: &types.Inode{
			Name:       "/",
			Kind:       types.KindDir,
			Owner:      "root",
			Mode:       0o755,
			CreatedAt:  now,
			ModifiedAt: now,
			Children:   make(map[string]*types.Inode),
		},
	}
}

// splitPath normaliza una ruta absoluta y devuelve sus segmentos.
//
// El cliente puede mandar rutas con "." , ".." o barras repetidas; path.Clean las
// resuelve antes de que toquen el árbol. Sin esta normalización, "/a/../../etc"
// escaparía de la raíz.
func splitPath(p string) ([]string, error) {
	if !strings.HasPrefix(p, "/") {
		return nil, fmt.Errorf("%w: la ruta debe ser absoluta, recibí %q", ErrBadRequest, p)
	}
	clean := path.Clean(p)
	if clean == "/" {
		return nil, nil
	}
	segments := strings.Split(strings.TrimPrefix(clean, "/"), "/")
	for _, s := range segments {
		if s == "" || s == "." || s == ".." {
			return nil, fmt.Errorf("%w: ruta inválida %q", ErrBadRequest, p)
		}
	}
	return segments, nil
}

// resolve recorre el árbol segmento a segmento hasta la ruta pedida.
//
// Todas las demás operaciones se apoyan en esta. Recorrer y no usar un mapa plano
// de rutas completas es lo que permite que Rename de un directorio sea O(1) y que
// los permisos se evalúen en cada segmento del camino, como exige la autorización
// POSIX del Hito 6.
//
// Quien llama debe tener el candado tomado.
func (ns *Namespace) resolve(p string) (*types.Inode, error) {
	segments, err := splitPath(p)
	if err != nil {
		return nil, err
	}
	node := ns.root
	for i, seg := range segments {
		if !node.IsDir() {
			return nil, fmt.Errorf("%w: %s", ErrNotDir, "/"+strings.Join(segments[:i], "/"))
		}
		child, ok := node.Children[seg]
		if !ok {
			return nil, fmt.Errorf("%w: %s", ErrNotFound, p)
		}
		node = child
	}
	return node, nil
}

// resolveParent devuelve el directorio padre de una ruta y el nombre del hijo.
func (ns *Namespace) resolveParent(p string) (*types.Inode, string, error) {
	segments, err := splitPath(p)
	if err != nil {
		return nil, "", err
	}
	if len(segments) == 0 {
		return nil, "", fmt.Errorf("%w: la raíz no tiene padre", ErrBadRequest)
	}
	name := segments[len(segments)-1]
	parent, err := ns.resolve("/" + strings.Join(segments[:len(segments)-1], "/"))
	if err != nil {
		return nil, "", err
	}
	if !parent.IsDir() {
		return nil, "", fmt.Errorf("%w: %s", ErrNotDir, p)
	}
	return parent, name, nil
}

// Mkdir crea un directorio. Falla si el padre no existe o el nombre está ocupado
// (RC-04). No crea padres intermedios: eso sería mkdir -p y esconde errores de ruta.
func (ns *Namespace) Mkdir(p, owner string) (*types.Inode, error) {
	ns.mu.Lock()
	defer ns.mu.Unlock()

	parent, name, err := ns.resolveParent(p)
	if err != nil {
		return nil, err
	}
	if _, taken := parent.Children[name]; taken {
		return nil, fmt.Errorf("%w: %s", ErrExists, p)
	}

	now := time.Now().UTC()
	node := &types.Inode{
		Name:       name,
		Kind:       types.KindDir,
		Owner:      owner,
		Mode:       0o755,
		CreatedAt:  now,
		ModifiedAt: now,
		Children:   make(map[string]*types.Inode),
	}
	parent.Children[name] = node
	parent.ModifiedAt = now
	return node, nil
}

// List devuelve los hijos de un directorio, ordenados por nombre.
//
// El orden lo impone aquí la presentación, no el almacenamiento: Children es un
// mapa precisamente porque el orden de los hijos no es información (a diferencia
// del orden de los chunks de una versión, que ES el archivo).
func (ns *Namespace) List(p string) ([]*types.Inode, error) {
	ns.mu.RLock()
	defer ns.mu.RUnlock()

	node, err := ns.resolve(p)
	if err != nil {
		return nil, err
	}
	if !node.IsDir() {
		// Listar un objeto devuelve el objeto, como hace ls con un archivo.
		return []*types.Inode{node}, nil
	}
	out := make([]*types.Inode, 0, len(node.Children))
	for _, child := range node.Children {
		out = append(out, child)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// Stat devuelve la entrada de una ruta.
func (ns *Namespace) Stat(p string) (*types.Inode, error) {
	ns.mu.RLock()
	defer ns.mu.RUnlock()
	return ns.resolve(p)
}

// CreateObject crea la entrada de un objeto sin versión publicada.
//
// El objeto queda visible en el namespace pero con Current en nil: existe y no
// tiene contenido. Es el estado intermedio entre OpenUpload y Commit.
func (ns *Namespace) CreateObject(p, owner string) (*types.Inode, error) {
	ns.mu.Lock()
	defer ns.mu.Unlock()

	parent, name, err := ns.resolveParent(p)
	if err != nil {
		return nil, err
	}
	if existing, taken := parent.Children[name]; taken {
		if existing.IsDir() {
			return nil, fmt.Errorf("%w: %s", ErrIsDir, p)
		}
		// Sobrescribir un objeto existente es válido: se publicará una versión
		// nueva y la anterior permanece accesible hasta el Commit (RN-06).
		return existing, nil
	}

	now := time.Now().UTC()
	node := &types.Inode{
		Name:       name,
		Kind:       types.KindObject,
		Owner:      owner,
		Mode:       0o644,
		CreatedAt:  now,
		ModifiedAt: now,
	}
	parent.Children[name] = node
	parent.ModifiedAt = now
	return node, nil
}

// PublishVersion instala una versión nueva como la vigente de un objeto.
//
// ESTA ES LA OPERACIÓN ATÓMICA DEL COMMIT (RT-07, RQ-10). Es una sola asignación de
// puntero bajo el candado de escritura: no existe instante en el que un lector
// pueda observar una versión a medias. Antes de esta línea, todo lector ve la
// versión anterior íntegra; después, todos ven la nueva. No hay estado intermedio
// porque publicar no consiste en copiar datos, sino en mover una referencia.
func (ns *Namespace) PublishVersion(p string, v *types.Version) error {
	ns.mu.Lock()
	defer ns.mu.Unlock()

	node, err := ns.resolve(p)
	if err != nil {
		return err
	}
	if node.IsDir() {
		return fmt.Errorf("%w: %s", ErrIsDir, p)
	}
	node.Current = v
	node.ModifiedAt = time.Now().UTC()
	return nil
}

// Remove elimina una entrada y devuelve los chunks que quedan sin referencia.
//
// Un directorio con hijos no se puede eliminar (RC-04): devuelve ErrNotEmpty, que se
// traduce a 409 y el cliente distingue de "no existe" (404). Borrar recursivamente
// por omisión es la clase de comodidad que destruye datos por accidente.
//
// Los chunks devueltos quedan marcados para recolección (RC-05); en el Hito 2 nadie
// los recoge todavía y el recolector es del Hito 3 (RN-07).
func (ns *Namespace) Remove(p string) ([]types.ChunkID, error) {
	ns.mu.Lock()
	defer ns.mu.Unlock()

	parent, name, err := ns.resolveParent(p)
	if err != nil {
		return nil, err
	}
	node, ok := parent.Children[name]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, p)
	}
	if node.IsDir() && len(node.Children) > 0 {
		return nil, fmt.Errorf("%w: %s tiene %d entradas", ErrNotEmpty, p, len(node.Children))
	}

	var orphans []types.ChunkID
	if node.Current != nil {
		for _, c := range node.Current.Chunks {
			orphans = append(orphans, c.ID)
		}
	}
	delete(parent.Children, name)
	parent.ModifiedAt = time.Now().UTC()
	return orphans, nil
}

// Rename mueve o renombra una entrada.
//
// El costo es independiente del tamaño del objeto (RC-07): no se mueve un solo byte,
// solo cambia de lugar una referencia en dos mapas. Los chunks no se enteran, y por
// eso tampoco hay que avisarle a ningún DataNode.
func (ns *Namespace) Rename(from, to string) error {
	ns.mu.Lock()
	defer ns.mu.Unlock()

	srcParent, srcName, err := ns.resolveParent(from)
	if err != nil {
		return err
	}
	node, ok := srcParent.Children[srcName]
	if !ok {
		return fmt.Errorf("%w: %s", ErrNotFound, from)
	}
	dstParent, dstName, err := ns.resolveParent(to)
	if err != nil {
		return err
	}
	if _, taken := dstParent.Children[dstName]; taken {
		return fmt.Errorf("%w: %s", ErrExists, to)
	}

	delete(srcParent.Children, srcName)
	node.Name = dstName
	dstParent.Children[dstName] = node

	now := time.Now().UTC()
	srcParent.ModifiedAt = now
	dstParent.ModifiedAt = now
	node.ModifiedAt = now
	return nil
}

func (ns *Namespace) CurrentChunks() []types.ChunkID {
	ns.mu.RLock()
	defer ns.mu.RUnlock()

	var out []types.ChunkID
	var walk func(n *types.Inode)
	walk = func(n *types.Inode) {
		if n.IsDir() {
			for _, child := range n.Children {
				walk(child)
			}
			return
		}
		if n.Current != nil {
			for _, c := range n.Current.Chunks {
				out = append(out, c.ID)
			}
		}
	}
	walk(ns.root)
	return out
}
