// Package datanode implementa el plano de datos: guarda y sirve chunks
// identificados globalmente, sin conocer el namespace (RD-01).
package datanode

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/thomaszambrano/DFS-Topicos-Telematica/internal/types"
)

// Errores del almacén. Se exponen como valores para que las capas de arriba los
// distingan con errors.Is y traduzcan cada uno a su código HTTP (RI-05).
var (
	ErrNotFound   = errors.New("chunk no encontrado")
	ErrCorrupt    = errors.New("chunk corrupto")
	ErrBadRequest = errors.New("petición inválida")
	ErrNoSpace    = errors.New("sin espacio")
)

const (
	chunkExt = ".chunk"
	sumExt   = ".sum"
	tmpExt   = ".tmp"
)

// Store es el almacén local de chunks de un DataNode.
//
// No conoce objetos, rutas ni versiones: su universo es chunk_id -> bytes. Esa
// ignorancia es deliberada (RD-01) y es la razón por la que un DataNode no puede
// reconstruir a qué archivo pertenece un chunk que tiene en disco.
type Store struct {
	baseDir  string
	capacity int64

	// mu protege used y count, que dos peticiones concurrentes pueden modificar.
	// El contenido de los archivos no necesita este candado: cada chunk se
	// escribe una sola vez y su nombre es único.
	mu    sync.RWMutex
	used  int64
	count int
}

// NewStore abre o crea el almacén en baseDir y calcula el espacio ocupado
// recorriendo el directorio.
//
// El recorrido inicial es la contraparte de no persistir el contador: el disco es
// la fuente de verdad de lo que este nodo tiene, igual que el inventario que
// reporta al ControlNode.
func NewStore(baseDir string, capacity int64) (*Store, error) {
	if err := os.MkdirAll(baseDir, 0o755); err != nil {
		return nil, fmt.Errorf("creando %s: %w", baseDir, err)
	}
	s := &Store{baseDir: baseDir, capacity: capacity}

	entries, err := os.ReadDir(baseDir)
	if err != nil {
		return nil, fmt.Errorf("leyendo %s: %w", baseDir, err)
	}
	for _, e := range entries {
		name := e.Name()
		// Los temporales son restos de escrituras que murieron a mitad. Se borran
		// al arrancar: nunca fueron visibles como chunks válidos.
		if strings.HasSuffix(name, tmpExt) {
			_ = os.Remove(filepath.Join(baseDir, name))
			continue
		}
		if !strings.HasSuffix(name, chunkExt) {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		s.used += info.Size()
		s.count++
	}
	return s, nil
}

func (s *Store) chunkPath(id types.ChunkID) string {
	return filepath.Join(s.baseDir, string(id)+chunkExt)
}

func (s *Store) sumPath(id types.ChunkID) string {
	return filepath.Join(s.baseDir, string(id)+sumExt)
}

// Put escribe un chunk leyendo de r por flujo y devuelve su tamaño y su checksum.
//
// Si expected no está vacío, el chunk se rechaza cuando el checksum calculado no
// coincide, y no queda nada en disco.
//
// La escritura es ATÓMICA POR RENOMBRADO: se escribe en un archivo temporal, se
// fuerza a disco con Sync, y solo entonces se renombra al nombre definitivo. El
// renombrado dentro del mismo sistema de archivos es atómico, así que el chunk o
// existe completo o no existe. La alternativa —escribir directo al nombre final—
// deja, si el proceso muere a mitad, un archivo a medias con nombre válido: el
// sistema lo creería bueno y serviría datos truncados, que es peor que no tenerlo.
func (s *Store) Put(id types.ChunkID, r io.Reader, expected string) (int64, string, error) {
	if err := checkID(string(id)); err != nil {
		return 0, "", err
	}

	s.mu.RLock()
	used, capacity := s.used, s.capacity
	s.mu.RUnlock()
	if capacity > 0 && used >= capacity {
		return 0, "", fmt.Errorf("%w: %d de %d bytes ocupados", ErrNoSpace, used, capacity)
	}

	tmp, err := os.CreateTemp(s.baseDir, string(id)+"-*"+tmpExt)
	if err != nil {
		return 0, "", fmt.Errorf("creando temporal: %w", err)
	}
	tmpName := tmp.Name()
	// Si algo falla más abajo, el temporal no debe sobrevivir.
	defer func() {
		tmp.Close()
		os.Remove(tmpName)
	}()

	// El hash se calcula sobre el mismo flujo que va al disco: una sola pasada,
	// sin cargar el chunk en memoria (RI-04, RD-02).
	sum, n, err := teeChecksum(tmp, r)
	if err != nil {
		return 0, "", fmt.Errorf("escribiendo chunk: %w", err)
	}
	if expected != "" && sum != expected {
		return 0, "", fmt.Errorf("%w: esperaba %s, calculé %s", ErrCorrupt, expected, sum)
	}

	// Sync antes del rename: sin esto el rename puede quedar visible mientras el
	// contenido sigue en el caché del sistema operativo, y un corte de energía
	// dejaría un chunk con nombre válido y cuerpo incompleto.
	if err := tmp.Sync(); err != nil {
		return 0, "", fmt.Errorf("sync del temporal: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return 0, "", fmt.Errorf("cerrando temporal: %w", err)
	}

	// Si el chunk ya existía (el cliente reintentó porque se perdió el ACK, o llega una
	// copia de re-replicación) se sobrescribe con los mismos bytes, y esa escritura NO
	// debe contarse como un chunk nuevo: de lo contrario la ocupación que se reporta al
	// ControlNode se infla con cada reintento y distorsiona la colocación.
	previous := int64(-1)
	if info, err := os.Stat(s.chunkPath(id)); err == nil {
		previous = info.Size()
	}

	if err := os.WriteFile(s.sumPath(id), []byte(sum), 0o644); err != nil {
		return 0, "", fmt.Errorf("escribiendo checksum: %w", err)
	}
	if err := os.Rename(tmpName, s.chunkPath(id)); err != nil {
		os.Remove(s.sumPath(id))
		return 0, "", fmt.Errorf("renombrando: %w", err)
	}
	syncDir(s.baseDir)

	s.mu.Lock()
	if previous >= 0 {
		s.used += n - previous // sobrescritura: solo cambia la diferencia de tamaño
	} else {
		s.used += n
		s.count++
	}
	s.mu.Unlock()

	return n, sum, nil
}

// teeChecksum copia r en w calculando el SHA-256 de lo que pasa.
func teeChecksum(w io.Writer, r io.Reader) (string, int64, error) {
	counted := &countingWriter{w: w}
	sum, _, err := ChecksumReader(io.TeeReader(r, counted))
	return sum, counted.n, err
}

type countingWriter struct {
	w io.Writer
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}

// syncDir fuerza a disco la entrada de directorio para que el renombrado
// sobreviva a un corte de energía. Es el paso que casi todo el mundo olvida.
func syncDir(dir string) {
	d, err := os.Open(dir)
	if err != nil {
		return
	}
	defer d.Close()
	_ = d.Sync()
}

// Open devuelve el chunk abierto para lectura por flujo, con su tamaño y el
// checksum registrado al escribirlo.
//
// Devuelve un descriptor en lugar de los bytes para que el servidor HTTP pueda
// hacer io.Copy hacia la respuesta sin cargar 64 MB en memoria por cada petición
// concurrente (RI-04). Quien llama debe cerrarlo.
func (s *Store) Open(id types.ChunkID) (*os.File, int64, string, error) {
	if err := checkID(string(id)); err != nil {
		return nil, 0, "", err
	}
	f, err := os.Open(s.chunkPath(id))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, 0, "", fmt.Errorf("%w: %s", ErrNotFound, id)
		}
		return nil, 0, "", err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, 0, "", err
	}
	sum, err := s.storedSum(id)
	if err != nil {
		f.Close()
		return nil, 0, "", err
	}
	return f, info.Size(), sum, nil
}

// Get lee el chunk completo y verifica su integridad antes de devolverlo (RD-02).
//
// Carga el contenido en memoria, así que se usa para chunks pequeños y para
// pruebas; el camino de servicio HTTP usa Open y verifica en el cliente (RT-13).
func (s *Store) Get(id types.ChunkID) ([]byte, error) {
	if err := checkID(string(id)); err != nil {
		return nil, err
	}
	data, err := os.ReadFile(s.chunkPath(id))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("%w: %s", ErrNotFound, id)
		}
		return nil, err
	}
	stored, err := s.storedSum(id)
	if err != nil {
		return nil, err
	}
	if got := Checksum(data); got != stored {
		return nil, fmt.Errorf("%w: %s tiene %s, esperaba %s", ErrCorrupt, id, got, stored)
	}
	return data, nil
}

// storedSum lee el checksum guardado junto al chunk.
//
// Se guarda en un archivo aparte en lugar de recalcularse en cada lectura para no
// pagar un SHA-256 completo por cada GET; el costo es un archivo más que puede
// desincronizarse, y por eso Verify existe (ver P-4 en DECISIONES.md).
func (s *Store) storedSum(id types.ChunkID) (string, error) {
	b, err := os.ReadFile(s.sumPath(id))
	if err != nil {
		if os.IsNotExist(err) {
			return "", fmt.Errorf("%w: falta el checksum de %s", ErrCorrupt, id)
		}
		return "", err
	}
	return strings.TrimSpace(string(b)), nil
}

// Has indica si el chunk está presente en disco.
func (s *Store) Has(id types.ChunkID) bool {
	if checkID(string(id)) != nil {
		return false
	}
	_, err := os.Stat(s.chunkPath(id))
	return err == nil
}

// Inventory lista los chunks presentes en disco.
//
// Es la respuesta a "¿qué tienes?" que el ControlNode necesita para reconstruir el
// mapa chunk -> ubicaciones (RN-03). Se calcula leyendo el directorio y no una
// estructura en memoria porque el disco es la única fuente de verdad.
func (s *Store) Inventory() ([]types.ChunkID, error) {
	entries, err := os.ReadDir(s.baseDir)
	if err != nil {
		return nil, err
	}
	ids := make([]types.ChunkID, 0, len(entries))
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, chunkExt) {
			continue
		}
		ids = append(ids, types.ChunkID(strings.TrimSuffix(name, chunkExt)))
	}
	return ids, nil
}

// Stats devuelve el espacio ocupado, la capacidad declarada y el número de chunks.
// Alimenta el heartbeat y la función de puntuación de colocación (RD-10).
func (s *Store) Stats() (used, capacity int64, count int) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.used, s.capacity, s.count
}

// Delete elimina un chunk y su checksum. Lo usan las órdenes de recolección.
func (s *Store) Delete(id types.ChunkID) error {
	if err := checkID(string(id)); err != nil {
		return err
	}
	info, err := os.Stat(s.chunkPath(id))
	if err != nil {
		if os.IsNotExist(err) {
			return nil // borrar lo que no existe no es un error
		}
		return err
	}
	if err := os.Remove(s.chunkPath(id)); err != nil {
		return err
	}
	_ = os.Remove(s.sumPath(id))

	s.mu.Lock()
	s.used -= info.Size()
	s.count--
	s.mu.Unlock()
	return nil
}

// Verify recalcula el checksum de un chunk y lo compara con el registrado.
// Es la pieza sobre la que se construye el barrido de integridad del Hito 3 (RD-03).
func (s *Store) Verify(id types.ChunkID) error {
	f, _, stored, err := s.Open(id)
	if err != nil {
		return err
	}
	defer f.Close()
	got, _, err := ChecksumReader(f)
	if err != nil {
		return err
	}
	if got != stored {
		return fmt.Errorf("%w: %s tiene %s, esperaba %s", ErrCorrupt, id, got, stored)
	}
	return nil
}
