package datanode

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/thomaszambrano/DFS-Topicos-Telematica/internal/types"
)

func nuevoStore(t *testing.T) *Store {
	t.Helper()
	s, err := NewStore(t.TempDir(), 1<<30)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	return s
}

func TestPutGetRoundTrip(t *testing.T) {
	s := nuevoStore(t)
	datos := []byte("contenido del chunk")

	n, sum, err := s.Put("abc123", bytes.NewReader(datos), Checksum(datos))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if n != int64(len(datos)) {
		t.Errorf("tamaño = %d, esperaba %d", n, len(datos))
	}
	if sum != Checksum(datos) {
		t.Errorf("checksum = %s, esperaba %s", sum, Checksum(datos))
	}

	leido, err := s.Get("abc123")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(leido, datos) {
		t.Errorf("Get devolvió %q, esperaba %q", leido, datos)
	}
}

// El último chunk conserva su tamaño real y no se rellena (RT-02).
func TestPutNoRellenaElUltimoChunk(t *testing.T) {
	s := nuevoStore(t)
	corto := []byte("7 bytes")

	n, _, err := s.Put("corto", bytes.NewReader(corto), "")
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if n != 7 {
		t.Fatalf("se guardaron %d bytes, esperaba 7: el chunk se rellenó", n)
	}
}

// Un checksum que no coincide se rechaza y NO deja nada en disco: ni el chunk final
// ni el temporal (escritura atómica por renombrado).
func TestPutRechazaChecksumIncorrectoSinDejarBasura(t *testing.T) {
	s := nuevoStore(t)

	_, _, err := s.Put("malo", bytes.NewReader([]byte("datos")), Checksum([]byte("otros datos")))
	if err == nil {
		t.Fatal("Put aceptó un chunk con checksum incorrecto")
	}
	if s.Has("malo") {
		t.Error("el chunk rechazado quedó en disco")
	}
	entradas, _ := os.ReadDir(s.baseDir)
	for _, e := range entradas {
		if strings.HasSuffix(e.Name(), tmpExt) {
			t.Errorf("quedó un temporal sin limpiar: %s", e.Name())
		}
	}
}

// Un chunk alterado en disco se detecta al servirlo (RD-02).
func TestGetDetectaCorrupcionEnDisco(t *testing.T) {
	s := nuevoStore(t)
	datos := []byte("contenido íntegro")
	if _, _, err := s.Put("victima", bytes.NewReader(datos), ""); err != nil {
		t.Fatalf("Put: %v", err)
	}

	if err := os.WriteFile(filepath.Join(s.baseDir, "victima"+chunkExt), []byte("alterado"), 0o644); err != nil {
		t.Fatalf("simulando corrupción: %v", err)
	}

	if _, err := s.Get("victima"); err == nil {
		t.Fatal("Get devolvió un chunk corrupto sin error")
	}
	if err := s.Verify("victima"); err == nil {
		t.Fatal("Verify no detectó la corrupción")
	}
}

// El chunk_id llega por la URL y se usa para construir una ruta: no debe poder
// escapar del directorio de datos.
func TestIDsPeligrososSeRechazan(t *testing.T) {
	s := nuevoStore(t)
	for _, id := range []types.ChunkID{"../fuga", "a/b", "", "con espacio", "punto.punto"} {
		if _, _, err := s.Put(id, bytes.NewReader([]byte("x")), ""); err == nil {
			t.Errorf("Put aceptó el id peligroso %q", id)
		}
		if s.Has(id) {
			t.Errorf("Has aceptó el id peligroso %q", id)
		}
	}
}

// Inventory es la respuesta a "¿qué tienes?" y se calcula leyendo el disco (RN-03).
func TestInventoryYStats(t *testing.T) {
	s := nuevoStore(t)
	for _, id := range []types.ChunkID{"uno", "dos", "tres"} {
		if _, _, err := s.Put(id, bytes.NewReader([]byte("1234")), ""); err != nil {
			t.Fatalf("Put %s: %v", id, err)
		}
	}

	inv, err := s.Inventory()
	if err != nil {
		t.Fatalf("Inventory: %v", err)
	}
	if len(inv) != 3 {
		t.Errorf("Inventory devolvió %d chunks, esperaba 3", len(inv))
	}
	if used, _, count := s.Stats(); used != 12 || count != 3 {
		t.Errorf("Stats = (%d, %d), esperaba (12, 3)", used, count)
	}

	if err := s.Delete("dos"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if used, _, count := s.Stats(); used != 8 || count != 2 {
		t.Errorf("tras Delete, Stats = (%d, %d), esperaba (8, 2)", used, count)
	}
}

// Un Store que reabre un directorio existente recalcula la ocupación recorriéndolo:
// el disco es la fuente de verdad, no un contador guardado.
func TestNewStoreRecalculaOcupacion(t *testing.T) {
	dir := t.TempDir()
	primero, err := NewStore(dir, 1<<30)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	if _, _, err := primero.Put("persistente", bytes.NewReader([]byte("0123456789")), ""); err != nil {
		t.Fatalf("Put: %v", err)
	}

	segundo, err := NewStore(dir, 1<<30)
	if err != nil {
		t.Fatalf("reabriendo: %v", err)
	}
	if used, _, count := segundo.Stats(); used != 10 || count != 1 {
		t.Errorf("tras reabrir, Stats = (%d, %d), esperaba (10, 1)", used, count)
	}
}

// Reenviar el mismo chunk (el ACK se perdió y el cliente reintenta) sobrescribe sin
// inflar la ocupación ni el contador: la escritura es idempotente.
func TestPutRepetidoEsIdempotente(t *testing.T) {
	s := nuevoStore(t)
	datos := []byte("12345678")

	for i := 0; i < 3; i++ {
		if _, _, err := s.Put("repetido", bytes.NewReader(datos), Checksum(datos)); err != nil {
			t.Fatalf("Put #%d: %v", i, err)
		}
	}

	if used, _, count := s.Stats(); used != 8 || count != 1 {
		t.Errorf("tras 3 Put del mismo chunk, Stats = (%d, %d), esperaba (8, 1)", used, count)
	}
	leido, err := s.Get("repetido")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(leido, datos) {
		t.Errorf("Get devolvió %q, esperaba %q", leido, datos)
	}
}
