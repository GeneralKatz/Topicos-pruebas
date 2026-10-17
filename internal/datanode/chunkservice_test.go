package datanode

import (
	"bytes"
	"context"
	"crypto/rand"
	"net"
	"os"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/thomaszambrano/DFS-Topicos-Telematica/internal/pb"
	"github.com/thomaszambrano/DFS-Topicos-Telematica/internal/types"
)

func startNode(t *testing.T, id string) (*DataNode, string) {
	t.Helper()
	store, err := NewStore(t.TempDir(), 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	node := New(store, id, "localhost:0")
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	pb.RegisterChunkServiceServer(srv, NewChunkServer(node))
	go srv.Serve(listener)
	t.Cleanup(srv.Stop)
	return node, listener.Addr().String()
}

func randomBytes(t *testing.T, size int) []byte {
	t.Helper()
	b := make([]byte, size)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return b
}

func TestCopyChunkBetweenNodes(t *testing.T) {
	src, _ := startNode(t, "src")
	dst, dstAddr := startNode(t, "dst")
	data := randomBytes(t, 3<<20+17)
	if _, _, err := src.Store.Put("copy1", bytes.NewReader(data), ""); err != nil {
		t.Fatal(err)
	}

	if err := src.copyChunk("copy1", dstAddr); err != nil {
		t.Fatalf("copyChunk: %v", err)
	}
	got, err := dst.Store.Get("copy1")
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("el destino no tiene el chunk íntegro: %v", err)
	}
}

func TestCopyChunkRefusesCorruptSource(t *testing.T) {
	src, _ := startNode(t, "src")
	dst, dstAddr := startNode(t, "dst")
	if _, _, err := src.Store.Put("bad1", bytes.NewReader(randomBytes(t, 1<<20)), ""); err != nil {
		t.Fatal(err)
	}
	path := src.Store.chunkPath("bad1")
	raw, _ := os.ReadFile(path)
	raw[10] ^= 0xFF
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}

	if err := src.copyChunk("bad1", dstAddr); err == nil {
		t.Fatalf("no se debe propagar un chunk corrupto")
	}
	if dst.Store.Has("bad1") {
		t.Fatalf("el destino no debe guardar un chunk de una fuente corrupta")
	}
}

func TestReplicateChunkPipeline(t *testing.T) {
	first, firstAddr := startNode(t, "first")
	second, secondAddr := startNode(t, "second")
	third, thirdAddr := startNode(t, "third")
	data := randomBytes(t, 2<<20+5)
	id := types.ChunkID("chain1")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, stream, err := openReplica(ctx, firstAddr, &pb.ChunkHeader{
		ChunkId:    string(id),
		Checksum:   Checksum(data),
		Size:       int64(len(data)),
		Downstream: []string{secondAddr, thirdAddr},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := stream.Send(dataMessage(data)); err != nil {
		t.Fatal(err)
	}
	resp, err := stream.CloseAndRecv()
	if err != nil || len(resp.GetAckedNodes()) != 3 {
		t.Fatalf("esperaba 3 confirmaciones: %v %v", resp.GetAckedNodes(), err)
	}
	for _, n := range []*DataNode{first, second, third} {
		if got, err := n.Store.Get(id); err != nil || !bytes.Equal(got, data) {
			t.Fatalf("%s no tiene el chunk íntegro: %v", n.NodeID, err)
		}
	}
}
