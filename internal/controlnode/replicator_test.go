package controlnode

import (
	"testing"
	"time"

	"github.com/thomaszambrano/DFS-Topicos-Telematica/internal/pb"
	"github.com/thomaszambrano/DFS-Topicos-Telematica/internal/types"
)

func report(id string, chunks ...string) *pb.NodeReport {
	return &pb.NodeReport{
		NodeId:        id,
		GrpcEndpoint:  id + ":9091",
		CapacityBytes: 10 << 30,
		UsedBytes:     int64(len(chunks)) << 20,
		ChunkIds:      chunks,
	}
}

func TestPlanReplicationRepairsUnderReplicated(t *testing.T) {
	reg := NewRegistry(9*time.Second, 30*time.Second)
	reg.Update(report("n1", "a", "b", "c"))
	reg.Update(report("n2", "a", "b"))
	reg.Update(report("n3", "a"))
	reg.Update(report("n4"))

	tasks := reg.PlanReplication([]types.ChunkID{"a", "b", "c", "z"}, 3, nil)

	if len(tasks) != 2 {
		t.Fatalf("esperaba reparar solo b y c, obtuve %+v", tasks)
	}
	if tasks[0].Chunk != "c" || tasks[0].Source != "n1" || tasks[0].Have != 1 {
		t.Fatalf("el chunk con una sola copia debe ir primero y copiarse desde n1: %+v", tasks[0])
	}
	if tasks[1].Chunk != "b" || (tasks[1].Target != "n3" && tasks[1].Target != "n4") {
		t.Fatalf("b debe copiarse a un nodo que no lo tiene: %+v", tasks[1])
	}
}

func TestPlanReplicationCountsPendingCopies(t *testing.T) {
	reg := NewRegistry(9*time.Second, 30*time.Second)
	reg.Update(report("n1", "a"))
	reg.Update(report("n2", "a"))
	reg.Update(report("n3"))

	pending := map[types.ChunkID]map[types.NodeID]struct{}{"a": {"n3": {}}}
	if tasks := reg.PlanReplication([]types.ChunkID{"a"}, 3, pending); len(tasks) != 0 {
		t.Fatalf("con una copia en curso no se debe ordenar otra: %+v", tasks)
	}
}

func TestAllocatorRejectsWritesWithSingleNode(t *testing.T) {
	reg := NewRegistry(9*time.Second, 30*time.Second)
	reg.Update(report("n1"))
	alloc := NewAllocator(NewNamespace(), NewPlacer(reg), reg, 4<<20, 3, 2, time.Minute)

	if _, err := alloc.OpenUpload("/x", "u", 10, ""); err == nil {
		t.Fatalf("con un solo DataNode y R=3 la escritura debe rechazarse")
	}
	if err := alloc.SetReplicationFactor(1); err != nil {
		t.Fatal(err)
	}
	if _, err := alloc.OpenUpload("/x", "u", 10, ""); err != nil {
		t.Fatalf("con R=1 un solo nodo basta: %v", err)
	}
}
