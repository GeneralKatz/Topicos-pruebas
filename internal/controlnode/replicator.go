package controlnode

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/thomaszambrano/DFS-Topicos-Telematica/internal/pb"
	"github.com/thomaszambrano/DFS-Topicos-Telematica/internal/types"
)

const (
	repairInterval = 5 * time.Second
	pendingTTL     = 45 * time.Second
)

type Replicator struct {
	ns    *Namespace
	reg   *Registry
	alloc *Allocator

	mu       sync.Mutex
	commands map[types.NodeID][]*pb.Command
	pending  map[types.ChunkID]map[types.NodeID]time.Time
}

func NewReplicator(ns *Namespace, reg *Registry, alloc *Allocator) *Replicator {
	return &Replicator{
		ns:       ns,
		reg:      reg,
		alloc:    alloc,
		commands: make(map[types.NodeID][]*pb.Command),
		pending:  make(map[types.ChunkID]map[types.NodeID]time.Time),
	}
}

func (rp *Replicator) Drain(id types.NodeID) []*pb.Command {
	rp.mu.Lock()
	defer rp.mu.Unlock()
	out := rp.commands[id]
	delete(rp.commands, id)
	return out
}

func (rp *Replicator) Run(ctx context.Context) {
	ticker := time.NewTicker(repairInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			rp.tick()
		}
	}
}

func (rp *Replicator) tick() {
	want := rp.alloc.ReplicationFactor()
	current := rp.ns.CurrentChunks()
	now := time.Now()

	rp.mu.Lock()
	pending := make(map[types.ChunkID]map[types.NodeID]struct{})
	for chunk, nodes := range rp.pending {
		for id, at := range nodes {
			if now.Sub(at) > pendingTTL {
				delete(nodes, id)
				continue
			}
			if pending[chunk] == nil {
				pending[chunk] = make(map[types.NodeID]struct{})
			}
			pending[chunk][id] = struct{}{}
		}
		if len(nodes) == 0 {
			delete(rp.pending, chunk)
		}
	}
	rp.mu.Unlock()

	tasks := rp.reg.PlanReplication(current, want, pending)
	if len(tasks) == 0 {
		return
	}

	rp.mu.Lock()
	defer rp.mu.Unlock()
	for _, t := range tasks {
		rp.commands[t.Source] = append(rp.commands[t.Source], &pb.Command{
			Type:           pb.CommandType_COMMAND_TYPE_COPY_CHUNK,
			ChunkId:        string(t.Chunk),
			TargetEndpoint: t.TargetGRPC,
		})
		if rp.pending[t.Chunk] == nil {
			rp.pending[t.Chunk] = make(map[types.NodeID]time.Time)
		}
		rp.pending[t.Chunk][t.Target] = now
		log.Printf("REPARAR chunk=%s | %s -> %s | réplicas %d/%d", t.Chunk, t.Source, t.Target, t.Have, want)
	}
}
