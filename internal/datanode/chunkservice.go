package datanode

import (
	"context"
	"errors"
	"io"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/thomaszambrano/DFS-Topicos-Telematica/internal/pb"
	"github.com/thomaszambrano/DFS-Topicos-Telematica/internal/types"
)

const streamPiece = 1 << 20

type replicaStream = grpc.ClientStreamingClient[pb.ReplicateChunkRequest, pb.ReplicateChunkResponse]

type ChunkServer struct {
	pb.UnimplementedChunkServiceServer
	node *DataNode
}

func NewChunkServer(node *DataNode) *ChunkServer {
	return &ChunkServer{node: node}
}

func openReplica(ctx context.Context, endpoint string, header *pb.ChunkHeader) (*grpc.ClientConn, replicaStream, error) {
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, err
	}
	stream, err := pb.NewChunkServiceClient(conn).ReplicateChunk(ctx)
	if err != nil {
		conn.Close()
		return nil, nil, err
	}
	first := &pb.ReplicateChunkRequest{Payload: &pb.ReplicateChunkRequest_Header{Header: header}}
	if err := stream.Send(first); err != nil {
		conn.Close()
		return nil, nil, err
	}
	return conn, stream, nil
}

func dataMessage(data []byte) *pb.ReplicateChunkRequest {
	return &pb.ReplicateChunkRequest{Payload: &pb.ReplicateChunkRequest_Data{Data: data}}
}

func grpcCode(err error) codes.Code {
	switch {
	case errors.Is(err, ErrCorrupt), errors.Is(err, ErrBadRequest):
		return codes.InvalidArgument
	case errors.Is(err, ErrNoSpace):
		return codes.ResourceExhausted
	default:
		return codes.Internal
	}
}

type putResult struct {
	size int64
	err  error
}

func (c *ChunkServer) ReplicateChunk(stream grpc.ClientStreamingServer[pb.ReplicateChunkRequest, pb.ReplicateChunkResponse]) error {
	d := c.node
	d.inFlight.Add(1)
	defer d.inFlight.Add(-1)

	first, err := stream.Recv()
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "flujo sin cabecera: %v", err)
	}
	header := first.GetHeader()
	if header == nil {
		return status.Error(codes.InvalidArgument, "el primer mensaje debe ser la cabecera")
	}
	id := types.ChunkID(header.GetChunkId())
	if err := checkID(string(id)); err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}

	pr, pw := io.Pipe()
	done := make(chan putResult, 1)
	go func() {
		n, _, err := d.Store.Put(id, pr, header.GetChecksum())
		if err != nil {
			pr.CloseWithError(err)
		} else {
			pr.Close()
		}
		done <- putResult{size: n, err: err}
	}()

	var forward replicaStream
	if next := header.GetDownstream(); len(next) > 0 {
		downstreamHeader := &pb.ChunkHeader{
			ChunkId:    header.GetChunkId(),
			Checksum:   header.GetChecksum(),
			Size:       header.GetSize(),
			Downstream: next[1:],
		}
		conn, opened, err := openReplica(stream.Context(), next[0], downstreamHeader)
		if err != nil {
			log.Printf("[%s] no pude abrir la cadena hacia %s: %v", d.NodeID, next[0], err)
		} else {
			defer conn.Close()
			forward = opened
		}
	}

	var recvErr error
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			recvErr = err
			break
		}
		data := msg.GetData()
		if len(data) == 0 {
			continue
		}
		if _, err := pw.Write(data); err != nil {
			break
		}
		if forward != nil {
			if err := forward.Send(dataMessage(data)); err != nil {
				log.Printf("[%s] la cadena de %s se cortó: %v", d.NodeID, id, err)
				forward = nil
			}
		}
	}
	if recvErr != nil {
		pw.CloseWithError(recvErr)
	} else {
		pw.Close()
	}

	result := <-done
	if result.err != nil {
		log.Printf("[%s] REPLICATE %s falló: %v", d.NodeID, id, result.err)
		return status.Error(grpcCode(result.err), result.err.Error())
	}

	acked := []string{d.NodeID}
	if forward != nil {
		resp, err := forward.CloseAndRecv()
		if err != nil {
			log.Printf("[%s] la cadena de %s no confirmó: %v", d.NodeID, id, err)
		} else {
			acked = append(acked, resp.GetAckedNodes()...)
		}
	}
	log.Printf("[%s] REPLICATE %s ok: %d bytes", d.NodeID, id, result.size)
	return stream.SendAndClose(&pb.ReplicateChunkResponse{AckedNodes: acked, Size: result.size})
}

func (d *DataNode) copyChunk(id types.ChunkID, target string) error {
	if err := d.Store.Verify(id); err != nil {
		return err
	}
	f, size, sum, err := d.Store.Open(id)
	if err != nil {
		return err
	}
	defer f.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	conn, stream, err := openReplica(ctx, target, &pb.ChunkHeader{
		ChunkId:  string(id),
		Checksum: sum,
		Size:     size,
	})
	if err != nil {
		return err
	}
	defer conn.Close()

	buf := make([]byte, streamPiece)
	for {
		n, readErr := f.Read(buf)
		if n > 0 {
			piece := make([]byte, n)
			copy(piece, buf[:n])
			if err := stream.Send(dataMessage(piece)); err != nil {
				return err
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return readErr
		}
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		return err
	}
	if resp.GetSize() != size {
		return errors.New("el destino guardó un tamaño distinto al enviado")
	}
	return nil
}
