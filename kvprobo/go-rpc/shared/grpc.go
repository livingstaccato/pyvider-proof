
// pyvider-rpcplugin/examples/kvprobo/go-plugin/shared/grpc.go

package shared

import (
    "context"
    "fmt"

    //"crypto/tls"
    //"crypto/x509"

    "github.com/hashicorp/go-hclog"
    "github.com/hashicorp/go-plugin"
    "google.golang.org/grpc"
    //"google.golang.org/grpc/credentials"

    "github.com/provide-io/pyvider-rpcplugin/examples/kvprobo/go-plugin/proto"
)

// GRPCClient is an implementation of KV that talks over RPC.
type GRPCClient struct {
    client proto.KVClient
    logger hclog.Logger
}

func (p *KVGRPCPlugin) GRPCClient(ctx context.Context, broker *plugin.GRPCBroker, c *grpc.ClientConn) (interface{}, error) {
    logger := hclog.New(&hclog.LoggerOptions{
        Name:  "🔌🌐 kv-grpc-client",
        Level: hclog.Debug,
    })

    if c == nil {
        logger.Error("🌐❌ received nil gRPC connection")
        return nil, fmt.Errorf("nil gRPC connection")
    }

    logger.Debug("🌐🔄 initializing new gRPC client connection",
        "connection_state", c.GetState().String(),
        "target", c.Target())

    grpcClient := &GRPCClient{
        client: proto.NewKVClient(c),
        logger: logger,
    }

    logger.Debug("🌐✨ GRPCClient wrapper initialized successfully",
        "client_implementation", fmt.Sprintf("%T", grpcClient))
    return grpcClient, nil
}

func (m *GRPCClient) Put(key string, value []byte) error {
    m.logger.Debug("🌐📤 initiating Put request",
        "key", key,
        "value_size", len(value))

    _, err := m.client.Put(context.Background(), &proto.PutRequest{
        Key:   key,
        Value: value,
    })

    if err != nil {
        m.logger.Error("🌐❌ Put request failed",
            "key", key,
            "error", err)
        return err
    }

    m.logger.Debug("🌐✅ Put request completed successfully",
        "key", key)
    return nil
}

func (m *GRPCClient) Get(key string) ([]byte, error) {
    m.logger.Debug("🌐📥 initiating Get request", "key", key)

    // Perform the Get operation
    resp, err := m.client.Get(context.Background(), &proto.GetRequest{
        Key: key,
    })
    if err != nil {
        m.logger.Error("🌐❌ Get request failed", "key", key, "error", err)
        return nil, err
    }

    m.logger.Debug("🌐✅ Get request completed successfully", "key", key, "value_size", len(resp.Value))
    return resp.Value, nil
}

// GRPCServer is the gRPC server that GRPCClient talks to.
type GRPCServer struct {
    proto.UnimplementedKVServer
    Impl   KV
    logger hclog.Logger
}

func (p *KVGRPCPlugin) GRPCServer(broker *plugin.GRPCBroker, s *grpc.Server) error {
    logger := hclog.New(&hclog.LoggerOptions{
        Name:  "🔌📡 kv-grpc-server",
        Level: hclog.Debug,
    })

    logger.Debug("📡🔄 initializing gRPC server registration")

    if p.Impl == nil {
        logger.Warn("📡⚠️ no implementation provided, using no-op implementation")
        p.Impl = &kvImpl{}
    }

    server := &GRPCServer{
        Impl:   p.Impl,
        logger: logger,
    }

    proto.RegisterKVServer(s, server)
    logger.Info("📡✅ gRPC server registered successfully",
        "server_type", fmt.Sprintf("%T", server))
    return nil
}

func (m *GRPCServer) Put(ctx context.Context, req *proto.PutRequest) (*proto.Empty, error) {
    m.logger.Debug("📡📤 handling Put request",
        "key", req.Key,
        "value_size", len(req.Value))

    if err := m.Impl.Put(req.Key, req.Value); err != nil {
        m.logger.Error("📡❌ Put operation failed",
            "key", req.Key,
            "error", err)
        return nil, err
    }

    m.logger.Debug("📡✅ Put operation completed successfully",
        "key", req.Key)
    return &proto.Empty{}, nil
}

func (m *GRPCServer) Get(ctx context.Context, req *proto.GetRequest) (*proto.GetResponse, error) {
    m.logger.Debug("📡📥 handling Get request",
        "key", req.Key)

    v, err := m.Impl.Get(req.Key)
    if err != nil {
        m.logger.Error("📡❌ Get operation failed",
            "key", req.Key,
            "error", err)
        return nil, err
    }

    m.logger.Debug("📡✅ Get operation completed successfully",
        "key", req.Key,
        "value_size", len(v))
    return &proto.GetResponse{Value: v}, nil
}
