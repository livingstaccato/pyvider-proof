
// pyvider-rpcplugin/examples/kvprobo/go-plugin/plugin-go-server/main.go

package main

import (
    "os"
    "os/signal"
    "sync"
    "syscall"
    "time"

    "strconv"
    "strings"

    "crypto/x509"

    "google.golang.org/grpc"
    // "google.golang.org/grpc/credentials"

    "github.com/hashicorp/go-hclog"
    "github.com/hashicorp/go-plugin"
    "github.com/provide-io/pyvider-rpcplugin/examples/kvprobo/go-plugin/shared"
)

type KV struct {
    logger hclog.Logger
    mu     sync.RWMutex
}

func (k *KV) Put(key string, value []byte) error {
    k.mu.Lock()
    defer k.mu.Unlock()

    if key == "" {
        return nil
    }

    k.logger.Debug("🗄️📤 putting value",
        "key", key,
        "value_length", len(value))

    return os.WriteFile("/tmp/kv-data-"+key, value, 0644)
}

func (k *KV) Get(key string) ([]byte, error) {
    k.mu.RLock()
    defer k.mu.RUnlock()

    if key == "" {
        return nil, nil
    }

    k.logger.Debug("🗄️📥 getting value", "key", key)
    return os.ReadFile("/tmp/kv-data-" + key)
}

func main() {
    logger := hclog.New(&hclog.LoggerOptions{
        Name:       "📡 kv-go-server",
        Level:      hclog.Trace,
        Output:     os.Stderr,
        JSONFormat: false,
    })

    // show some environment variables if `PLUGIN_SHOW_ENV` is `true`
    shared.DisplayFilteredEnv(logger, []string{
        "PLUGIN",
        "GRPC",
        "DEBUG",
    })

    // Determine if AutoMTLS is enabled
    autoMTLS := true // Default to true
    autoMTLSValue := os.Getenv("PLUGIN_AUTO_MTLS")
    if autoMTLSValue != "" {
        autoMTLS, _ = strconv.ParseBool(strings.ToLower(autoMTLSValue))
    }

    if autoMTLS {
        logger.Info("📡🔐 AutoMTLS is enabled. Proceeding with TLS setup...")

        // Load and parse certificate from the environment variable
        certPEM := os.Getenv("PLUGIN_CLIENT_CERT")
        if certPEM == "" {
            logger.Error("📡❌ Certificate not found in PLUGIN_CLIENT_CERT")
            exitWithError()
        }

        // Display certificate details if
        logger.Info("🔌🔐 Client Certificate Details:")
        if shared.DecodeAndLogCertificate(certPEM, logger) != nil {
            exitWithError()
        }

        // Create TLS configuration
        certPool := x509.NewCertPool()
        if !certPool.AppendCertsFromPEM([]byte(certPEM)) {
            logger.Error("📡❌ Failed to append certificate to trust pool")
            exitWithError()
        }

    } else {
        logger.Info("📡🚫 AutoMTLS is disabled. Skipping TLS setup.")
    }

    // Create shutdown channel
    shutdown := make(chan os.Signal, 1)
    signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM)

    // Create KV implementation
    kv := &KV{
        logger: logger.Named("kv"),
        mu:     sync.RWMutex{},
    }

    config := &plugin.ServeConfig{
        HandshakeConfig: shared.Handshake,
        Plugins: map[string]plugin.Plugin{
            "kv_grpc": &shared.KVGRPCPlugin{
                Impl: kv,
            },
        },
        Logger: logger,
        //TLSProvider: tlsConfig,
        GRPCServer: func(opts []grpc.ServerOption) *grpc.Server {
            // Extract and log the certificate
            if autoMTLS {
                logger.Info("🔐⛓️‍💥✅ AutoMTLS support is enabled.")
            }

            return grpc.NewServer(opts...)
        },
    }

    // Start serving in a goroutine
    var wg sync.WaitGroup
    wg.Add(1)

    // Create a channel to signal when the plugin server is done
    serverDone := make(chan struct{})

    go func() {
        defer wg.Done()
        logger.Info("🗄️✨ starting plugin server")
        plugin.Serve(config)
        close(serverDone)
    }()

    // Handle shutdown
    go func() {
        select {
        case sig := <-shutdown:
            logger.Info("🗄️🛑 shutting down plugin server", "signal", sig)
        case <-serverDone:
            logger.Info("🗄️🛑 plugin server exited before receiving a signal")
        }

        cleanup := make(chan struct{})
        go func() {
            wg.Wait()
            close(cleanup)
        }()

        select {
        case <-cleanup:
            logger.Info("🗄️✅ clean shutdown completed")
        case <-time.After(5 * time.Second):
            logger.Warn("🗄️⏳ cleanup timeout reached")
        }

        os.Exit(0)
    }()

    <-serverDone
}

func exitWithError() {
    os.Exit(1)
}
