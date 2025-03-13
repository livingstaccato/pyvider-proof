// shared/interface.go
package shared

import (
    "github.com/hashicorp/go-plugin"
)

// Handshake is a common handshake that is shared by plugin and host.
var Handshake = plugin.HandshakeConfig{
    ProtocolVersion:  1,
    MagicCookieKey:   "BASIC_PLUGIN",
    MagicCookieValue: "hello",
}

// KV is the interface that we're exposing as a plugin.
type KV interface {
    Put(key string, value []byte) error
    Get(key string) ([]byte, error)
}

// kvImpl provides a default no-op implementation
type kvImpl struct{}

func (*kvImpl) Put(key string, value []byte) error { return nil }
func (*kvImpl) Get(key string) ([]byte, error)     { return nil, nil }

// KVPlugin is the implementation of plugin.GRPCPlugin so we can serve/consume this.
type KVGRPCPlugin struct {
    // RPCPlugin must still implement the Plugin interface
    plugin.Plugin
    // Concrete implementation, written in Go. This is only used for plugins
    // that are written in Go.
    Impl KV
}

// Add this method
func (p *KVGRPCPlugin) GRPCPlugin() plugin.GRPCPlugin {
    return p
}
