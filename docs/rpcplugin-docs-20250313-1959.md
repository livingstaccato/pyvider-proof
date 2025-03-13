# Pyvider RPC Plugin Architecture Guide

*Version 1.0 - March 2025*

## Contents

1. [Executive Summary](#executive-summary)
2. [System Architecture](#system-architecture)
3. [Transport System](#transport-system)
4. [Protocol Implementation](#protocol-implementation)
5. [Security Architecture](#security-architecture)
6. [Client/Server Architecture](#clientserver-architecture)
7. [Implementation Priorities](#implementation-priorities)
8. [Compatibility Strategy](#compatibility-strategy)

---

## Executive Summary

The Pyvider RPC Plugin system provides a comprehensive implementation of Terraform's plugin protocol in Python, enabling Terraform providers to be written natively in Python while maintaining full compatibility with Terraform's Go-based architecture. 

This architecture:
- Implements a complete client/server model for bidirectional communication
- Supports multiple transport mechanisms (TCP and Unix sockets)
- Provides robust security through mTLS
- Handles protocol versioning and backwards compatibility
- Implements proper logging and error handling throughout
- Offers a Go-bridge for seamless integration with Terraform's plugin discovery

The design prioritizes robustness, security, and compatibility while enabling Pythonic implementation patterns through modern Python features like asyncio, attrs, and type hints.

---

## System Architecture

### Overall Design

The Pyvider RPC Plugin architecture is built around a modular, layered design that separates concerns between transport, protocol, and business logic.

### Module Structure

The Pyvider RPC Plugin module is organized into the following key components:

```
pyvider/rpcplugin/
├── __init__.py            # Public API exports
├── exception.py           # Exception hierarchy
├── config.py              # Configuration system
├── handler.py             # RPC handler interface
├── server.py              # Server implementation
├── types.py               # Core type definitions
├── client/                # Client implementation
│   ├── __init__.py        # Client API exports
│   ├── base.py            # Base client class
│   ├── connection.py      # Client connection handling
│   └── types.py           # Client-specific types
├── crypto/                # Cryptography components
│   ├── __init__.py        # Crypto API exports
│   ├── certificate.py     # Certificate management
│   ├── constants.py       # Crypto constants
│   ├── debug.py           # Certificate debugging
│   ├── generators.py      # Key generation utilities
│   └── types.py           # Crypto-specific types
├── handshake.py           # Handshake implementation
├── logger/                # Logging system
│   ├── __init__.py        # Logger API exports
│   ├── base.py            # Logger base classes
│   ├── emoji_matrix.py    # Structured emoji logging
│   ├── formatters.py      # Log formatters
│   └── messages/          # Log message definitions
├── protocol/              # Protocol implementation
│   ├── __init__.py        # Protocol API exports
│   ├── base.py            # Protocol base class
│   ├── grpc_*.proto       # Protocol buffer definitions
│   ├── service.py         # Protocol service implementation
│   └── grpc_*_pb2*.py     # Generated protocol code
└── transport/             # Transport implementations
    ├── __init__.py        # Transport API exports
    ├── base.py            # Transport base class
    ├── tcp.py             # TCP transport implementation
    ├── types.py           # Transport-specific types
    └── unix.py            # Unix socket transport
```

### Key Components

1. **Transport Layer**: Provides communication channels between client and server
2. **Protocol Layer**: Implements the gRPC-based plugin protocol
3. **Security Layer**: Manages certificates, handshake, and authentication
4. **Core Services**: Configuration, logging, and exception handling
5. **Server/Client**: The main server and client implementations

### Component Interactions

The system is designed around a clear separation of concerns, with each component having specific responsibilities:

- **Transport Layer** handles low-level socket communications (TCP or Unix)
- **Protocol Layer** defines service interfaces and serialization formats
- **Security Layer** ensures secure communications via mTLS and magic cookies
- **Server** manages the lifecycle of plugin connections from Terraform
- **Client** provides an API for provider implementations to interact with Terraform

---

## Transport System

### Transport Base Class

At the core of the transport system is the abstract `RPCPluginTransport` base class:

```python
@attrs.define(frozen=False, slots=False)
class RPCPluginTransport(abc.ABC):
    endpoint: str | None = attrs.field(init=False, default=None)

    @abc.abstractmethod
    async def listen(self) -> str: ...

    @abc.abstractmethod
    async def connect(self, endpoint: str) -> None: ...

    @abc.abstractmethod
    async def close(self) -> None: ...
```

This interface defines the essential methods that all transport implementations must provide:

- `listen()`: Starts listening for connections and returns an endpoint string
- `connect(endpoint)`: Connects to a remote endpoint
- `close()`: Closes connections and cleans up resources

### TCP Transport Implementation

The TCP transport implementation uses asyncio for non-blocking IO operations:

Key features of the TCP transport:

- Uses asyncio's `start_server` and `open_connection` for non-blocking operations
- Supports dynamic port allocation when listening
- Includes robust DNS resolution before connection attempts
- Implements proper connection timeout handling
- Uses a structured emoji logging system for comprehensive observability

### Unix Socket Transport Implementation

The Unix socket transport provides an alternative, particularly for local communications:

- Creates and manages Unix domain socket files
- Performs proper cleanup on shutdown
- Handles permission settings for cross-process access
- Manages socket lifecycle including stale socket detection

### Transport Selection and Negotiation

The transport system includes a negotiation mechanism to select the optimal transport method:

```python
async def negotiate_transport(server_transports: list[str]) -> tuple[str, TransportT]:
    """
    (🗣️🚊 Transport Negotiation) Negotiates the transport type with the server and
    creates the appropriate transport instance.

    Returns:
      A tuple of (transport_name, transport_instance).

    Raises:
      TransportError: If no compatible transport can be negotiated.
    """
    if "unix" in server_transports:
        # Unix socket transport is preferred when available
        return "unix", UnixSocketTransport(...)
    elif "tcp" in server_transports:
        return "tcp", TCPSocketTransport()
    else:
        raise TransportError("No supported transport found")
```

The negotiation prioritizes Unix sockets over TCP when available, as Unix sockets typically provide better performance for local communications.

---

## Protocol Implementation

### Plugin Protocol Architecture

The Pyvider RPC Plugin protocol implementation is based on gRPC and follows Terraform's plugin protocol specification. It consists of three core services:



1. **GRPCStdio Service**: Streams stdout/stderr from the plugin to Terraform
2. **GRPCBroker Service**: Enables multiplexing multiple connections 
3. **GRPCController Service**: Manages plugin lifecycle (e.g., shutdown)

### Protocol Base Interface

The protocol system is built around an abstract base class:

```python
class RPCPluginProtocol(ABC, Generic[ServerT, HandlerT]): 
    """
    Abstract base class for defining RPC protocols.
    ServerT: Type of gRPC server
    HandlerT: Type of handler implementation
    """

    @abstractmethod
    def get_grpc_descriptors(self) -> tuple[Any, str]:
        """Returns the protobuf descriptor set and service name."""
        pass

    @abstractmethod
    def add_to_server(self, server: ServerT, handler: HandlerT) -> None:
        """
        Adds the protocol implementation to the gRPC server.
        Args:
            server: The gRPC async server instance
            handler: The handler implementing the RPC methods
        """
        pass
```

This interface ensures that all protocol implementations can be registered with the gRPC server and provide proper descriptors.

### Protocol Service Registration

The protocol system includes a registration mechanism to attach services to the gRPC server:

```python
def register_protocol_service(server, shutdown_event: asyncio.Event) -> None:
    """
    This function is called by your `server.py` to attach all the needed gRPC services.
    """
    # Create the "shared" Stdio service instance
    stdio_service = GRPCStdioService()

    # Initialize the broker + controller
    broker_service = GRPCBrokerService()
    controller_service = GRPCControllerService(shutdown_event, stdio_service)

    # Register them on the server
    add_GRPCStdioServicer_to_server(stdio_service, server)
    add_GRPCBrokerServicer_to_server(broker_service, server)
    add_GRPCControllerServicer_to_server(controller_service, server)
```

This unified registration ensures that all required services are properly attached to the gRPC server.

### Service Protocol Buffers

The system uses protocol buffer definitions for the gRPC services:

1. **grpc_stdio.proto**: Defines the stdio streaming service
2. **grpc_controller.proto**: Defines the controller service for lifecycle management
3. **grpc_broker.proto**: Defines the broker service for connection multiplexing

These proto files are compiled to Python modules using protoc, generating the necessary client and server code for gRPC communication.

---

## Security Architecture

### Certificate Management

The security system includes comprehensive certificate management:



The Certificate system:

- Supports RSA and ECDSA key types with configurable parameters
- Manages trust chains for certificate validation
- Provides comprehensive certificate validation and verification
- Includes debugging tools for certificate inspection
- Handles both self-signed and CA-signed certificates

### Handshake Process

The handshake process ensures secure communication establishment:

```python
async def build_handshake_response(
    plugin_version: int,
    transport_name: str,
    transport: TransportT,
    server_cert: Certificate | None = None,
    port: int | None = None,
) -> str:
    """
    🤝📝✅ Constructs the handshake response string in the format:
    CORE_VERSION|PLUGIN_VERSION|NETWORK|ADDRESS|PROTOCOL|TLS_CERT
    """
    # ... implementation details ...
```

Key aspects of the handshake:

1. **Magic Cookie Validation**: Ensures both sides share the same authentication token
2. **Protocol Version Negotiation**: Selects a compatible protocol version
3. **Transport Negotiation**: Determines the communication channel
4. **Certificate Exchange**: Optionally shares TLS certificates for mTLS

### Mutual TLS Implementation

The system supports Mutual TLS (mTLS) for secure bidirectional authentication:

1. **Certificate Generation**: Creates self-signed certificates when needed
2. **Certificate Validation**: Validates peer certificates against trust chains
3. **TLS Channel Configuration**: Sets up secure gRPC channels with proper options

---

## Client/Server Architecture

### Server Implementation

The server component handles incoming connections from Terraform:





The server implementation highlights:

- Handles the complete plugin lifecycle from startup to shutdown
- Manages signals for graceful termination
- Configures and starts the gRPC server
- Performs handshake with Terraform
- Sets up the appropriate transport and protocol

### Client Implementation

The client component provides the API for Python provider implementations:

```python
@attrs.define
class RPCPluginClient:
    """
    RPCPluginClient updated to interact with the new broker, stdio, and controller services.
    This version:
      • Launches or attaches to a plugin server subprocess.
      • Performs handshake, sets up TLS.
      • Creates a secure gRPC channel.
      • Exposes methods to:
         => read plugin logs (StdioStub.StreamStdio)
         => manage broker subchannels (BrokerStub.StartStream)
         => send shutdown signals (ControllerStub.Shutdown).
    """
    # ... implementation details ...
```

Key client features:

- Manages connections to the server
- Handles certificate management for mTLS
- Provides access to all protocol services
- Supports launching server processes if needed
- Implements clean shutdown procedures

### Connection Management

Both client and server implementations include robust connection management:

```python
@attrs.define(slots=True, frozen=False)
class ClientConnection:
    """
    Represents an active client connection with associated metrics and state.

    This class wraps the asyncio StreamReader and StreamWriter with additional
    functionality for tracking metrics and managing connection state.
    """
    # ... implementation details ...
```

Connection management features:

- Tracks connection state and metrics
- Provides clear interfaces for sending/receiving data
- Ensures proper resource cleanup
- Supports dependency injection for testing

---

## Implementation Priorities

The implementation should proceed in this order:

1. **Core Abstractions**
   - Base interfaces for transport, protocol, and handlers
   - Exception hierarchy
   - Configuration system

2. **Transport Layer**
   - TCP transport implementation
   - Unix socket transport implementation
   - Transport negotiation

3. **Protocol Implementation**
   - Protocol buffer definitions
   - gRPC service implementations
   - Protocol registration

4. **Security System**
   - Certificate management
   - Handshake implementation
   - Magic cookie validation

5. **Client/Server Implementation**
   - Server process lifecycle
   - Client connection management
   - Error handling and recovery

6. **Go-Bridge Integration**
   - Terraform plugin compatibility
   - Process management
   - IO proxying

7. **Testing & Documentation**
   - Unit tests for all components
   - Integration tests with Terraform
   - Documentation and examples

---

## Compatibility Strategy

Maintaining compatibility with Terraform is critical. The following areas require careful attention:

### Terraform Protocol Compatibility

The implementation must faithfully implement Terraform's plugin protocol:

1. **Version Support**: Support multiple protocol versions
2. **Protocol Buffers**: Properly implement all required messages and services
3. **Transport Mechanisms**: Support all required transport types

### Handshake Compatibility

The handshake process must exactly match Terraform's expectations:

1. **Magic Cookie**: Implement the correct magic cookie validation
2. **Response Format**: Ensure the handshake response format is correct
3. **Protocol Negotiation**: Implement proper version negotiation

### Lifecycle Management

Plugin lifecycle must be correctly managed:

1. **Startup Sequence**: Implement proper initialization flow
2. **Shutdown Handling**: Respond correctly to termination signals
3. **Error Recovery**: Handle unexpected conditions gracefully

### Python/Go Interoperability

As a bridge between Python and Go, special attention is needed for:

1. **Data Type Conversion**: Handle type differences between languages
2. **IO Handling**: Manage differences in IO behaviors
3. **Error Propagation**: Translate errors between systems

---

This architecture guide provides a comprehensive overview of the Pyvider RPC Plugin system, highlighting its design, components, and implementation approach. The modular, layered architecture ensures clean separation of concerns while providing the necessary flexibility and extensibility for future enhancements.
