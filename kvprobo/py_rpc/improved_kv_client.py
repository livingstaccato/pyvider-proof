#!/usr/bin/env python3

# examples/kvprobo/improved_kv_client.py

import asyncio
import logging
import os
import sys
import time
import traceback
from pathlib import Path

from pyvider.rpcplugin.logger import logger
from pyvider.rpcplugin.client import RPCPluginClient

from examples.kvprobo.py_rpc.proto import (
    KVProtocol,
    kv_pb2,
    kv_pb2_grpc,
)

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s.%(msecs)03d %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


class KVClient:
    """Client for KV plugin server with improved error handling & diagnostics."""

    def __init__(self, server_path: str) -> None:
        """Initialize KV client.

        Args:
            server_path: Path to KV server executable
        """
        self.server_path = server_path
        self._client = None
        self._stub = None
        self.connection_timeout = 15.0  # Increased timeout

        # Configure environment for plugin - FORCE unix transport for stability
        os.environ.update(
            {
                "PLUGIN_MAGIC_COOKIE_KEY": "BASIC_PLUGIN",
                "PLUGIN_MAGIC_COOKIE": "hello",
                "PLUGIN_PROTOCOL_VERSIONS": "1",
                "PLUGIN_TRANSPORTS": "unix",  # Force Unix for stability
                "PLUGIN_AUTO_MTLS": "true",
                "PYTHONUNBUFFERED": "1",      # Ensure Python output is unbuffered
                "GODEBUG": "asyncpreemptoff=1,panicasync=1", # Improve Go coroutine behavior
            }
        )

    async def start(self) -> None:
        """Connect to the KV server with improved error handling."""
        start_time = time.time()
        try:
            logger.debug(f"🤝 Creating an RPCPluginClient for server path: {self.server_path}")
            
            # Validate server path
            if not os.path.exists(self.server_path):
                logger.error(f"🚨 Server executable not found at {self.server_path}")
                raise FileNotFoundError(f"Server executable not found at {self.server_path}")
            
            if not os.access(self.server_path, os.X_OK):
                logger.error(f"🚨 Server executable is not executable: {self.server_path}")
                raise PermissionError(f"Server executable is not executable: {self.server_path}")
            
            # Create plugin client with explicit environment settings
            self._client = RPCPluginClient(
                command=[self.server_path], 
                config={
                    "plugins": {"kv": KVProtocol()},
                    "env": {
                        # Explicitly setting these in config overrides env vars
                        "PLUGIN_MAGIC_COOKIE_KEY": "BASIC_PLUGIN",
                        "PLUGIN_MAGIC_COOKIE": "hello",
                        "PLUGIN_PROTOCOL_VERSIONS": "1",
                        "PLUGIN_TRANSPORTS": "unix",
                        "PLUGIN_AUTO_MTLS": "true",
                        "PYTHONUNBUFFERED": "1",
                        "GODEBUG": "asyncpreemptoff=1,panicasync=1",
                    }
                }
            )

            # Start a background thread to relay stderr for diagnostics
            self._relay_stderr()
            
            # Start client with explicit timeout
            logger.debug(f"▶️ Starting the client with {self.connection_timeout} second timeout")
            await asyncio.wait_for(self._client.start(), timeout=self.connection_timeout)
            
            # Log connection details
            if hasattr(self._client, "_transport") and self._client._transport:
                transport_type = type(self._client._transport).__name__
                endpoint = getattr(self._client._transport, "endpoint", "unknown")
                logger.debug(f"🤝✅ Connected via {transport_type} to {endpoint}")

            # Add a small delay to ensure transport is ready
            await asyncio.sleep(0.5)

            # Create gRPC stub
            self._stub = kv_pb2_grpc.KVStub(self._client._channel)
            logger.info(f"✅ Connected to KV server successfully in {time.time() - start_time:.3f}s")

        except asyncio.TimeoutError:
            logger.error(f"🚨 Connection to KV server timed out after {time.time() - start_time:.3f}s")
            # Add extra diagnostics
            if self._client and self._client._process and self._client._process.poll() is None:
                logger.debug("📝 Server process is still running")
                if self._client._process.stderr:
                    try:
                        stderr = self._client._process.stderr.read(2048)
                        if stderr:
                            logger.debug(f"📝 Server stderr: {stderr.decode('utf-8', errors='replace')}")
                    except:
                        pass
            await self.close()
            raise

        except Exception as e:
            logger.error(f"🚨 Failed to connect to KV server: {e}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            await self.close()
            raise

    def _relay_stderr(self) -> None:
        """Start a background thread to read and relay stderr from the server process."""
        import threading
        
        def read_stderr():
            while self._client and hasattr(self._client, "_process") and self._client._process:
                try:
                    if not self._client._process or not self._client._process.stderr:
                        break
                    line = self._client._process.stderr.read(1024)
                    if not line:
                        break
                    decoded = line.decode('utf-8', errors='replace').strip()
                    if decoded:
                        logger.debug(f"📝 SERVER: {decoded}")
                except Exception as e:
                    logger.error(f"📝 Error reading stderr: {e}")
                    break
        
        thread = threading.Thread(target=read_stderr, daemon=True)
        thread.start()
        logger.debug("📝 Started background stderr reader")

    async def close(self) -> None:
        """Close the connection with improved cleanup."""
        if self._client:
            logger.debug("🔒 Closing client connection")
            try:
                await self._client.close()
                self._client = None
                self._stub = None
                logger.debug("🔒 Client connection closed successfully")
            except Exception as e:
                logger.error(f"🔒 Error closing client connection: {e}")
                logger.error(traceback.format_exc())

    async def put(self, key: str, value: bytes | str) -> None:
        """Put a value into the KV store.

        Args:
            key: Key to store value under
            value: Value to store (bytes or str)
        """
        if not self._stub:
            raise RuntimeError("Not connected to KV server")

        # Convert string value to bytes if needed
        if isinstance(value, str):
            value = value.encode('utf-8')

        try:
            logger.debug(f"Put request - key={key}, value_size={len(value)}")
            await asyncio.wait_for(
                self._stub.Put(kv_pb2.PutRequest(key=key, value=value)),
                timeout=5.0
            )
            logger.debug(f"Put successful: key={key}")

        except asyncio.TimeoutError:
            logger.error(f"Put timed out: key={key}")
            raise
        except Exception as e:
            logger.error(f"Put failed: key={key}, error={e}")
            raise

    async def get(self, key: str) -> bytes:
        """Get a value from the KV store.

        Args:
            key: Key to retrieve

        Returns:
            Value if found, None if not found
        """
        if not self._stub:
            raise RuntimeError("Not connected to KV server")

        try:
            response = await asyncio.wait_for(
                self._stub.Get(kv_pb2.GetRequest(key=key)),
                timeout=5.0
            )
            value = response.value if response else None
            logger.debug(f"Get successful: key={key}, found={'yes' if value else 'no'}")
            return value

        except asyncio.TimeoutError:
            logger.error(f"Get timed out: key={key}")
            raise
        except Exception as e:
            logger.error(f"Get failed: key={key}, error={e}")
            raise


async def main() -> None:
    """Example usage of KVClient."""
    # Get server path from environment or use default
    default_path = str(Path(__file__).parent / "go-plugin" / "bin" / "kv-go-server")
    server_path = os.environ.get("PLUGIN_SERVER_PATH", default_path)
    
    if not os.path.exists(server_path):
        logger.error(f"🚨 Server executable not found at {server_path}")
        logger.error("Please build it or set PLUGIN_SERVER_PATH environment variable")
        sys.exit(1)
        
    logger.info(f"🚀 Starting KV client with server: {server_path}")

    # Create client
    client = KVClient(server_path)

    try:
        # Connect to server
        logger.info("🔌 Connecting to server...")
        await client.start()
        logger.info("🔌 Connected successfully")

        # Store a value
        test_key = "hello"
        test_value = b"world"
        
        logger.info(f"📝 Putting key={test_key}, value={test_value}")
        await client.put(test_key, test_value)
        logger.info("📝 Put operation successful")

        # Retrieve the value
        logger.info(f"📚 Getting key={test_key}")
        value = await client.get(test_key)
        logger.info(f"📚 Get result: {value.decode() if value else None}")

        if value == test_value:
            logger.info("✅ Test successful! Value matches")
        else:
            logger.error(f"❌ Test failed! Expected {test_value}, got {value}")

    except Exception as e:
        logger.error(f"❌ Error: {e}")
        logger.debug(f"Traceback: {traceback.format_exc()}")
        sys.exit(1)
    finally:
        await client.close()
        logger.info("🏁 Test completed")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("👋 Interrupted by user")
        sys.exit(130)
