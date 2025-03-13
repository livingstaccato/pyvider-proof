#!/usr/bin/env python3


# examples/kvprobo/py_kv_client.py

import asyncio
import logging
import os

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
    """Client for KV plugin server."""

    def __init__(self, server_path: str) -> None:
        """Initialize KV client.

        Args:
            server_path: Path to KV server executable
        """
        self.server_path = server_path
        self._client = None
        self._stub = None

        # Configure environment for plugin
        os.environ.update(
            {
                "PLUGIN_MAGIC_COOKIE_KEY": "BASIC_PLUGIN",
                "PLUGIN_MAGIC_COOKIE": "hello",
                "PLUGIN_PROTOCOL_VERSIONS": "1",
                "PLUGIN_TRANSPORTS": "unix",
                "PLUGIN_AUTO_MTLS": "true",
            }
        )

    async def start(self) -> None:
        """Connect to the KV server."""
        try:
            # Create plugin client
            self._client = RPCPluginClient(
                command=[self.server_path], config={"plugins": {"kv": KVProtocol()}}
            )
            logger.debug("🤝 Created an RPCPluginClient.")

            # Start client and establish connection
            logger.debug("▶️ Starting the client.")
            await self._client.start()

            # Add diagnostic logging
            if hasattr(self._client, "_client_cert"):
                logger.info("🔐 Client certificate generated")

            await self._client.start()
            logger.debug("🤝✅ RPCPluginClient connected to server successfully.")

            # Create gRPC stub
            self._stub = kv_pb2_grpc.KVStub(self._client._channel)
            logger.info("✅ Connected to KV server successfully")

        except Exception as e:
            logger.error(f"🚨 Failed to connect to KV server: {e}")
            await self.close()
            raise

    async def close(self) -> None:
        """Close the connection."""
        if self._client:
            await self._client.close()
            self._client = None
            self._stub = None

    async def put(self, key: str, value: bytes) -> None:
        """Put a value into the KV store.

        Args:
            key: Key to store value under
            value: Value to store
        """
        if not self._stub:
            raise RuntimeError("Not connected to KV server")

        try:
            logger.debug(f"Put request - key={key}, value_size={len(value)}")
            await self._stub.Put(kv_pb2.PutRequest(key=key, value=value))
            logger.debug(f"Put successful: key={key}")

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
            response = await self._stub.Get(kv_pb2.GetRequest(key=key))
            value = response.value if response else None
            logger.debug(f"Get successful: key={key}, found={'yes' if value else 'no'}")
            return value

        except Exception as e:
            logger.error(f"Get failed: key={key}, error={e}")
            raise


async def main() -> None:
    """Example usage of KVClient."""
    # Get server path from environment
    server_path = os.environ.get("PLUGIN_SERVER_PATH")
    if not server_path:
        raise ValueError("PLUGIN_SERVER_PATH environment variable not set")

    # Create client
    client = KVClient(server_path)

    try:
        # Connect to server
        await client.start()

        # Store a value
        await client.put("hello", b"world")

        # Retrieve the value
        value = await client.get("hello")
        print(f"Value: {value.decode() if value else None}")

    except Exception:
        logger.debug("🚨 Could not connect the client.")
        raise Exception
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
