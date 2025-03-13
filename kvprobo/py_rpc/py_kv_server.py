#!/usr/bin/env python3
"""
py-kv-server.py

This Python key/value (KV) plugin server uses the RPCPluginServer to set up a gRPC
server and implements a file‑based key/value store. Each key/value pair is persisted
in a text file called "kv-data-<key>".

On startup, the server performs a self‑test by executing a Put/Get with a key of
"status" and a value of "pyvider server listening". This validates that the internal
storage functions work correctly and assists during gRPC debugging.
"""

import asyncio
import os
import grpc

from pyvider.rpcplugin.logger import logger
from pyvider.rpcplugin.server import RPCPluginServer

from examples.kvprobo.py_rpc.proto import (
    KVProtocol,
    kv_pb2,
    kv_pb2_grpc,
)


# ------------------------------------------------------------------------------
# Dummy context for self‑testing (to satisfy the context parameter)
# ------------------------------------------------------------------------------
class DummyContext:
    async def abort(self, code, details):
        raise Exception(f"Abort: {code}, {details}")

    def peer(self) -> str:
        return "dummy_peer"

    def auth_context(self):
        return {}


# ------------------------------------------------------------------------------
# Helper: Summarize a text value by showing first and last 32 characters.
# ------------------------------------------------------------------------------
def summarize_text(text: str, length: int = 32) -> str:
    if len(text) <= 2 * length:
        return text
    return f"{text[:length]} ... {text[-length:]}"


# ------------------------------------------------------------------------------
# KVHandler: File‑based KV store with detailed logging
# ------------------------------------------------------------------------------
class KVHandler(kv_pb2_grpc.KVServicer):
    """
    KV service implementation that persists each key/value pair to a file.
    The file is named "kv-data-<key>" and stores the value as plain text.
    Detailed logging is added to both Put and Get methods.
    """

    def __init__(self) -> None:
        logger.debug("🛎️📡✅ KVHandler: Initialized with file‑based persistence.")
        # Add explicit logging of certificate parameters
        if hasattr(self, "_server_cert_obj"):
            cert = self._server_cert_obj._cert
            public_key = cert.public_key()
            if isinstance(public_key, ec.EllipticCurvePublicKey):
                logger.info(f"🔐 Server using curve: {public_key.curve.name}")

    async def Put(
        self, request: kv_pb2.PutRequest, context: grpc.aio.ServicerContext
    ) -> kv_pb2.Empty:
        """
        🛎️📡🚀 Put:
          - Receives a key/value pair.
          - Writes the value (decoded from UTF‑8, with errors replaced) to a text file
            named "kv-data-<key>".
          - Logs the key, full file name, and a summary (first 32 and last 32 characters)
            of the value.
        """
        try:
            key = request.key
            logger.info(f"🛎️📡🚀 Put: Received request for key: '{key}'")
            value_str = request.value.decode("utf-8", errors="replace")
            summary = summarize_text(value_str)
            logger.debug(
                f"🛎️📡📝 Put: Storing key '{key}' with value (summary): {summary}"
            )
            filename = f"/tmp/kv-data-{key}"
            with open(filename, "w", encoding="utf-8") as f:
                f.write(value_str)
            logger.debug(
                f"🛎️📡✅ Put: Successfully stored key '{key}' in file '{filename}'."
            )
            return kv_pb2.Empty()
        except Exception as e:
            logger.error(
                f"🛎️📡❌ Put: Error storing key '{request.key}': {e}",
                extra={"error": str(e)},
            )
            await context.abort(grpc.StatusCode.INTERNAL, str(e))

    async def Get(
        self, request: kv_pb2.GetRequest, context: grpc.aio.ServicerContext
    ) -> kv_pb2.GetResponse:
        """
        🛎️📡🚀 Get:
          - Retrieves the value for the given key by reading the file "kv-data-<key>".
          - Logs the lookup process and displays a summary (first 32 and last 32 characters)
            of the retrieved value.
        """
        try:
            key = request.key
            logger.info(f"🛎️📡🚀 Get: Received request for key: '{key}'")
            filename = f"/tmp/kv-data-{key}"
            logger.debug(f"🛎️📡📝 Get: Looking for file '{filename}' for key '{key}'.")
            if not os.path.exists(filename):
                logger.error(
                    f"🛎️📡❌ Get: Key '{key}' not found (file '{filename}' does not exist)."
                )
                await context.abort(grpc.StatusCode.NOT_FOUND, f"Key not found: {key}")
            with open(filename, "r", encoding="utf-8") as f:
                value_str = f.read()
            summary = summarize_text(value_str)
            logger.debug(
                f"🛎️📡✅ Get: Successfully retrieved key '{key}' with value (summary): {summary}"
            )
            return kv_pb2.GetResponse(value=value_str.encode("utf-8"))
        except Exception as e:
            logger.error(
                f"🛎️📡❌ Get: Error retrieving key '{request.key}': {e}",
                extra={"error": str(e)},
            )
            await context.abort(grpc.StatusCode.INTERNAL, str(e))

    async def _log_request_details(self, context: grpc.aio.ServicerContext) -> None:
        """Log request details (peer and auth context) for debugging."""
        try:
            logger.debug(f"🛎️🧰🔍 Utils: Request from peer: {context.peer()}")
            for k, v in context.auth_context().items():
                logger.debug(f"🛎️🧰🔍 Utils: Auth Context {k}: {v}")
        except Exception as e:
            logger.error(
                f"🛎️🧰❌ Utils: Error logging request details: {e}",
                extra={"error": str(e)},
            )


# ------------------------------------------------------------------------------
# Server entry point
# ------------------------------------------------------------------------------
async def serve() -> None:
    logger.info("🛎️🚀 Starting KV plugin server...")

    # Create an instance of KVHandler.
    kv_handler = KVHandler()

    # Self-Test: Put and then Get with key "status" and value "pyvider server listening"
    dummy_context = DummyContext()
    try:
        test_key = "status"
        test_value = "pyvider server listening"
        logger.info(
            f"🛎️🧪 Self-Test: Executing Put for key '{test_key}' with value '{test_value}'"
        )

        await kv_handler.Put(
            kv_pb2.PutRequest(key=test_key, value=test_value.encode("utf-8")),
            dummy_context,
        )

        logger.info("🛎️🧪 Self-Test: Put executed successfully.")
        logger.info(f"🛎️🧪 Self-Test: Executing Get for key '{test_key}'")

        response = await kv_handler.Get(kv_pb2.GetRequest(key=test_key), dummy_context)

        retrieved = response.value.decode("utf-8")

        logger.info(f"🛎️🧪 Self-Test: Get returned: {retrieved}")

    except Exception as e:
        logger.error(
            f"🛎️🧪 Self-Test: Error during self-test: {e}", extra={"error": str(e)}
        )

    try:
        # Create and configure the RPCPluginServer with KVProtocol.
        logger.debug("🛎️🚀✅ Server: Server started successfully")
        server = RPCPluginServer(
            protocol=KVProtocol(),
            handler=kv_handler,
            config={
                "max_workers": 10,
                "max_message_length": 16 * 1024 * 1024,  # 16 MB
            },
        )

        await server.serve()
        logger.info("🛎️🚀✅ Server: Server started successfully")

        try:
            await server._serving_future
        except asyncio.CancelledError:
            logger.info("🛎️🛑 Received shutdown signal")
        finally:
            await server.stop()
            logger.info("🛎️🛑 Server: Server stopped")

    except Exception as e:
        logger.error(f"🛎️❗ Fatal error: {e}", extra={"error": str(e)})
        raise


if __name__ == "__main__":
    logger.info("-------------------------------------------------")
    logger.info(os.environ.get("PLUGIN_CLIENT_CERT"))
    logger.info("-------------------------------------------------")
    try:
        asyncio.run(serve())
    except KeyboardInterrupt:
        logger.info("🛎️🛑 Server: Server stopped by user")
    except Exception as e:
        logger.error(f"🛎️❗ Server: Server failed: {e}", extra={"error": str(e)})
        raise
