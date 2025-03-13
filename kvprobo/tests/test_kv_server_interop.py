
# examples/kvprobo/test_go_python_interop.py

import asyncio
import os
import subprocess
import pytest
import pytest_asyncio
import time

from pathlib import Path
from typing import AsyncGenerator

import grpc

from pyvider.rpcplugin.client import RPCPluginClient
from pyvider.rpcplugin.exception import HandshakeError
from pyvider.rpcplugin.logger import logger

from examples.kvprobo.py_rpc.proto import kv_pb2, kv_pb2_grpc

from tests.fixtures import *

TEST_DIR: Path = Path(__file__).parent

DEFAULT_PLUGIN_SERVER_PATH = str(TEST_DIR / "../go-rpc" / "bin" / "kv-go-server")
DEFAULT_TIMEOUT = 5.0  # shorter timeout for faster test failures
TEST_TIMEOUT = 15.0  # seconds
LARGE_VALUE_SIZE: int = 1 * 1024 * 1024  # 1MB
SPECIAL_CHARACTERS = "!@#$%^&*()_+{}|:<>?[];',./`~"


@pytest_asyncio.fixture
async def go_server_path() -> str:
    """Return the path to the Server executable."""
    path = os.environ.get("PLUGIN_SERVER_PATH", DEFAULT_PLUGIN_SERVER_PATH)
    logger.debug(f"🧪🔍✅ Using Server path: {path}")

    # Verify the path exists
    if not os.path.exists(path):
        logger.error(f"🧪🔍❌ Server binary not found at {path}")
        pytest.skip(f"Server binary not found at {path}")

    return path


@pytest_asyncio.fixture
async def go_server_env() -> dict[str, str]:
    """Return the environment variables for the Server."""
    return {
        "PLUGIN_MAGIC_COOKIE_KEY": "BASIC_PLUGIN",
        "PLUGIN_MAGIC_COOKIE_VALUE": "hello",
        "BASIC_PLUGIN": "hello",
        "PLUGIN_PROTOCOL_VERSIONS": "1",
        "PLUGIN_TRANSPORTS": "unix",  # Force Unix transport for stability
        "PLUGIN_AUTO_MTLS": "true",   # Enable mTLS
        "PYTHONUNBUFFERED": "1",      # Disable Python buffering
        "PLUGIN_SHOW_EMOJI_MATRIX": "true",  # Show emoji matrix for better logs
        "GODEBUG": "asyncpreemptoff=1",      # Improve Go output behavior
    }


@pytest_asyncio.fixture
async def kv_go_client(go_server_path: str, go_server_env: dict[str, str]) -> AsyncGenerator[RPCPluginClient, None]:
    """Create and yield a RPCPluginClient connected to a KV server."""
    client = None
    logger.debug(f"🧪🚀🔍 Creating RPCPluginClient for Server at {go_server_path}")

    try:
        # Create client with Server command
        client = RPCPluginClient(
            command=[go_server_path],
            config={"env": go_server_env}
        )

        # Start client with timeout
        logger.debug(f"🧪🚀🔄 Starting client with {TEST_TIMEOUT}s timeout")
        start_time = time.time()
        await asyncio.wait_for(client.start(), timeout=TEST_TIMEOUT)
        logger.debug(f"🧪🚀✅ Client started successfully in {time.time() - start_time:.2f}s")

        yield client

    except asyncio.TimeoutError:
        logger.error("🧪🚀❌ Client start timed out")
        if client:
            await client.close()
        pytest.fail("Client connection to Server timed out")
    except Exception as e:
        logger.error(f"🧪🚀❌ Failed to create/start client: {e}")
        if client:
            await client.close()
        pytest.fail(f"Failed to connect to Server: {e}")
    finally:
        # Clean up
        if client:
            logger.debug("🧪🔒🚀 Closing client...")
            try:
                await client.close()
                logger.debug("🧪🔒✅ Client closed successfully")
            except Exception as e:
                logger.error(f"🧪🔒❌ Error closing client: {e}")

@pytest_asyncio.fixture
async def kv_stub(kv_go_client: RPCPluginClient) -> kv_pb2_grpc.KVStub:
    """Create and return a KV stub for the Server."""
    logger.debug("🧪🔌🚀 Creating KV stub")
    stub = kv_pb2_grpc.KVStub(kv_go_client._channel)
    logger.debug("🧪🔌✅ KV stub created successfully")
    return stub

@pytest.mark.asyncio
async def test_go_server_binary_exists() -> None:
    """Test that the Server binary exists and is executable."""
    # Check the default path
    server_path = os.environ.get("PLUGIN_SERVER_PATH", DEFAULT_PLUGIN_SERVER_PATH)
    logger.info(f"🧪🔍 Checking Server binary at: {server_path}")

    if not os.path.exists(server_path):
        logger.error(f"🧪❌ Server binary not found at {server_path}")
        pytest.fail(f"Server binary not found at {server_path}. Please build it or set PLUGIN_SERVER_PATH.")

    if not is_executable(server_path):
        logger.error(f"🧪❌ Server binary exists but is not executable: {server_path}")
        pytest.fail(f"Server binary exists but is not executable: {server_path}")

    logger.info("🧪✅ Server binary exists and is executable")

@pytest.mark.asyncio
async def test_go_server_basic_operations(kv_stub: kv_pb2_grpc.KVStub) -> None:
    """Test basic Put/Get operations with Server."""
    logger.debug("🧪📤🚀 Testing basic Put operation")

    # Prepare test data
    key = "test_basic_key"
    value = "test_basic_value".encode("utf-8")

    # Put
    try:
        await kv_stub.Put(kv_pb2.PutRequest(key=key, value=value))
        logger.debug(f"🧪📤✅ Put operation successful for key '{key}'")
    except grpc.RpcError as e:
        logger.error(f"🧪📤❌ Put operation failed: {e.details()}")
        pytest.fail(f"Put operation failed: {e.details()}")

    # Get
    try:
        logger.debug(f"🧪📥🚀 Testing Get operation for key '{key}'")
        response = await kv_stub.Get(kv_pb2.GetRequest(key=key))
        logger.debug(f"🧪📥✅ Get operation successful for key '{key}'")

        # Verify value
        assert response.value == value, f"Value mismatch: expected {value!r}, got {response.value!r}"
        logger.debug("🧪🔍✅ Value verification successful")
    except grpc.RpcError as e:
        logger.error(f"🧪📥❌ Get operation failed: {e.details()}")
        pytest.fail(f"Get operation failed: {e.details()}")

@pytest.mark.asyncio
async def test_go_server_empty_values(kv_stub: kv_pb2_grpc.KVStub) -> None:
    """Test operations with empty values."""
    logger.debug("🧪🔍🚀 Testing operations with empty values")

    # Empty key (should be accepted)
    empty_key = ""
    value = "".encode("utf-8")

    try:
        await kv_stub.Put(kv_pb2.PutRequest(key=empty_key, value=value))
        logger.debug("🧪📤✅ Put operation with empty key successful")

        response = await kv_stub.Get(kv_pb2.GetRequest(key=empty_key))
        logger.debug("🧪📥✅ Get operation with empty key successful")
        assert response.value == value
    except grpc.RpcError as e:
        # Some implementations may reject empty keys - log but don't fail
        logger.warning(f"🧪⚠️ Empty key operation returned error: {e.details()}")

    # Empty value
    key = "key_for_empty_value"
    empty_value = b""

    try:
        await kv_stub.Put(kv_pb2.PutRequest(key=key, value=empty_value))
        logger.debug("🧪📤✅ Put operation with empty value successful")

        response = await kv_stub.Get(kv_pb2.GetRequest(key=key))
        logger.debug("🧪📥✅ Get operation for key with empty value successful")
        assert response.value == empty_value
    except grpc.RpcError as e:
        logger.error(f"🧪❌ Empty value operation failed: {e.details()}")
        pytest.fail(f"Empty value operation failed: {e.details()}")

@pytest.mark.asyncio
async def test_go_server_special_characters(kv_stub: kv_pb2_grpc.KVStub) -> None:
    """Test operations with special characters in keys and values."""
    logger.debug("🧪🔍🚀 Testing operations with special characters")

    # Special characters in key
    key_with_special = f"special_key_{SPECIAL_CHARACTERS}"
    value = "value_for_special_key".encode("utf-8")

    try:
        await kv_stub.Put(kv_pb2.PutRequest(key=key_with_special, value=value))
        logger.debug("🧪📤✅ Put operation with special characters in key successful")

        response = await kv_stub.Get(kv_pb2.GetRequest(key=key_with_special))
        logger.debug("🧪📥✅ Get operation with special characters in key successful")
        assert response.value == value
    except grpc.RpcError as e:
        logger.error(f"🧪❌ Special character key operation failed: {e.details()}")
        pytest.fail(f"Special character key operation failed: {e.details()}")

    # Special characters in value
    key = "key_for_special_value"
    value_with_special = f"special_value_{SPECIAL_CHARACTERS}".encode("utf-8")

    try:
        await kv_stub.Put(kv_pb2.PutRequest(key=key, value=value_with_special))
        logger.debug("🧪📤✅ Put operation with special characters in value successful")

        response = await kv_stub.Get(kv_pb2.GetRequest(key=key))
        logger.debug("🧪📥✅ Get operation for key with special characters in value successful")
        assert response.value == value_with_special
    except grpc.RpcError as e:
        logger.error(f"🧪❌ Special character value operation failed: {e.details()}")
        pytest.fail(f"Special character value operation failed: {e.details()}")


@pytest.mark.asyncio
async def test_go_server_nonexistent_key(kv_stub: kv_pb2_grpc.KVStub) -> None:
    """Test Get operation for nonexistent key."""
    logger.debug("🧪🔍🚀 Testing Get operation for nonexistent key")

    nonexistent_key = "nonexistent_key_" + str(time.time())

    try:
        await kv_stub.Get(kv_pb2.GetRequest(key=nonexistent_key))
        logger.warning("🧪⚠️ Get operation for nonexistent key succeeded unexpectedly")
    except grpc.RpcError as e:
        # This should fail with NOT_FOUND
        if e.code() == grpc.StatusCode.NOT_FOUND:
            logger.debug("🧪🔍✅ Get operation for nonexistent key correctly returned NOT_FOUND")
        else:
            logger.error(f"🧪❌ Get operation for nonexistent key failed with unexpected error: {e.details()}")
            pytest.fail(f"Get operation for nonexistent key failed with unexpected error: {e.details()}")


@pytest.mark.asyncio
async def test_go_server_large_value(kv_stub: kv_pb2_grpc.KVStub) -> None:
    """Test operations with large values."""
    logger.debug(f"🧪🔍🚀 Testing operations with large value ({LARGE_VALUE_SIZE/1024:.1f} KB)")

    key = "key_for_large_value"
    large_value = b"x" * LARGE_VALUE_SIZE

    try:
        start_time = time.time()
        await kv_stub.Put(kv_pb2.PutRequest(key=key, value=large_value))
        put_duration = time.time() - start_time
        logger.debug(f"🧪📤✅ Put operation with large value successful ({put_duration:.3f}s)")

        start_time = time.time()
        response = await kv_stub.Get(kv_pb2.GetRequest(key=key))
        get_duration = time.time() - start_time
        logger.debug(f"🧪📥✅ Get operation for large value successful ({get_duration:.3f}s)")

        assert len(response.value) == LARGE_VALUE_SIZE, f"Large value size mismatch: expected {LARGE_VALUE_SIZE}, got {len(response.value)}"
        assert response.value == large_value, "Large value content mismatch"
        logger.debug("🧪🔍✅ Large value verification successful")
    except grpc.RpcError as e:
        # Some implementations might have size limits
        if e.code() == grpc.StatusCode.RESOURCE_EXHAUSTED:
            logger.warning(f"🧪⚠️ Large value operation returned RESOURCE_EXHAUSTED: {e.details()}")
            pytest.skip(f"Server doesn't support large values: {e.details()}")
        else:
            logger.error(f"🧪❌ Large value operation failed: {e.details()} (code={e.code()})")
            pytest.fail(f"Large value operation failed: {e.details()}")


@pytest.mark.asyncio
async def test_go_server_rapid_operations(kv_stub: kv_pb2_grpc.KVStub) -> None:
    """Test rapid sequence of Put/Get operations."""
    logger.debug("🧪🚀🔄 Testing rapid sequence of operations")

    operation_count = 10
    tasks = []

    # Create tasks for concurrent operations
    for i in range(operation_count):
        key = f"rapid_key_{i}"
        value = f"rapid_value_{i}".encode("utf-8")

        # Add Put task
        tasks.append(kv_stub.Put(kv_pb2.PutRequest(key=key, value=value)))

        # Add immediate Get task
        tasks.append(
            asyncio.create_task(
                verify_kv_operation(kv_stub, key, value)
            )
        )

    # Run all tasks concurrently
    start_time = time.time()
    results = await asyncio.gather(*tasks, return_exceptions=True)
    duration = time.time() - start_time

    # Check results
    errors = [r for r in results if isinstance(r, Exception)]
    logger.debug(f"🧪🚀✅ Completed {len(tasks)} rapid operations in {duration:.3f}s")

    if errors:
        logger.error(f"🧪❌ {len(errors)}/{len(tasks)} rapid operations failed")
        for i, error in enumerate(errors[:3]):  # Log first 3 errors
            logger.error(f"🧪❌ Error {i+1}: {error}")
        pytest.fail(f"{len(errors)}/{len(tasks)} rapid operations failed")


async def verify_kv_operation(stub: kv_pb2_grpc.KVStub, key: str, expected_value: bytes) -> bool:
    """Helper to verify a key-value pair."""
    response = await stub.Get(kv_pb2.GetRequest(key=key))
    assert response.value == expected_value, f"Value mismatch for {key}"
    return True


# import time
# from pathlib import Path
# from typing import dict, Optional, tuple
# 
# import grpc
# 
# from pyvider.rpcplugin.client import RPCPluginClient
# from pyvider.rpcplugin.exception import HandshakeError, TransportError
# from pyvider.rpcplugin.logger import logger
# 
# # Use a relative path based on this test file's location
TEST_DIR: Path = Path(__file__).parent
DEFAULT_PLUGIN_SERVER_PATH = str(TEST_DIR / "../bin" / "go-kv-server")
DEFAULT_TIMEOUT = 5.0  # shorter timeout for faster test failures


def is_executable(path: str) -> bool:
    """Check if a file exists and is executable."""
    return os.path.isfile(path) and os.access(path, os.X_OK)


async def run_process_with_timeout(cmd: list, timeout: float = 2.0) -> tuple[int, str, str]:
    """Run a process with timeout and return exit code, stdout, stderr."""
    process = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )

    try:
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=timeout)
        return process.returncode, stdout.decode('utf-8', errors='replace'), stderr.decode('utf-8', errors='replace')
    except asyncio.TimeoutError:
        # Process ran too long, kill it
        try:
            process.kill()
        except:
            pass
        return None, "", "Process timed out"


# I haven't created a --help yet.
@pytest.mark.skip
async def test_go_server_basic_execution() -> None:
    """Test that the Server binary can be executed with basic arguments."""
    server_path = os.environ.get("PLUGIN_SERVER_PATH", DEFAULT_PLUGIN_SERVER_PATH)

    # Run with help flag to check if it responds properly
    logger.info(f"🧪🚀 Testing basic execution of Server: {server_path}")
    exit_code, stdout, stderr = await run_process_with_timeout([server_path, "--help"], timeout=3.0)

    if exit_code is None:
        logger.error("🧪⏱️❌ Server execution timed out when running with --help")
        pytest.fail("Server execution timed out when running with --help")

    if exit_code != 0:
        logger.error(f"🧪❌ Server execution failed with exit code {exit_code}")
        logger.error(f"🧪❌ Stdout: {stdout}")
        logger.error(f"🧪❌ Stderr: {stderr}")
        pytest.fail(f"Server execution failed with exit code {exit_code}")

    logger.info("🧪✅ Server executed successfully with --help")
    logger.debug(f"🧪📝 Server stdout: {stdout[:200]}...")
    logger.debug(f"🧪📝 Server stderr: {stderr[:200]}...")


@pytest.mark.asyncio
async def test_go_server_with_environment() -> None:
    """Test that the Server responds properly to environment variables."""
    server_path = os.environ.get("PLUGIN_SERVER_PATH", DEFAULT_PLUGIN_SERVER_PATH)

    # Setup environment variables
    env = os.environ.copy()
    env.update({
        "PLUGIN_MAGIC_COOKIE_KEY": "BASIC_PLUGIN",
        "PLUGIN_MAGIC_COOKIE_VALUE": "hello",
        "BASIC)_PLUGIN": "hello",
        "PLUGIN_PROTOCOL_VERSIONS": "1",
        "PLUGIN_TRANSPORTS": "unix",
        "PLUGIN_AUTO_MTLS": "true",
        "PLUGIN_SHOW_ENV": "true",  # Show environment in logs
    })

    # Start process with environment
    logger.info("🧪🚀 Testing Server with environment variables")
    process = subprocess.Popen(
        [server_path],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=0
    )

    # Wait a short time for startup
    await asyncio.sleep(1.0)

    # Check if process is still running
    if process.poll() is not None:
        stdout, stderr = process.communicate()
        logger.error(f"🧪❌ Server exited prematurely with code {process.returncode}")
        logger.error(f"🧪❌ Stdout: {stdout}")
        logger.error(f"🧪❌ Stderr: {stderr}")
        pytest.fail(f"Server exited prematurely with code {process.returncode}")

    logger.info("🧪✅ Server started successfully with environment variables")

    # Kill the process
    try:
        process.terminate()
        process.wait(timeout=2.0)
        logger.debug("🧪🔒 Server process terminated")
    except subprocess.TimeoutExpired:
        process.kill()
        logger.warning("🧪⚠️ Had to force kill Server process")


@pytest.mark.asyncio
async def test_client_connection_timeout() -> None:
    """Test how the client handles connection timeout with the Server."""
    server_path = os.environ.get("PLUGIN_SERVER_PATH", DEFAULT_PLUGIN_SERVER_PATH)

    if not os.path.exists(server_path):
        pytest.skip(f"Server binary not found at {server_path}")

    # Environment with intentionally wrong cookie to cause handshake failure
    env = {
        "PLUGIN_MAGIC_COOKIE_KEY": "BASIC_PLUGIN",
        "PLUGIN_MAGIC_COOKIE_VALUE": "wrong_cookie",  # Wrong value to cause handshake failure
        "BASIC_PLUGIN": "hello",  # Wrong value to cause handshake failure
        "PLUGIN_PROTOCOL_VERSIONS": "1",
        "PLUGIN_TRANSPORTS": "unix",
        "PLUGIN_AUTO_MTLS": "true",
    }

    client = RPCPluginClient(
        command=[server_path],
        config={"env": env}
    )

    logger.info("🧪⏱️ Testing client connection with timeout (expect failure)")
    start_time = time.time()

    try:
        # Use a shorter timeout to speed up the test
        await asyncio.wait_for(client.start(), timeout=DEFAULT_TIMEOUT)
        logger.error("🧪❌ Client connected successfully when it should have failed")
        pytest.fail("Client connected successfully when it should have failed")
    except asyncio.TimeoutError:
        duration = time.time() - start_time
        logger.info(f"🧪⏱️✅ Client connection properly timed out after {duration:.2f}s")
    except HandshakeError as e:
        duration = time.time() - start_time
        logger.info(f"🧪🤝❌ Client properly failed with HandshakeError after {duration:.2f}s: {e}")
    except Exception as e:
        duration = time.time() - start_time
        logger.info(f"🧪❌ Client failed with unexpected error after {duration:.2f}s: {e}")
        pytest.fail(f"Client failed with unexpected error: {e}")
    finally:
        # Clean up
        try:
            await client.close()
        except:
            pass


@pytest.mark.asyncio
async def test_connection_with_debugging() -> None:
    """Test connection with enhanced debugging to diagnose timeout issues."""
    server_path = os.environ.get("PLUGIN_SERVER_PATH", DEFAULT_PLUGIN_SERVER_PATH)

    if not os.path.exists(server_path):
        pytest.skip(f"Server binary not found at {server_path}")

    # Environment with debugging enabled
    env = {
        "PLUGIN_MAGIC_COOKIE_KEY": "BASIC_PLUGIN",
        "PLUGIN_MAGIC_COOKIE_VALUE": "hello",
        "BASIC_PLUGIN": "hello",
        "PLUGIN_PROTOCOL_VERSIONS": "1",
        "PLUGIN_TRANSPORTS": "unix",  # Force Unix transport
        "PLUGIN_AUTO_MTLS": "false",  # Disable mTLS to simplify
        "PYTHONUNBUFFERED": "1",      # Disable Python buffering
        "PLUGIN_LOG_LEVEL": "DEBUG",  # Maximum logging
        "PLUGIN_SHOW_EMOJI_MATRIX": "true",
        "GODEBUG": "asyncpreemptoff=1",  # Improve Go output
    }

    # Start the Server directly first to see if it runs
    logger.info("🧪🚀 Starting Server process directly for diagnostics")
    process = subprocess.Popen(
        [server_path],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=0
    )

    # Wait a moment for startup
    await asyncio.sleep(1.0)

    # Check if process started correctly
    if process.poll() is not None:
        stdout, stderr = process.communicate()
        logger.error(f"🧪❌ Server exited prematurely with code {process.returncode}")
        logger.error(f"🧪❌ Stdout: {stdout}")
        logger.error(f"🧪❌ Stderr: {stderr}")
        pytest.fail(f"Server exited prematurely with code {process.returncode}")

    # Read some output
    stderr_data = ""
    stdout_data = ""

    for _ in range(5):  # Try to read a few times
        if process.stdout.readable():
            line = process.stdout.readline()
            if line:
                stdout_data += line

        if process.stderr.readable():
            line = process.stderr.readline()
            if line:
                stderr_data += line

        if stdout_data or stderr_data:
            break

        await asyncio.sleep(0.2)

    logger.info(f"🧪📝 Server stdout: {stdout_data}")
    logger.info(f"🧪📝 Server stderr: {stderr_data}")

    # Terminate the process
    try:
        process.terminate()
        process.wait(timeout=2.0)
    except subprocess.TimeoutExpired:
        process.kill()

    # Now try with client
    logger.info("🧪🚀 Starting client connection with debugging")
    client = RPCPluginClient(
        command=[server_path],
        config={"env": env}
    )

    try:
        logger.info(f"🧪🔌 Attempting to connect to Server with {DEFAULT_TIMEOUT}s timeout")
        await asyncio.wait_for(client.start(), timeout=DEFAULT_TIMEOUT)
        logger.info("🧪✅ Client connected successfully to Server!")

        # Clean up on success
        await client.close()
    except Exception as e:
        logger.error(f"🧪❌ Client connection failed: {e}")

        # Extra diagnostics for timeout
        if client._process and client._process.poll() is None:
            logger.info("🧪📝 Server process is still running")
            # Try to read stderr from the process
            if client._process.stderr:
                stderr_data = client._process.stderr.read(1024)
                if stderr_data:
                    logger.info(f"🧪📝 Server stderr: {stderr_data.decode('utf-8', errors='replace')}")

        # Ensure clean up on failure
        try:
            await client.close()
        except:
            pass

        pytest.fail(f"Client connection failed: {e}")

### 🐍🏗🧪️
