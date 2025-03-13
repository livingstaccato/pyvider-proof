#!/usr/bin/env python3
# components/resources/database_connection.py

import json
import time
import hashlib
import uuid
import re
from typing import Dict, List, Optional, Any, Union, Tuple

import attrs

from pyvider.telemetry import logger
from pyvider.exceptions import ResourceError
from pyvider.hub.decorators import register_resource
from pyvider.resources.base import BaseResource
from pyvider.resources.context import ResourceContext
from pyvider.schema.pvfactory import (
    a_str, a_num, a_bool, a_map, a_obj, a_list, s_resource
)

@attrs.define(frozen=True)
class DatabaseConnectionConfig:
    """Configuration for database connection resource with extended capabilities."""
    name: str = attrs.field()
    type: str = attrs.field()
    host: str = attrs.field()
    port: int = attrs.field()
    username: str = attrs.field()
    password: str = attrs.field()
    database: str = attrs.field()
    parameters: Dict[str, str] = attrs.field(factory=dict)
    ssl_enabled: bool = attrs.field(default=False)
    ssl_ca_cert: Optional[str] = attrs.field(default=None)
    ssl_client_cert: Optional[str] = attrs.field(default=None)
    ssl_client_key: Optional[str] = attrs.field(default=None)
    ssl_verify_server: bool = attrs.field(default=True)
    connection_timeout: int = attrs.field(default=30)
    connection_pool_size: int = attrs.field(default=1)
    connection_pool_timeout: int = attrs.field(default=30)
    encrypt_connection: bool = attrs.field(default=False)
    retry_attempts: int = attrs.field(default=3)
    retry_delay: int = attrs.field(default=1)
    tags: Dict[str, str] = attrs.field(factory=dict)

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "name": self.name,
            "type": self.type,
            "host": self.host,
            "port": self.port,
            "username": self.username,
            "password": self.password,
            "database": self.database,
            "parameters": self.parameters,
            "ssl_enabled": self.ssl_enabled,
            "ssl_ca_cert": self.ssl_ca_cert,
            "ssl_client_cert": self.ssl_client_cert,
            "ssl_client_key": self.ssl_client_key,
            "ssl_verify_server": self.ssl_verify_server,
            "connection_timeout": self.connection_timeout,
            "connection_pool_size": self.connection_pool_size,
            "connection_pool_timeout": self.connection_pool_timeout,
            "encrypt_connection": self.encrypt_connection,
            "retry_attempts": self.retry_attempts,
            "retry_delay": self.retry_delay,
            "tags": self.tags
        }

@attrs.define(frozen=True)
class DatabaseConnectionState:
    """State representation of database connection resource with monitoring data."""
    name: str = attrs.field()
    type: str = attrs.field()
    host: str = attrs.field()
    port: int = attrs.field()
    username: str = attrs.field()
    password: str = attrs.field()
    database: str = attrs.field()
    parameters: Dict[str, str] = attrs.field(factory=dict)
    ssl_enabled: bool = attrs.field(default=False)
    connection_id: str = attrs.field(default="")
    connected: bool = attrs.field(default=False)
    last_connected: Optional[str] = attrs.field(default=None)
    connection_count: int = attrs.field(default=0)
    avg_connection_time_ms: float = attrs.field(default=0.0)
    last_error: Optional[str] = attrs.field(default=None)
    healthy: bool = attrs.field(default=False)
    server_version: Optional[str] = attrs.field(default=None)
    server_features: List[str] = attrs.field(factory=list)
    connection_string: Optional[str] = attrs.field(default=None)
    tags: Dict[str, str] = attrs.field(factory=dict)

    def to_dict(self) -> dict:
        """Convert state to dictionary."""
        return {
            "name": self.name,
            "type": self.type,
            "host": self.host,
            "port": self.port,
            "username": self.username,
            "password": self.password,
            "database": self.database,
            "parameters": self.parameters,
            "ssl_enabled": self.ssl_enabled,
            "connection_id": self.connection_id,
            "connected": self.connected,
            "last_connected": self.last_connected,
            "connection_count": self.connection_count,
            "avg_connection_time_ms": self.avg_connection_time_ms,
            "last_error": self.last_error,
            "healthy": self.healthy,
            "server_version": self.server_version,
            "server_features": self.server_features,
            "connection_string": self.connection_string,
            "tags": self.tags
        }

@register_resource("pyvider_database_connection")
class DatabaseConnectionResource(BaseResource["pyvider_database_connection", DatabaseConnectionState, DatabaseConnectionConfig]):
    """Resource for managing database connections with comprehensive monitoring."""

    def __init__(self) -> None:
        schema = self.get_schema()
        super().__init__(schema)
        self.connections = {}  # Mock storage for connections
        self.connection_stats = {}  # Store connection statistics

    @staticmethod
    def get_schema():
        """Create the schema for the database connection resource with advanced options."""
        from pyvider.schema.types import SchemaType  # Import SchemaType directly
        
        schema = s_resource({
            # Use SchemaType constants for attribute types instead of string literals
            "name": a_str(required=True, description="Name of the database connection"),
            "type": a_str(required=True, description="Database type (mysql, postgres, etc.)"),
            "host": a_str(required=True, description="Database host address"),
            "port": a_num(required=True, description="Database port"),
            "username": a_str(required=True, description="Database username"),
            "password": a_str(required=True, sensitive=True, description="Database password"),
            "database": a_str(required=True, description="Database name"),
            "parameters": a_map(a_str(), description="Additional connection parameters"),

            # SSL/TLS configuration
            "ssl_enabled": a_bool(default=False, description="Whether to use SSL/TLS for connection"),
            "ssl_ca_cert": a_str(description="CA certificate for SSL verification"),
            "ssl_client_cert": a_str(description="Client certificate for mutual SSL authentication"),
            "ssl_client_key": a_str(sensitive=True, description="Client private key for mutual SSL authentication"),
            "ssl_verify_server": a_bool(default=True, description="Whether to verify server certificate"),
            
            # Connection behavior
            "connection_timeout": a_num(default=30, description="Connection timeout in seconds"),
            "connection_pool_size": a_num(default=1, description="Size of the connection pool"),
            "connection_pool_timeout": a_num(default=30, description="Timeout for acquiring connection from pool"),
            "encrypt_connection": a_bool(default=False, description="Whether to encrypt the entire connection"),
            
            # Retry configuration
            "retry_attempts": a_num(default=3, description="Number of connection retry attempts"),
            "retry_delay": a_num(default=1, description="Delay between retry attempts in seconds"),
            
            # Tagging
            "tags": a_map(a_str(), description="Tags for resource organization"),
            
            # Computed outputs
            "connection_id": a_str(computed=True, description="Unique identifier for the connection"),
            "connected": a_bool(computed=True, description="Whether the connection is active"),
            "last_connected": a_str(computed=True, description="Timestamp of the last successful connection"),
            "connection_count": a_num(computed=True, description="Total number of successful connections"),
            "avg_connection_time_ms": a_num(computed=True, description="Average connection time in milliseconds"),
            "last_error": a_str(computed=True, description="Last error message if connection failed"),
            "healthy": a_bool(computed=True, description="Whether the connection is healthy"),
            "server_version": a_str(computed=True, description="Database server version"),
            "server_features": a_list(a_str(), computed=True, description="Database server features"),
            "connection_string": a_str(computed=True, description="Formatted connection string (sensitive parts redacted)")
        })

    async def read(self, ctx: ResourceContext[DatabaseConnectionConfig, DatabaseConnectionState]) -> DatabaseConnectionState:
        """Read the current state of the database connection with extended monitoring."""
        logger.debug(f"📖 Reading database connection. Context: {ctx}")

        try:
            # Get connection info from config or state
            name, connection_id = self._extract_connection_info(ctx)

            # Check if connection exists in our mock storage
            conn_key = f"{name}:{connection_id}" if connection_id else name
            connection = self.connections.get(conn_key)
            
            if not connection:
                logger.debug(f"📖 Connection {name} not found")
                return self._create_initial_state(ctx)
            
            # Perform a test connection to update status
            connection_test, stats = await self._test_connection(connection)
            
            # Update the connection with current status
            connection["connected"] = connection_test["success"]
            connection["last_error"] = connection_test.get("error")
            connection["healthy"] = connection_test["success"]
            connection["server_version"] = connection_test.get("server_version")
            connection["server_features"] = connection_test.get("features", [])
            
            # Update connection stats
            if stats:
                old_stats = self.connection_stats.get(conn_key, {
                    "connection_count": 0,
                    "total_time_ms": 0
                })
                
                new_count = old_stats["connection_count"] + 1
                new_total_time = old_stats["total_time_ms"] + stats["connection_time_ms"]
                new_avg_time = new_total_time / new_count
                
                self.connection_stats[conn_key] = {
                    "connection_count": new_count,
                    "total_time_ms": new_total_time
                }
                
                connection["connection_count"] = new_count
                connection["avg_connection_time_ms"] = new_avg_time
            
            # Create redacted connection string
            connection_string = self._generate_connection_string(connection)
            connection["connection_string"] = connection_string
            
            # Return connection state
            return DatabaseConnectionState(**connection)

        except Exception as e:
            if isinstance(e, ResourceError):
                raise
            logger.error(f"📖 Error reading database connection: {e}", exc_info=True)
            raise ResourceError(f"Failed to read database connection: {e}")

    async def plan(self, ctx: ResourceContext[DatabaseConnectionConfig, DatabaseConnectionState]) -> tuple[DatabaseConnectionState, List[str]]:
        """Plan changes to the database connection with validation and testing."""
        logger.debug(f"📋 Planning changes. Context: {ctx}")
        diagnostics = []

        try:
            if ctx.config is None:
                logger.debug("📋 Delete operation detected")
                return None, []

            # Get config and state values
            config_dict = self._extract_config_dict(ctx.config)
            state_dict = self._extract_state_dict(ctx.state) if ctx.state else {}
            
            # Validate configuration
            validation_errors = await self._validate_connection_config(config_dict)
            if validation_errors:
                return None, validation_errors
            
            # Get current values from state or generate new ones
            connection_id = state_dict.get('connection_id', '')
            last_connected = state_dict.get('last_connected')
            connection_count = state_dict.get('connection_count', 0)
            avg_connection_time_ms = state_dict.get('avg_connection_time_ms', 0)
            server_version = state_dict.get('server_version')
            server_features = state_dict.get('server_features', [])
            
            if not connection_id:
                # Generate a new connection ID
                connection_id = hashlib.md5(f"{config_dict['name']}:{uuid.uuid4()}".encode()).hexdigest()

            # Create planned state
            planned_state = DatabaseConnectionState(
                name=config_dict['name'],
                type=config_dict['type'],
                host=config_dict['host'],
                port=config_dict['port'],
                username=config_dict['username'],
                password=config_dict['password'],
                database=config_dict['database'],
                parameters=config_dict.get('parameters', {}),
                ssl_enabled=config_dict.get('ssl_enabled', False),
                connection_id=connection_id,
                connected=True,  # We plan for a successful connection
                last_connected=last_connected,
                connection_count=connection_count,
                avg_connection_time_ms=avg_connection_time_ms,
                healthy=True,
                server_version=server_version,
                server_features=server_features,
                connection_string=self._generate_connection_string(config_dict),
                tags=config_dict.get('tags', {})
            )

            logger.debug(f"📋 Plan complete. State: {planned_state}")
            return planned_state, []

        except Exception as e:
            logger.error(f"📋 Error during planning: {e}", exc_info=True)
            diagnostics.append(f"Planning failed: {e}")
            return None, diagnostics

    async def apply(self, ctx: ResourceContext[DatabaseConnectionConfig, DatabaseConnectionState]) -> tuple[Optional[DatabaseConnectionState], List[str]]:
        """Apply changes to the database connection with robust connection testing."""
        logger.debug(f"🚀 Applying changes. Context: {ctx}")
        diagnostics = []

        try:
            # Handle delete operation
            if ctx.planned_state is None:
                logger.debug("🚀 Delete operation detected")
                await self.delete(ctx)
                return None, []

            # Get planned state values
            planned_dict = self._extract_state_dict(ctx.planned_state)
            
            # Generate ID if needed
            if not planned_dict.get('connection_id'):
                planned_dict['connection_id'] = hashlib.md5(
                    f"{planned_dict['name']}:{uuid.uuid4()}".encode()
                ).hexdigest()
            
            # Test the connection with retry logic
            config_dict = self._extract_config_dict(ctx.config)
            retry_attempts = config_dict.get('retry_attempts', 3)
            retry_delay = config_dict.get('retry_delay', 1)
            
            success = False
            last_error = None
            stats = None
            
            for attempt in range(retry_attempts):
                logger.debug(f"🚀 Testing connection (attempt {attempt+1}/{retry_attempts})")
                connection_test, test_stats = await self._test_connection(planned_dict)
                
                if connection_test["success"]:
                    success = True
                    stats = test_stats
                    last_error = None
                    server_version = connection_test.get("server_version")
                    server_features = connection_test.get("features", [])
                    break
                else:
                    last_error = connection_test.get("error", "Unknown error")
                    logger.debug(f"🚀 Connection attempt {attempt+1} failed: {last_error}")
                    if attempt < retry_attempts - 1:
                        time.sleep(retry_delay * (2 ** attempt))  # Exponential backoff
            
            if not success:
                error_msg = f"Failed to establish database connection after {retry_attempts} attempts: {last_error}"
                logger.error(f"🚀 {error_msg}")
                diagnostics.append(error_msg)
                
                # Return a state with connected=False to indicate the connection failed
                planned_dict['connected'] = False
                planned_dict['healthy'] = False
                planned_dict['last_error'] = last_error
                return DatabaseConnectionState(**planned_dict), diagnostics
            
            # Update connection timestamp and stats
            from datetime import datetime
            current_time = datetime.now().isoformat()
            
            # Update connection stats
            conn_key = f"{planned_dict['name']}:{planned_dict['connection_id']}"
            old_stats = self.connection_stats.get(conn_key, {
                "connection_count": 0,
                "total_time_ms": 0
            })
            
            new_count = old_stats["connection_count"] + 1
            new_total_time = old_stats["total_time_ms"] + stats["connection_time_ms"]
            new_avg_time = new_total_time / new_count
            
            self.connection_stats[conn_key] = {
                "connection_count": new_count,
                "total_time_ms": new_total_time
            }
            
            # Update connection state
            planned_dict['last_connected'] = current_time
            planned_dict['connected'] = True
            planned_dict['healthy'] = True
            planned_dict['last_error'] = None
            planned_dict['connection_count'] = new_count
            planned_dict['avg_connection_time_ms'] = new_avg_time
            planned_dict['server_version'] = server_version
            planned_dict['server_features'] = server_features
            planned_dict['connection_string'] = self._generate_connection_string(planned_dict)

            # Store connection in our mock storage
            self.connections[conn_key] = planned_dict

            # Create new state
            new_state = DatabaseConnectionState(**planned_dict)
            logger.debug(f"🚀 Successfully applied changes to {planned_dict['name']}")
            return new_state, []

        except Exception as e:
            logger.error(f"🚀 Error applying changes: {e}", exc_info=True)
            diagnostics.append(f"Apply failed: {e}")
            return None, diagnostics

    async def delete(self, ctx: ResourceContext[DatabaseConnectionConfig, DatabaseConnectionState]) -> None:
        """Delete the database connection with proper cleanup."""
        logger.debug(f"🗑️ Deleting database connection. Context: {ctx}")

        try:
            # Get connection info
            name, connection_id = self._extract_connection_info(ctx)

            # Close and remove the connection
            conn_key = f"{name}:{connection_id}" if connection_id else name
            if conn_key in self.connections:
                # Simulate closing connection
                logger.debug(f"🗑️ Closing connection {conn_key}")
                time.sleep(0.2)  # Simulate delay
                
                # Remove from storage and stats
                del self.connections[conn_key]
                if conn_key in self.connection_stats:
                    del self.connection_stats[conn_key]
                    
                logger.debug(f"🗑️ Successfully deleted connection {conn_key}")
            else:
                logger.debug(f"🗑️ Connection {conn_key} not found, nothing to delete")

        except Exception as e:
            logger.error(f"🗑️ Error deleting database connection: {e}")
            raise ResourceError(f"Failed to delete database connection: {e}")

    async def validate(self, config: DatabaseConnectionConfig) -> None:
        """Validate resource configuration."""
        await self._validate_schema(config)
        await self.validate_config(config)

    async def validate_config(self, config: DatabaseConnectionConfig) -> None:
        """Validate resource configuration with comprehensive checks."""
        logger.debug(f"🔍 Validating config: {config}")

        config_dict = self._extract_config_dict(config)
        validation_errors = await self._validate_connection_config(config_dict)
        if validation_errors:
            raise ResourceError(validation_errors[0])

        logger.debug("🔍 Validation passed")

    async def _test_connection(self, config) -> Tuple[Dict[str, Any], Optional[Dict[str, Any]]]:
        """
        Test database connection with detailed diagnostics.
        
        Returns:
            Tuple containing:
            - Dictionary with connection test results
            - Dictionary with connection statistics (if successful)
        """
        logger.debug(f"🔌 Testing connection to {config['type']} database at {config['host']}:{config['port']}")
        
        # Start timing
        start_time = time.time()
        
        # Simulate connection test with different DB types
        db_type = config['type'].lower()
        
        # Connection result defaults
        result = {
            "success": False,
            "error": None,
            "server_version": None,
            "features": []
        }
        
        # Simulate connection failures for specific scenarios
        if config['host'] == 'invalid.host':
            result["error"] = "Host not found"
            return result, None
            
        if config['port'] == 0:
            result["error"] = "Invalid port"
            return result, None
            
        # Type-specific simulation
        if db_type == "mysql":
            # Simulate MySQL connection
            result["success"] = True
            result["server_version"] = "8.0.28"
            result["features"] = ["JSON", "Transactions", "Stored Procedures"]
        elif db_type == "postgres":
            # Simulate PostgreSQL connection
            result["success"] = True
            result["server_version"] = "14.2"
            result["features"] = ["JSONB", "Materialized Views", "Full Text Search"]
        elif db_type == "mongodb":
            # Simulate MongoDB connection
            result["success"] = True
            result["server_version"] = "5.0.6"
            result["features"] = ["Aggregation", "Replication", "Sharding"]
        elif db_type == "sqlserver":
            # Simulate SQL Server connection
            result["success"] = True
            result["server_version"] = "2019"
            result["features"] = ["Always Encrypted", "In-Memory OLTP", "JSON"]
        elif db_type == "oracle":
            # Simulate Oracle connection
            result["success"] = True
            result["server_version"] = "19c"
            result["features"] = ["Multitenant", "In-Memory", "Partitioning"]
        else:
            # Generic success
            result["success"] = True
            result["server_version"] = "Unknown"
            
        # Calculate connection time
        end_time = time.time()
        connection_time_ms = (end_time - start_time) * 1000
        
        # Create stats if successful
        stats = {
            "connection_time_ms": connection_time_ms
        } if result["success"] else None
        
        logger.debug(f"🔌 Connection {'successful' if result['success'] else 'failed'}")
        return result, stats

    async def _validate_connection_config(self, config: Dict[str, Any]) -> List[str]:
        """Validate database connection configuration with detailed checks."""
        errors = []
        
        # Required fields
        required_fields = ['name', 'type', 'host', 'port', 'username', 'password', 'database']
        for field in required_fields:
            if not config.get(field):
                errors.append(f"The '{field}' field cannot be empty")
        
        if errors:
            return errors
        
        # Validate database type
        valid_types = ['mysql', 'postgres', 'mongodb', 'sqlite', 'oracle', 'sqlserver', 'mariadb', 'db2', 'firebird']
        if config['type'].lower() not in valid_types:
            errors.append(f"Invalid database type: {config['type']}. Valid types: {', '.join(valid_types)}")
        
        # Validate port
        if config['port'] <= 0 or config['port'] > 65535:
            errors.append(f"Invalid port number: {config['port']}. Must be between 1 and 65535")
        
        # Validate host
        hostname_regex = r'^(([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\-]*[a-zA-Z0-9])\.)*([A-Za-z0-9]|[A-Za-z0-9][A-Za-z0-9\-]*[A-Za-z0-9])$'
        ip_regex = r'^(\d{1,3}\.){3}\d{1,3}$'
        if not (re.match(hostname_regex, config['host']) or re.match(ip_regex, config['host']) or config['host'] == 'localhost'):
            errors.append(f"Invalid hostname or IP address: {config['host']}")
        
        # Validate SSL configuration
        if config.get('ssl_enabled'):
            if config.get('ssl_verify_server') and not config.get('ssl_ca_cert'):
                errors.append("SSL CA certificate is required when SSL verification is enabled")
                
            if (config.get('ssl_client_cert') and not config.get('ssl_client_key')) or \
               (config.get('ssl_client_key') and not config.get('ssl_client_cert')):
                errors.append("Both client certificate and key are required for mutual SSL authentication")
        
        # Validate connection pool settings
        if config.get('connection_pool_size', 1) < 1:
            errors.append("Connection pool size must be at least 1")
            
        if config.get('connection_pool_timeout', 30) < 1:
            errors.append("Connection pool timeout must be at least 1 second")
        
        # Validate retry settings
        if config.get('retry_attempts', 3) < 0:
            errors.append("Retry attempts cannot be negative")
            
        if config.get('retry_delay', 1) < 0:
            errors.append("Retry delay cannot be negative")
        
        return errors

    def _extract_connection_info(self, ctx: ResourceContext[DatabaseConnectionConfig, DatabaseConnectionState]) -> Tuple[str, str]:
        """Extract database connection name and ID from context."""
        if ctx.state:
            if isinstance(ctx.state, dict):
                name = ctx.state.get('name', '')
                connection_id = ctx.state.get('connection_id', '')
            else:
                name = ctx.state.name
                connection_id = ctx.state.connection_id
        elif ctx.config:
            if isinstance(ctx.config, dict):
                name = ctx.config.get('name', '')
            else:
                name = ctx.config.name
            connection_id = ""
        else:
            raise ResourceError("Neither state nor config provided")
            
        return name, connection_id
        
    def _extract_config_dict(self, config) -> Dict[str, Any]:
        """Extract configuration as dictionary."""
        if isinstance(config, dict):
            return config
        return config.to_dict() if hasattr(config, "to_dict") else vars(config)
        
    def _extract_state_dict(self, state) -> Dict[str, Any]:
        """Extract state as dictionary."""
        if isinstance(state, dict):
            return state
        return state.to_dict() if hasattr(state, "to_dict") else vars(state)
        
    def _create_initial_state(self, ctx: ResourceContext[DatabaseConnectionConfig, DatabaseConnectionState]) -> DatabaseConnectionState:
        """Create initial state when no existing connection is found."""
        if ctx.state:
            if isinstance(ctx.state, dict):
                return DatabaseConnectionState(
                    name=ctx.state.get('name', ''),
                    type=ctx.state.get('type', ''),
                    host=ctx.state.get('host', ''),
                    port=ctx.state.get('port', 0),
                    username=ctx.state.get('username', ''),
                    password=ctx.state.get('password', ''),
                    database=ctx.state.get('database', ''),
                    parameters=ctx.state.get('parameters', {}),
                    ssl_enabled=ctx.state.get('ssl_enabled', False),
                    connection_id=ctx.state.get('connection_id', ''),
                    connected=False,
                    last_connected=ctx.state.get('last_connected')
                )
            else:
                return DatabaseConnectionState(
                    name=ctx.state.name,
                    type=ctx.state.type,
                    host=ctx.state.host,
                    port=ctx.state.port,
                    username=ctx.state.username,
                    password=ctx.state.password,
                    database=ctx.state.database,
                    parameters=ctx.state.parameters,
                    ssl_enabled=ctx.state.ssl_enabled,
                    connection_id=ctx.state.connection_id,
                    connected=False,
                    last_connected=ctx.state.last_connected
                )
        elif ctx.config:
            if isinstance(ctx.config, dict):
                return DatabaseConnectionState(
                    name=ctx.config.get('name', ''),
                    type=ctx.config.get('type', ''),
                    host=ctx.config.get('host', ''),
                    port=ctx.config.get('port', 0),
                    username=ctx.config.get('username', ''),
                    password=ctx.config.get('password', ''),
                    database=ctx.config.get('database', ''),
                    parameters=ctx.config.get('parameters', {}),
                    ssl_enabled=ctx.config.get('ssl_enabled', False),
                    connected=False
                )
            else:
                return DatabaseConnectionState(
                    name=ctx.config.name,
                    type=ctx.config.type,
                    host=ctx.config.host,
                    port=ctx.config.port,
                    username=ctx.config.username,
                    password=ctx.config.password,
                    database=ctx.config.database,
                    parameters=ctx.config.parameters,
                    ssl_enabled=ctx.config.ssl_enabled,
                    connected=False
                )
        else:
            raise ResourceError("Neither state nor config provided")
            
    def _generate_connection_string(self, config: Dict[str, Any]) -> str:
        """Generate a connection string with sensitive data redacted."""
        db_type = config.get("type", "").lower()
        
        if db_type == "mysql" or db_type == "mariadb":
            return f"mysql://{config['username']}:****@{config['host']}:{config['port']}/{config['database']}"
        elif db_type == "postgres":
            return f"postgresql://{config['username']}:****@{config['host']}:{config['port']}/{config['database']}"
        elif db_type == "sqlserver":
            return f"mssql://{config['username']}:****@{config['host']}:{config['port']}/{config['database']}"
        elif db_type == "oracle":
            return f"oracle://{config['username']}:****@{config['host']}:{config['port']}/{config['database']}"
        elif db_type == "mongodb":
            return f"mongodb://{config['username']}:****@{config['host']}:{config['port']}/{config['database']}"
        else:
            # Generic format
            return f"{db_type}://{config['username']}:****@{config['host']}:{config['port']}/{config['database']}"
