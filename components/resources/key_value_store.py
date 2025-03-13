#!/usr/bin/env python3
# components/resources/key_value_store.py

import json
import os
import time
import hashlib
import uuid
from pathlib import Path
from typing import Dict, List, Optional, Any, Set, Union

import attrs

from pyvider.telemetry import logger
from pyvider.exceptions import ResourceError
from pyvider.hub import register_resource
from pyvider.resources.base import BaseResource
from pyvider.resources.context import ResourceContext
from pyvider.schema.pvfactory import (
    a_str, a_num, a_bool, a_map, a_list, s_resource
)

from pyvider.schema.pvfactory import (
    a_str, a_num, a_bool, a_map, a_dyn, s_data_source
)

@attrs.define(frozen=True)
class KeyValueConfig:
    """Configuration for key-value store resource with enhanced features."""
    name: str = attrs.field()
    location: str = attrs.field()
    values: Dict[str, str] = attrs.field(factory=dict)
    encryption_enabled: bool = attrs.field(default=False)
    encryption_key: Optional[str] = attrs.field(default=None)
    ttl_seconds: Optional[int] = attrs.field(default=None)
    backup_enabled: bool = attrs.field(default=False)
    backup_count: int = attrs.field(default=3)
    flatten_json_values: bool = attrs.field(default=False)
    tags: Dict[str, str] = attrs.field(factory=dict)

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "name": self.name,
            "location": self.location,
            "values": self.values,
            "encryption_enabled": self.encryption_enabled,
            "encryption_key": self.encryption_key,
            "ttl_seconds": self.ttl_seconds,
            "backup_enabled": self.backup_enabled,
            "backup_count": self.backup_count,
            "flatten_json_values": self.flatten_json_values,
            "tags": self.tags
        }

@attrs.define(frozen=True)
class KeyValueState:
    """State representation of key-value store resource with monitoring."""
    name: str = attrs.field()
    location: str = attrs.field()
    values: Dict[str, str] = attrs.field(factory=dict)
    store_id: str = attrs.field(default="")
    last_updated: str = attrs.field(default="")
    created_at: str = attrs.field(default="")
    expires_at: Optional[str] = attrs.field(default=None)
    is_encrypted: bool = attrs.field(default=False)
    value_count: int = attrs.field(default=0)
    size_bytes: int = attrs.field(default=0)
    backup_count: int = attrs.field(default=0)
    tags: Dict[str, str] = attrs.field(factory=dict)

    def to_dict(self) -> dict:
        """Convert state to dictionary."""
        return {
            "name": self.name,
            "location": self.location,
            "values": self.values,
            "store_id": self.store_id,
            "last_updated": self.last_updated,
            "created_at": self.created_at,
            "expires_at": self.expires_at,
            "is_encrypted": self.is_encrypted,
            "value_count": self.value_count,
            "size_bytes": self.size_bytes,
            "backup_count": self.backup_count,
            "tags": self.tags
        }

@register_resource("pyvider_key_value_store")
class KeyValueStoreResource(BaseResource["pyvider_key_value_store", KeyValueState, KeyValueConfig]):
    """Resource for managing key-value stores with advanced features."""

    def __init__(self) -> None:
        schema = self.get_schema()
        super().__init__(schema)

    @staticmethod
    def get_schema():
        """Create the schema for the key-value store resource with enhanced capabilities."""
        return s_resource({
            # Base configuration
            "name": a_str(required=True, description="Name of the key-value store"),
            "location": a_str(required=True, description="Directory location to store the data"),
            "values": a_map(a_str(), description="Key-value pairs to store"),
            
            # Security and encryption
            "encryption_enabled": a_bool(default=False, description="Whether to encrypt the stored values"),
            "encryption_key": a_str(sensitive=True, description="Key for encryption (required if encryption_enabled)"),
            
            # Lifecycle management
            "ttl_seconds": a_num(description="Time-to-live in seconds before automatic expiration"),
            "backup_enabled": a_bool(default=False, description="Whether to maintain backups of previous versions"),
            "backup_count": a_num(default=3, description="Number of backups to maintain"),
            
            # Value processing
            "flatten_json_values": a_bool(default=False, description="Whether to flatten JSON values to dot notation"),
            
            # Tagging and metadata
            "tags": a_map(a_str(), description="Tags for resource organization and filtering"),
            
            # Computed outputs
            "store_id": a_str(computed=True, description="Unique identifier for the store"),
            "last_updated": a_str(computed=True, description="Timestamp of the last update"),
            "created_at": a_str(computed=True, description="Timestamp when the store was created"),
            "expires_at": a_str(computed=True, description="Timestamp when the store will expire (if TTL set)"),
            "is_encrypted": a_bool(computed=True, description="Whether the store is encrypted"),
            "value_count": a_num(computed=True, description="Number of key-value pairs in the store"),
            "size_bytes": a_num(computed=True, description="Size of the store in bytes"),
            "backup_count": a_num(computed=True, description="Number of backups currently maintained")
        })

    async def read(self, ctx: ResourceContext[KeyValueConfig, KeyValueState]) -> KeyValueState:
        """Read the current state of the key-value store with advanced monitoring."""
        logger.debug(f"📖 Reading key-value store. Context: {ctx}")

        try:
            # Get the store name and location from config or state
            name, location = self._extract_name_location(ctx)
            
            logger.debug(f"📖 Looking for store {name} at {location}")
            
            # Check if the store file exists
            store_path = Path(location) / f"{name}.json"
            if not store_path.exists():
                logger.debug(f"📖 Store {name} does not exist at {location}")
                return KeyValueState(
                    name=name,
                    location=location,
                    values={},
                    store_id="",
                    last_updated="",
                    created_at="",
                    value_count=0,
                    size_bytes=0
                )

            # Read store data
            try:
                with open(store_path, 'r') as f:
                    data = json.load(f)
                
                # Check if the data is encrypted
                is_encrypted = data.get('is_encrypted', False)
                values = data.get('values', {})
                
                # Decrypt values if needed
                if is_encrypted:
                    # In a real implementation, decrypt the values using the encryption_key
                    # For this example, we'll just indicate that it's encrypted
                    pass
                
                # Calculate size in bytes
                size_bytes = os.path.getsize(store_path)
                
                # Count backups
                backup_count = 0
                if data.get('backup_enabled', False):
                    backup_pattern = f"{name}.*.backup.json"
                    backup_count = len(list(Path(location).glob(backup_pattern)))
                
                # Create state
                state = KeyValueState(
                    name=name,
                    location=location,
                    values=values,
                    store_id=data.get('store_id', ""),
                    last_updated=data.get('last_updated', ""),
                    created_at=data.get('created_at', ""),
                    expires_at=data.get('expires_at', None),
                    is_encrypted=is_encrypted,
                    value_count=len(values),
                    size_bytes=size_bytes,
                    backup_count=backup_count,
                    tags=data.get('tags', {})
                )
                
                logger.debug(f"📖 Successfully read store {name} with {len(values)} values")
                return state
                
            except json.JSONDecodeError:
                logger.error(f"📖 Failed to parse JSON in {store_path}")
                raise ResourceError(f"Invalid JSON in store file {store_path}")

        except Exception as e:
            if isinstance(e, ResourceError):
                raise
            logger.error(f"📖 Error reading key-value store: {e}")
            raise ResourceError(f"Failed to read key-value store: {e}")

    async def plan(self, ctx: ResourceContext[KeyValueConfig, KeyValueState]) -> tuple[KeyValueState, List[str]]:
        """Plan changes to the key-value store with lifecycle management."""
        logger.debug(f"📋 Planning changes. Context: {ctx}")
        diagnostics = []

        try:
            if ctx.config is None:
                logger.debug("📋 Delete operation detected")
                return None, []

            # Extract config values
            config_dict = self._extract_config_dict(ctx.config)
            name = config_dict["name"]
            location = config_dict["location"]
            values = config_dict.get("values", {})
            encryption_enabled = config_dict.get("encryption_enabled", False)
            ttl_seconds = config_dict.get("ttl_seconds")
            backup_enabled = config_dict.get("backup_enabled", False)
            backup_count = config_dict.get("backup_count", 3)
            tags = config_dict.get("tags", {})
            
            # Validate encryption config
            if encryption_enabled and not config_dict.get("encryption_key"):
                diagnostics.append("encryption_key is required when encryption_enabled is true")
                return None, diagnostics

            # Get current state
            if ctx.state:
                state_dict = self._extract_state_dict(ctx.state)
                store_id = state_dict.get('store_id', "")
                created_at = state_dict.get('created_at', "")
            else:
                # Generate a new store ID
                store_id = hashlib.md5(f"{name}:{uuid.uuid4()}".encode()).hexdigest()
                created_at = self._current_timestamp()

            # Calculate TTL and expiration time
            expires_at = None
            if ttl_seconds:
                from datetime import datetime, timedelta
                expiry_time = datetime.now() + timedelta(seconds=ttl_seconds)
                expires_at = expiry_time.isoformat()
            
            # Process JSON flattening if enabled
            if config_dict.get("flatten_json_values", False):
                flattened_values = {}
                for key, value in values.items():
                    try:
                        # If value is JSON string, try to flatten it
                        if isinstance(value, str) and (value.startswith('{') or value.startswith('[')):
                            json_value = json.loads(value)
                            flattened = self._flatten_json(json_value)
                            for flat_key, flat_value in flattened.items():
                                flattened_values[f"{key}.{flat_key}"] = str(flat_value)
                        else:
                            flattened_values[key] = value
                    except json.JSONDecodeError:
                        # Not valid JSON, keep as is
                        flattened_values[key] = value
                values = flattened_values

            # Create planned state
            planned_state = KeyValueState(
                name=name,
                location=location,
                values=values,
                store_id=store_id,
                last_updated=self._current_timestamp(),
                created_at=created_at,
                expires_at=expires_at,
                is_encrypted=encryption_enabled,
                value_count=len(values),
                size_bytes=0,  # Will be calculated during apply
                backup_count=0,  # Will be updated during apply
                tags=tags
            )

            logger.debug(f"📋 Plan complete. State: {planned_state}")
            return planned_state, []

        except Exception as e:
            logger.error(f"📋 Error during planning: {e}")
            diagnostics.append(f"Planning failed: {e}")
            return None, diagnostics

    async def apply(self, ctx: ResourceContext[KeyValueConfig, KeyValueState]) -> tuple[Optional[KeyValueState], List[str]]:
        """Apply changes to the key-value store with backup and encryption support."""
        logger.debug(f"🚀 Applying changes. Context: {ctx}")
        diagnostics = []

        try:
            # Handle delete operation
            if ctx.planned_state is None:
                logger.debug("🚀 Delete operation detected")
                await self.delete(ctx)
                return None, []

            # Extract planned state values
            planned_dict = self._extract_state_dict(ctx.planned_state)
            name = planned_dict["name"]
            location = planned_dict["location"]
            values = planned_dict.get("values", {})
            store_id = planned_dict.get("store_id", "")
            created_at = planned_dict.get("created_at", self._current_timestamp())
            expires_at = planned_dict.get("expires_at")
            is_encrypted = planned_dict.get("is_encrypted", False)
            tags = planned_dict.get("tags", {})
            
            # Extract config values for additional options
            if isinstance(ctx.config, dict):
                config_dict = ctx.config
            else:
                config_dict = ctx.config.to_dict() if hasattr(ctx.config, "to_dict") else vars(ctx.config)
                
            backup_enabled = config_dict.get("backup_enabled", False)
            backup_count = config_dict.get("backup_count", 3)
            encryption_key = config_dict.get("encryption_key")

            # Generate ID if needed
            if not store_id:
                store_id = hashlib.md5(f"{name}:{uuid.uuid4()}".encode()).hexdigest()

            # Create location directory if it doesn't exist
            path = Path(location)
            path.mkdir(parents=True, exist_ok=True)

            # Create store file path
            store_path = path / f"{name}.json"
            
            # Create backup if enabled and file exists
            if backup_enabled and store_path.exists():
                timestamp = int(time.time())
                backup_path = path / f"{name}.{timestamp}.backup.json"
                logger.debug(f"🚀 Creating backup at {backup_path}")
                import shutil
                shutil.copy2(store_path, backup_path)
                
                # Clean up old backups if exceeding backup_count
                backup_pattern = f"{name}.*.backup.json"
                backups = sorted(path.glob(backup_pattern), key=lambda x: x.stat().st_mtime, reverse=True)
                if len(backups) > backup_count:
                    for old_backup in backups[backup_count:]:
                        logger.debug(f"🚀 Removing old backup {old_backup}")
                        old_backup.unlink()
            
            # Encrypt values if needed
            encrypted_values = {}
            if is_encrypted and encryption_key:
                # In a real implementation, encrypt the values using the encryption_key
                # For this example, we'll just mark them as encrypted
                encrypted_values = values
                logger.debug(f"🚀 Values would be encrypted with key {encryption_key[:3]}...")
            else:
                encrypted_values = values
            
            # Prepare data
            last_updated = self._current_timestamp()
            data = {
                "name": name,
                "values": encrypted_values,
                "store_id": store_id,
                "last_updated": last_updated,
                "created_at": created_at,
                "expires_at": expires_at,
                "is_encrypted": is_encrypted,
                "backup_enabled": backup_enabled,
                "backup_count": backup_count,
                "tags": tags
            }
            
            # Write to file
            with open(store_path, 'w') as f:
                json.dump(data, f, indent=2)
                
            # Count actual backups
            backup_pattern = f"{name}.*.backup.json"
            current_backup_count = len(list(path.glob(backup_pattern)))
            
            # Calculate file size
            size_bytes = os.path.getsize(store_path)

            # Create new state
            new_state = KeyValueState(
                name=name,
                location=location,
                values=values,
                store_id=store_id,
                last_updated=last_updated,
                created_at=created_at,
                expires_at=expires_at,
                is_encrypted=is_encrypted,
                value_count=len(values),
                size_bytes=size_bytes,
                backup_count=current_backup_count,
                tags=tags
            )

            logger.debug(f"🚀 Successfully applied changes to {name}")
            return new_state, []

        except Exception as e:
            logger.error(f"🚀 Error applying changes: {e}")
            diagnostics.append(f"Apply failed: {e}")
            return None, diagnostics

    async def delete(self, ctx: ResourceContext[KeyValueConfig, KeyValueState]) -> None:
        """Delete the key-value store and its backups."""
        logger.debug(f"🗑️ Deleting key-value store. Context: {ctx}")

        try:
            # Get store info
            name, location = self._extract_name_location(ctx)

            # Delete the store file
            store_path = Path(location) / f"{name}.json"
            if store_path.exists():
                os.remove(store_path)
                logger.debug(f"🗑️ Successfully deleted store {name}")
                
                # Also delete backups
                backup_pattern = f"{name}.*.backup.json"
                for backup in Path(location).glob(backup_pattern):
                    backup.unlink()
                    logger.debug(f"🗑️ Deleted backup {backup}")
            else:
                logger.debug(f"🗑️ Store {name} does not exist, nothing to delete")

        except Exception as e:
            logger.error(f"🗑️ Error deleting key-value store: {e}")
            raise ResourceError(f"Failed to delete key-value store: {e}")

    async def validate(self, config: KeyValueConfig) -> None:
        """Validate resource configuration."""
        await self._validate_schema(config)
        await self.validate_config(config)

    async def validate_config(self, config: KeyValueConfig) -> None:
        """Validate resource configuration with comprehensive checks."""
        logger.debug(f"🔍 Validating config: {config}")

        if isinstance(config, dict):
            name = config.get('name', '')
            location = config.get('location', '')
            encryption_enabled = config.get('encryption_enabled', False)
            encryption_key = config.get('encryption_key')
            ttl_seconds = config.get('ttl_seconds')
            backup_count = config.get('backup_count', 3)
        else:
            name = config.name
            loca
