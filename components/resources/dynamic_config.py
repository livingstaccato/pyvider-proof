#!/usr/bin/env python3
# components/resources/dynamic_config.py

import json
import os
import time
import hashlib
from typing import Any, Dict, List, Optional, Set, Union
from pathlib import Path

import attrs

from pyvider.telemetry import logger
from pyvider.core.base_types import ConfigType, StateType
from pyvider.exceptions import ResourceError
from pyvider.hub import register_resource
from pyvider.resources.base import BaseResource
from pyvider.resources.context import ResourceContext
from pyvider.schema.pvfactory import (
    a_str, a_obj, a_map, a_dyn, a_bool, a_num, s_resource
)

@attrs.define(frozen=True)
class DynamicConfigState:
    """State representation of dynamic configuration."""
    data: Dict[str, Any] = attrs.field(factory=dict)
    config_id: str = attrs.field(default="")
    last_updated: str = attrs.field(default="")
    version: int = attrs.field(default=1)
    checksum: str = attrs.field(default="")
    metadata: Dict[str, Any] = attrs.field(factory=dict)

    def to_dict(self) -> dict:
        """Convert state to dictionary."""
        return {
            "data": self.data,
            "config_id": self.config_id,
            "last_updated": self.last_updated,
            "version": self.version,
            "checksum": self.checksum,
            "metadata": self.metadata
        }

@register_resource("pyvider_dynamic_config")
class DynamicConfigResource(BaseResource["pyvider_dynamic_config", DynamicConfigState, ConfigType]):
    """
    A resource that handles dynamic, nested configurations of arbitrary types.
    Supports advanced validation, type conversion, and structure enforcement.
    """

    def __init__(self) -> None:
        schema = self.get_schema()
        super().__init__(schema)
        self._config_store = {}  # In-memory store for configs

    def get_schema(self):
        """Create the schema for the dynamic config resource with advanced capabilities."""
        return s_resource({
            # Primary data structure
            "data": a_dyn(required=True, description="A nested map or object of arbitrary configuration data"),
            
            # Storage options
            "persistent": a_bool(default=False, description="Whether to persist configuration to disk"),
            "storage_path": a_str(description="Path to store configuration if persistent"),
            "format": a_str(default="json", description="Storage format (json, yaml, or toml)"),
            
            # Validation options
            "schema_validation": a_bool(default=False, description="Whether to validate against a JSON schema"),
            "schema": a_dyn(description="JSON schema to validate against (if schema_validation is true)"),
            "allow_unknown_props": a_bool(default=True, description="Whether to allow properties not defined in schema"),
            
            # Type conversion
            "string_keys": a_bool(default=True, description="Convert all keys to strings"),
            "auto_convert_types": a_bool(default=False, description="Attempt automatic type conversion"),
            
            # Access control
            "readonly": a_bool(default=False, description="Whether the configuration is read-only"),
            "sensitive_paths": a_list(a_str(), description="JSON paths to sensitive data that should be redacted"),
            
            # Metadata
            "metadata": a_map(a_str(), description="User-defined metadata for the configuration"),
            
            # Computed outputs
            "config_id": a_str(computed=True, description="Unique identifier for the configuration"),
            "last_updated": a_str(computed=True, description="Timestamp of the last update"),
            "version": a_num(computed=True, description="Version number that increments with each change"),
            "checksum": a_str(computed=True, description="SHA-256 checksum of the configuration data"),
            "paths": a_list(a_str(), computed=True, description="Available top-level paths in the configuration")
        })

    async def read(self, ctx: ResourceContext) -> DynamicConfigState:
        """Read the current state of the dynamic configuration."""
        logger.debug(f"📖 Reading dynamic config. Context: {ctx}")

        try:
            # Extract config_id from state
            config_id = None
            if ctx.state:
                if isinstance(ctx.state, dict):
                    config_id = ctx.state.get('config_id')
                else:
                    config_id = ctx.state.config_id

            # Check in-memory store first
            if config_id and config_id in self._config_store:
                logger.debug(f"📖 Found config {config_id} in memory store")
                return self._config_store[config_id]

            # Check persistent storage if configured
            if ctx.config and isinstance(ctx.config, dict) and ctx.config.get('persistent'):
                storage_path = ctx.config.get('storage_path')
                if storage_path and config_id:
                    logger.debug(f"📖 Looking for persistent config at {storage_path}")
                    config_path = Path(storage_path) / f"{config_id}.json"
                    if config_path.exists():
                        try:
                            with open(config_path, 'r') as f:
                                data = json.load(f)
                                logger.debug(f"📖 Loaded config from {config_path}")
                                return DynamicConfigState(**data)
                        except json.JSONDecodeError:
                            logger.error(f"📖 Failed to parse JSON in {config_path}")
                            raise ResourceError(f"Invalid JSON in config file {config_path}")

            # If we get here, either no config exists or we couldn't find it
            # Return initial state based on config
            if ctx.config:
                if isinstance(ctx.config, dict):
                    data = ctx.config.get('data', {})
                else:
                    data = ctx.config.data if hasattr(ctx.config, 'data') else {}
                
                # Generate initial state
                initial_state = self._create_initial_state(data, {})
                logger.debug(f"📖 Created initial state for new config")
                return initial_state
            
            # No config or state available
            logger.debug(f"📖 No config or state available, returning empty state")
            return DynamicConfigState()

        except Exception as e:
            if isinstance(e, ResourceError):
                raise
            logger.error(f"📖 Error reading dynamic config: {e}", exc_info=True)
            raise ResourceError(f"Failed to read dynamic config: {e}")

    async def plan(self, ctx: ResourceContext) -> tuple[StateType, List[str]]:
        """Plan changes to the dynamic configuration with validation."""
        logger.debug(f"📋 Planning changes to dynamic config. Context: {ctx}")
        diagnostics = []

        try:
            if ctx.config is None:
                logger.debug("📋 Delete operation detected")
                return None, []

            # Extract data and options from config
            config_dict = self._extract_config_dict(ctx.config)
            data = config_dict.get('data', {})
            
            # Apply schema validation if enabled
            if config_dict.get('schema_validation'):
                schema = config_dict.get('schema')
                allow_unknown = config_dict.get('allow_unknown_props', True)
                
                if schema:
                    logger.debug(f"📋 Validating data against schema")
                    validation_errors = await self._validate_against_schema(data, schema, allow_unknown)
                    if validation_errors:
                        logger.error(f"📋 Schema validation failed: {validation_errors}")
                        return None, validation_errors

            # Apply type conversions if requested
            if config_dict.get('string_keys', True):
                logger.debug(f"📋 Converting all keys to strings")
                data = self._convert_keys_to_strings(data)
                
            if config_dict.get('auto_convert_types', False):
                logger.debug(f"📋 Applying automatic type conversion")
                data = self._auto_convert_types(data)
            
            # Get state information for updating
            current_version = 1
            if ctx.state:
                if isinstance(ctx.state, dict):
                    current_version = ctx.state.get('version', 0) + 1
                    current_config_id = ctx.state.get('config_id')
                else:
                    current_version = ctx.state.version + 1
                    current_config_id = ctx.state.config_id
            else:
                current_version = 1
                current_config_id = None
                
            # Create planned state
            metadata = config_dict.get('metadata', {})
            
            # Merge with existing metadata if available
            if ctx.state and hasattr(ctx.state, 'metadata'):
                existing_metadata = ctx.state.metadata if hasattr(ctx.state, 'metadata') else {}
                metadata = {**existing_metadata, **metadata}
            
            planned_state = self._create_initial_state(
                data=data,
                metadata=metadata,
                config_id=current_config_id,
                version=current_version
            )

            logger.debug(f"📋 Plan complete. Data checksum: {planned_state.checksum}")
            return planned_state, []

        except Exception as e:
            logger.error(f"📋 Error during planning: {e}", exc_info=True)
            diagnostics.append(f"Planning failed: {e}")
            return None, diagnostics

    async def apply(self, ctx: ResourceContext) -> tuple[StateType, List[str]]:
        """Apply changes to the dynamic configuration with persistence support."""
        logger.debug(f"🚀 Applying changes to dynamic config. Context: {ctx}")
        diagnostics = []

        try:
            # Handle delete operation
            if ctx.planned_state is None:
                logger.debug("🚀 Delete operation detected")
                await self.delete(ctx)
                return None, []

            # Extract data from planned state
            if isinstance(ctx.planned_state, dict):
                planned_dict = ctx.planned_state
            else:
                planned_dict = ctx.planned_state.to_dict() if hasattr(ctx.planned_state, 'to_dict') else vars(ctx.planned_state)
            
            data = planned_dict.get('data', {})
            metadata = planned_dict.get('metadata', {})
            version = planned_dict.get('version', 1)
            config_id = planned_dict.get('config_id')
            
            # Generate new config_id if needed
            if not config_id:
                import uuid
                config_id = str(uuid.uuid4())
                logger.debug(f"🚀 Generated new config_id: {config_id}")
                
            # Create new state with updated timestamp and checksum
            new_state = self._create_initial_state(
                data=data,
                metadata=metadata,
                config_id=config_id,
                version=version
            )
            
            # Store in memory
            self._config_store[new_state.config_id] = new_state
            
            # Handle persistent storage if configured
            if ctx.config and isinstance(ctx.config, dict) and ctx.config.get('persistent'):
                storage_path = ctx.config.get('storage_path')
                format_type = ctx.config.get('format', 'json').lower()
                
                if storage_path:
                    logger.debug(f"🚀 Persisting config to {storage_path}")
                    storage_dir = Path(storage_path)
                    storage_dir.mkdir(parents=True, exist_ok=True)
                    
                    # Determine file format and write accordingly
                    if format_type == 'json':
                        file_path = storage_dir / f"{new_state.config_id}.json"
                        with open(file_path, 'w') as f:
                            json.dump(new_state.to_dict(), f, indent=2)
                    elif format_type == 'yaml':
                        try:
                            import yaml
                            file_path = storage_dir / f"{new_state.config_id}.yaml"
                            with open(file_path, 'w') as f:
                                yaml.dump(new_state.to_dict(), f)
                        except ImportError:
                            logger.error("🚀 YAML format requested but PyYAML not installed")
                            diagnostics.append("YAML format requested but PyYAML not installed")
                    elif format_type == 'toml':
                        try:
                            import toml
                            file_path = storage_dir / f"{new_state.config_id}.toml"
                            with open(file_path, 'w') as f:
                                toml.dump(new_state.to_dict(), f)
                        except ImportError:
                            logger.error("🚀 TOML format requested but toml not installed")
                            diagnostics.append("TOML format requested but toml not installed")
                    else:
                        logger.error(f"🚀 Unsupported format: {format_type}")
                        diagnostics.append(f"Unsupported format: {format_type}")

            logger.debug(f"🚀 Config applied successfully. Version: {new_state.version}, Checksum: {new_state.checksum}")
            return new_state, diagnostics

        except Exception as e:
            logger.error(f"🚀 Error applying dynamic config: {e}", exc_info=True)
            diagnostics.append(f"Apply failed: {e}")
            return None, diagnostics

    async def delete(self, ctx: ResourceContext) -> None:
        """Delete the dynamic configuration from memory and persistent storage if applicable."""
        logger.debug(f"🗑️ Deleting dynamic config. Context: {ctx}")

        try:
            # Extract config_id from state
            config_id = None
            if ctx.state:
                if isinstance(ctx.state, dict):
                    config_id = ctx.state.get('config_id')
                else:
                    config_id = ctx.state.config_id
            
            if not config_id:
                logger.debug("🗑️ No config_id found, nothing to delete")
                return
                
            # Remove from in-memory store
            if config_id in self._config_store:
                logger.debug(f"🗑️ Removing config {config_id} from memory store")
                del self._config_store[config_id]
            
            # Check if we need to delete from persistent storage
            if ctx.config and isinstance(ctx.config, dict) and ctx.config.get('persistent'):
                storage_path = ctx.config.get('storage_path')
                if storage_path:
                    # Check for all possible format extensions
                    for ext in ['json', 'yaml', 'yml', 'toml']:
                        file_path = Path(storage_path) / f"{config_id}.{ext}"
                        if file_path.exists():
                            logger.debug(f"🗑️ Deleting persistent config file: {file_path}")
                            file_path.unlink()

        except Exception as e:
            logger.error(f"🗑️ Error deleting dynamic config: {e}")
            raise ResourceError(f"Failed to delete dynamic config: {e}")

    async def validate_config(self, config: ConfigType) -> None:
        """Validate resource configuration with comprehensive checks."""
        logger.debug(f"🔍 Validating dynamic config: {config}")

        if isinstance(config, dict):
            # Ensure data field is present
            if 'data' not in config:
                raise ResourceError("Missing required 'data' field in configuration")

            # Validate data is a dict or list
            if not isinstance(config['data'], (dict, list)):
                raise ResourceError("'data' must be a dictionary or list")
                
            # Validate storage configuration
            if config.get('persistent', False):
                if not config.get('storage_path'):
                    raise ResourceError("'storage_path' is required when 'persistent' is true")
                    
                # Check storage path is valid
                storage_path = Path(config['storage_path'])
                if storage_path.exists() and not storage_path.is_dir():
                    raise ResourceError(f"'storage_path' exists but is not a directory: {storage_path}")
                    
                # Check format is valid    
                format_type = config.get('format', 'json').lower()
                if format_type not in ['json', 'yaml', 'toml']:
                    raise ResourceError(f"Unsupported format: {format_type}. Must be one of: json, yaml, toml")
                    
            # Validate schema configuration
            if config.get('schema_validation', False) and not config.get('schema'):
                raise ResourceError("'schema' is required when 'schema_validation' is true")

            # Validate nested data structure
            self._validate_nested(config['data'], "root")
        else:
            # Handle class instance
            if not hasattr(config, 'data'):
                raise ResourceError("Missing required 'data' attribute in configuration")
                
            # Validate data is a dict or list    
            if not isinstance(config.data, (dict, list)):
                raise ResourceError("'data' must be a dictionary or list")
                
            # Validate nested structure    
            self._validate_nested(config.data, "root")

        logger.debug("🔍 Validation passed")

    def _validate_nested(self, data: Any, path: str) -> None:
        """Recursively validate nested data structures."""
        if isinstance(data, dict):
            for key, value in data.items():
                if not isinstance(key, str):
                    raise ResourceError(f"Invalid key '{key}' at '{path}'. Must be a string.")
                    
                # Validate key format
                if not key.strip():
                    raise ResourceError(f"Empty key at '{path}'")
                    
                self._validate_nested(value, f"{path}.{key}")
        elif isinstance(data, list):
            for index, item in enumerate(data):
                # Validate list element isn't None
                if item is None:
                    raise ResourceError(f"Null value at '{path}[{index}]'")
                    
                self._validate_nested(item, f"{path}[{index}]")

    def _extract_config_dict(self, config) -> Dict[str, Any]:
        """Extract configuration as dictionary."""
        if isinstance(config, dict):
            return config
        return config.to_dict() if hasattr(config, "to_dict") else vars(config)

    async def _validate_against_schema(self, data: Any, schema: Dict[str, Any], allow_unknown: bool) -> List[str]:
        """Validate data against a JSON schema."""
        try:
            # Try to use jsonschema if available
            try:
                import jsonschema
                
                # Configure validator
                validator_cls = jsonschema.validators.validator_for(schema)
                validator = validator_cls(schema)
                
                if not allow_unknown:
                    validator.META_SCHEMA['additionalProperties'] = False
                
                # Collect errors
                errors = []
                for error in validator.iter_errors(data):
                    errors.append(f"{error.message} at {'.'.join([str(x) for x in error.path])}")
                
                return errors
            except ImportError:
                # Fallback to simple validation
                logger.warning("📋 jsonschema not installed, falling back to simple validation")
                return self._simple_schema_validation(data, schema, allow_unknown)
                
        except Exception as e:
            logger.error(f"📋 Schema validation error: {e}")
            return [f"Schema validation error: {e}"]

    def _simple_schema_validation(self, data: Any, schema: Dict[str, Any], allow_unknown: bool) -> List[str]:
        """Simple schema validation for when jsonschema isn't available."""
        errors = []
        
        # Basic type checking
        if 'type' in schema:
            schema_type = schema['type']
            if schema_type == 'object' and not isinstance(data, dict):
                errors.append(f"Expected object, got {type(data).__name__}")
            elif schema_type == 'array' and not isinstance(data, list):
                errors.append(f"Expected array, got {type(data).__name__}")
            elif schema_type == 'string' and not isinstance(data, str):
                errors.append(f"Expected string, got {type(data).__name__}")
            elif schema_type == 'number' and not isinstance(data, (int, float)):
                errors.append(f"Expected number, got {type(data).__name__}")
            elif schema_type == 'boolean' and not isinstance(data, bool):
                errors.append(f"Expected boolean, got {type(data).__name__}")
        
        # Check required properties
        if 'required' in schema and isinstance(data, dict):
            for prop in schema.get('required', []):
                if prop not in data:
                    errors.append(f"Missing required property: {prop}")
        
        # Check properties
        if 'properties' in schema and isinstance(data, dict):
            for prop_name, prop_schema in schema.get('properties', {}).items():
                if prop_name in data:
                    # Recursively validate property
                    prop_errors = self._simple_schema_validation(data[prop_name], prop_schema, allow_unknown)
                    errors.extend([f"{prop_name}.{err}" for err in prop_errors])
            
            # Check for unknown properties
            if not allow_unknown:
                known_props = set(schema.get('properties', {}).keys())
                unknown_props = set(data.keys()) - known_props
                for prop in unknown_props:
                    errors.append(f"Unknown property: {prop}")
        
        # Check array items
        if 'items' in schema and isinstance(data, list):
            item_schema = schema['items']
            for i, item in enumerate(data):
                item_errors = self._simple_schema_validation(item, item_schema, allow_unknown)
                errors.extend([f"[{i}].{err}" for err in item_errors])
        
        return errors

    def _convert_keys_to_strings(self, data: Any) -> Any:
        """Recursively convert all dictionary keys to strings."""
        if isinstance(data, dict):
            return {str(k): self._convert_keys_to_strings(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [self._convert_keys_to_strings(item) for item in data]
        else:
            return data

    def _auto_convert_types(self, data: Any) -> Any:
        """Recursively attempt type conversion for data values."""
        if isinstance(data, dict):
            return {k: self._auto_convert_types(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [self._auto_convert_types(item) for item in data]
        elif isinstance(data, str):
            # Try to convert strings to appropriate types
            if data.lower() == 'true':
                return True
            elif data.lower() == 'false':
                return False
            elif data.lower() == 'null' or data.lower() == 'none':
                return None
                
            # Try to convert to number
            try:
                if '.' in data:
                    return float(data)
                else:
                    val = int(data)
                    # Check if this is actually meant to be a string (e.g., ZIP codes)
                    if data.startswith('0') and len(data) > 1:
                        return data
                    return val
            except (ValueError, TypeError):
                return data
        else:
            return data

    def _create_initial_state(self, data: Any, metadata: Dict[str, Any], config_id: Optional[str] = None, version: int = 1) -> DynamicConfigState:
        """Create a new state with calculated fields."""
        import hashlib
        import uuid
        import json
        from datetime import datetime
        
        # Generate config_id if not provided
        if not config_id:
            config_id = str(uuid.uuid4())
            
        # Calculate checksum
        checksum = hashlib.sha256(json.dumps(data, sort_keys=True).encode()).hexdigest()
        
        # Get current timestamp
        timestamp = datetime.now().isoformat()
        
        # Create new state
        return DynamicConfigState(
            data=data,
            config_id=config_id,
            last_updated=timestamp,
            version=version,
            checksum=checksum,
            metadata=metadata
        )