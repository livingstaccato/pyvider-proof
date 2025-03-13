#!/usr/bin/env python3
# components/data_sources/env_variables.py

import os
import re
from typing import Any, Dict, List, Optional, Set, Union

import attrs

from pyvider.hub.decorators import register_data_source
from pyvider.telemetry import logger
from pyvider.exceptions import DataSourceError
from pyvider.resources.context import ResourceContext
from pyvider.schema.pvfactory import (
    a_str, a_bool, a_list, a_map, a_num, s_data_source
)

@attrs.define(frozen=True)
class EnvVariablesConfig:
    """Configuration for environment variables data source with enhanced filtering."""
    prefix: Optional[str] = attrs.field(default=None)
    regex: Optional[str] = attrs.field(default=None)
    keys: Optional[List[str]] = attrs.field(default=None)
    sensitive_keys: Set[str] = attrs.field(factory=set)
    exclude_empty: bool = attrs.field(default=False)
    case_sensitive: bool = attrs.field(default=True)
    transform_keys: Optional[str] = attrs.field(default=None)  # none, lower, upper, replace
    transform_values: Optional[str] = attrs.field(default=None)  # none, lower, upper, base64

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "prefix": self.prefix,
            "regex": self.regex,
            "keys": self.keys,
            "sensitive_keys": list(self.sensitive_keys) if self.sensitive_keys else [],
            "exclude_empty": self.exclude_empty,
            "case_sensitive": self.case_sensitive,
            "transform_keys": self.transform_keys,
            "transform_values": self.transform_values
        }

@register_data_source("pyvider_env_variables")
class EnvVariablesDataSource:
    """Data source for reading environment variables with filtering and transformation."""

    @staticmethod
    def get_schema():
        """Create the schema for the environment variables data source with enhanced options."""
        return s_data_source({
            # Filtering options
            "prefix": a_str(description="Filter variables by prefix (e.g., 'AWS_')"),
            "regex": a_str(description="Regular expression to filter variables"),
            "keys": a_list(a_str(), description="Specific environment variable keys to read"),
            "exclude_empty": a_bool(default=False, description="Exclude variables with empty values"),
            "case_sensitive": a_bool(default=True, description="Whether filtering is case-sensitive"),
            
            # Transformation options
            "transform_keys": a_str(description="Key transformation: none, lower, upper, or replace:old=new"),
            "transform_values": a_str(description="Value transformation: none, lower, upper, or base64"),
            
            # Security options
            "sensitive_keys": a_list(a_str(), description="Keys that should be marked as sensitive"),
            
            # Output (computed)
            "values": a_map(a_str(), computed=True, description="Map of environment variables"),
            "count": a_num(computed=True, description="Number of variables returned"),
            "filtered_out": a_num(computed=True, description="Number of variables filtered out"),
            "sensitive_count": a_num(computed=True, description="Number of sensitive variables")
        })

    async def read(self, ctx: ResourceContext) -> Dict[str, Any]:
        """Read environment variables with filtering and transformation."""
        logger.debug(f"📡📖✅ Reading environment variables. Context: {ctx}")

        try:
            # Extract configuration from context
            if isinstance(ctx.config, dict):
                prefix = ctx.config.get('prefix')
                regex_pattern = ctx.config.get('regex')
                keys = ctx.config.get('keys', []) or []
                sensitive_keys = set(ctx.config.get('sensitive_keys', []) or [])
                exclude_empty = ctx.config.get('exclude_empty', False)
                case_sensitive = ctx.config.case_sensitive if ctx.config and hasattr(ctx.config, 'case_sensitive') else True
                transform_keys = ctx.config.transform_keys if ctx.config else None
                transform_values = ctx.config.transform_values if ctx.config else None

            # Compile regex if provided
            regex = None
            if regex_pattern:
                try:
                    flags = 0 if case_sensitive else re.IGNORECASE
                    regex = re.compile(regex_pattern, flags)
                except re.error as e:
                    logger.error(f"📡📖❌ Invalid regex pattern: {e}")
                    raise DataSourceError(f"Invalid regex pattern: {e}")

            # Get all environment variables
            env_vars = dict(os.environ)
            total_vars = len(env_vars)
            result = {}
            filtered_out = 0

            # Filter the environment variables
            if keys:
                logger.debug(f"📡📖🔍 Filtering by keys: {keys}")
                # Match by exact keys
                for k in keys:
                    if not case_sensitive:
                        matching_keys = [env_key for env_key in env_vars if env_key.lower() == k.lower()]
                        for matching_key in matching_keys:
                            if matching_key in env_vars:
                                result[matching_key] = env_vars[matching_key]
                    else:
                        if k in env_vars:
                            result[k] = env_vars[k]
            else:
                # Apply filtering based on prefix and regex
                for key, value in env_vars.items():
                    include = True
                    
                    # Filter by prefix if specified
                    if prefix:
                        if case_sensitive:
                            if not key.startswith(prefix):
                                include = False
                        else:
                            if not key.lower().startswith(prefix.lower()):
                                include = False
                    
                    # Filter by regex if specified
                    if include and regex:
                        if not regex.match(key):
                            include = False
                    
                    # Filter empty values if requested
                    if include and exclude_empty and not value:
                        include = False
                    
                    if include:
                        result[key] = value
                    else:
                        filtered_out += 1

            # Apply key transformations
            if transform_keys:
                transformed_result = {}
                if transform_keys == "lower":
                    for k, v in result.items():
                        transformed_result[k.lower()] = v
                elif transform_keys == "upper":
                    for k, v in result.items():
                        transformed_result[k.upper()] = v
                elif transform_keys.startswith("replace:"):
                    # Format: replace:old=new
                    try:
                        old_new = transform_keys[8:].split('=', 1)
                        if len(old_new) == 2:
                            old, new = old_new
                            for k, v in result.items():
                                transformed_result[k.replace(old, new)] = v
                        else:
                            logger.error(f"📡📖❌ Invalid replace format: {transform_keys}")
                            transformed_result = result
                    except Exception as e:
                        logger.error(f"📡📖❌ Error in key transformation: {e}")
                        transformed_result = result
                else:
                    transformed_result = result
                
                result = transformed_result

            # Apply value transformations
            if transform_values:
                if transform_values == "lower":
                    result = {k: v.lower() if isinstance(v, str) else v for k, v in result.items()}
                elif transform_values == "upper":
                    result = {k: v.upper() if isinstance(v, str) else v for k, v in result.items()}
                elif transform_values == "base64":
                    import base64
                    result = {
                        k: base64.b64encode(v.encode()).decode() if isinstance(v, str) else v 
                        for k, v in result.items()
                    }

            # Count sensitive variables
            sensitive_count = sum(1 for k in result if k in sensitive_keys)

            # Prepare safe result for logging (mask sensitive values)
            safe_result = {
                k: "[SENSITIVE]" if k in sensitive_keys else v
                for k, v in result.items()
            }
            logger.debug(f"📡📖✅ Found {len(result)} environment variables (filtered out {filtered_out})")

            return {
                "prefix": prefix,
                "regex": regex_pattern,
                "keys": keys,
                "sensitive_keys": list(sensitive_keys) if sensitive_keys else [],
                "exclude_empty": exclude_empty,
                "case_sensitive": case_sensitive,
                "transform_keys": transform_keys,
                "transform_values": transform_values,
                "values": result,
                "count": len(result),
                "filtered_out": filtered_out,
                "sensitive_count": sensitive_count
            }

        except Exception as e:
            logger.error(f"📡📖❌ Error reading environment variables: {e}", exc_info=True)
            raise DataSourceError(f"Failed to read environment variables: {e}")

    async def validate(self, config) -> List[str]:
        """Validate data source configuration with comprehensive checks."""
        logger.debug(f"📡🔍✅ Validating config: {config}")
        diagnostics = []

        # Extract configuration
        if isinstance(config, dict):
            regex_pattern = config.get('regex')
            transform_keys = config.get('transform_keys')
            transform_values = config.get('transform_values')
        else:
            regex_pattern = config.regex if hasattr(config, 'regex') else None
            transform_keys = config.transform_keys if hasattr(config, 'transform_keys') else None
            transform_values = config.transform_values if hasattr(config, 'transform_values') else None

        # Validate regex pattern
        if regex_pattern:
            try:
                re.compile(regex_pattern)
            except re.error as e:
                error = f"Invalid regex pattern: {e}"
                diagnostics.append(error)
                logger.error(f"📡🔍❌ {error}")

        # Validate key transformation
        if transform_keys:
            valid_key_transforms = ["lower", "upper"]
            if transform_keys not in valid_key_transforms and not transform_keys.startswith("replace:"):
                error = f"Invalid key transformation: {transform_keys}. Must be one of: {', '.join(valid_key_transforms)}, or replace:old=new"
                diagnostics.append(error)
                logger.error(f"📡🔍❌ {error}")
            
            if transform_keys.startswith("replace:"):
                if "=" not in transform_keys[8:]:
                    error = f"Invalid replace format: {transform_keys}. Must be replace:old=new"
                    diagnostics.append(error)
                    logger.error(f"📡🔍❌ {error}")

        # Validate value transformation
        if transform_values:
            valid_value_transforms = ["lower", "upper", "base64"]
            if transform_values not in valid_value_transforms:
                error = f"Invalid value transformation: {transform_values}. Must be one of: {', '.join(valid_value_transforms)}"
                diagnostics.append(error)
                logger.error(f"📡🔍❌ {error}")

        logger.debug(f"📡🔍✅ Validation complete: {len(diagnostics)} issues found")
        return diagnostics
