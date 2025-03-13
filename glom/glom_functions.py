#!/usr/bin/env python3
# components/functions/glom_functions.py

"""
Glom Functions for Terraform

This module implements Terraform functions that support glom operations for extracting,
transforming, and manipulating nested data structures. These functions work with both
native CTY/schema structures and JSON data.
"""

import json
from typing import Any, Dict, List, Union, Optional, Callable

import glom
from glom import glom as glom_extract, Path, Assign, T, Coalesce, Literal, Spec
from glom.core import GlomError, PathAccessError, PathAssignError

from pyvider.hub import register_function
from pyvider.telemetry import logger
from pyvider.cty import (
    CtyString, CtyNumber, CtyBool,
    CtyList, CtyMap, CtySet,
    CtyObject, CtyDynamic
)
from pyvider.exceptions import FunctionError

class Extract:
    """
    Filter elements in a collection based on a predicate.
    
    Args:
        path: Path to the collection
        predicate: Function that returns True/False for each element
        
    Example:
        Extract("items", lambda x: x["active"] == True)
    """
    def __init__(self, path, predicate):
        self.path = path
        self.predicate = predicate
        
    def __call__(self, target):
        logger.debug(f"🧰🔍🔄 Extract filtering collection at {self.path}")
        try:
            # Get the collection using glom
            collection = glom_extract(target, self.path)
            
            # Filter using predicate
            if not isinstance(collection, (list, tuple)):
                logger.warning(f"🧰🔍⚠️ Extract expected list/tuple at {self.path}, got {type(collection).__name__}")
                return []
                
            result = [item for item in collection if self.predicate(item)]
            logger.debug(f"🧰🔍✅ Extract filtered from {len(collection)} to {len(result)} items")
            return result
            
        except Exception as e:
            logger.error(f"🧰🔍❌ Extract operation failed: {e}")
            return []

# --- Type Conversion Utilities ---

def cty_to_python(value: Any) -> Any:
    """
    Convert CTY types to Python native types that glom can work with.
    """
    logger.debug(f"🧰🔍🔄 Converting CTY to Python: {type(value).__name__}")
    
    # Handle None
    if value is None:
        return None
        
    # Handle already native types
    if isinstance(value, (str, int, float, bool, list, dict, set, tuple)):
        return value
        
    # Handle CTY types
    if hasattr(value, "__class__"):
        class_name = value.__class__.__name__
        
        # Extract value from primitive types
        if class_name in ("CtyString", "CtyNumber", "CtyBool"):
            if hasattr(value, "value"):
                return value.value
                
        # Handle CtyList -> list
        elif class_name == "CtyList" and hasattr(value, "elements"):
            return [cty_to_python(elem) for elem in value.elements]
            
        # Handle CtyMap -> dict
        elif class_name == "CtyMap" and hasattr(value, "elements"):
            return {str(k): cty_to_python(v) for k, v in value.elements.items()}
            
        # Handle CtySet -> set
        elif class_name == "CtySet" and hasattr(value, "elements"):
            return {cty_to_python(elem) for elem in value.elements}
            
        # Handle CtyObject -> dict
        elif class_name == "CtyObject" and hasattr(value, "attributes"):
            return {k: cty_to_python(v) for k, v in value.attributes.items()}
            
        # Handle CtyDynamic - try to get underlying value
        elif class_name == "CtyDynamic" and hasattr(value, "value"):
            inner_value = value.value
            # Recursively convert if it's another CTY type
            if inner_value is not None and hasattr(inner_value, "__class__") and inner_value.__class__.__name__.startswith("Cty"):
                return cty_to_python(inner_value)
            return inner_value
    
    # Try direct value extraction as fallback
    if hasattr(value, "value"):
        return value.value
        
    # Last resort - use as is with warning
    logger.warning(f"🧰⚠️⚠️ Unhandled CTY type: {type(value).__name__}, using as-is")
    return value

def python_to_cty(value: Any) -> Any:
    """
    Convert Python native types back to CTY types.
    """
    logger.debug(f"🧰🔍🔄 Converting Python to CTY: {type(value).__name__}")
    
    # Handle None
    if value is None:
        return None
        
    # Handle primitives
    if isinstance(value, str):
        return CtyString(value)
    elif isinstance(value, (int, float)):
        return CtyNumber(value)
    elif isinstance(value, bool):
        return CtyBool(value)
        
    # Handle collections
    elif isinstance(value, list):
        # Empty list needs element type, default to dynamic
        if not value:
            return CtyList(element_type=CtyDynamic())
        
        # Convert all elements
        elements = [python_to_cty(elem) for elem in value]
        
        # Try to determine a common element type
        element_types = set(type(elem) for elem in elements if elem is not None)
        if len(element_types) == 1:
            # All elements have the same type
            element_type = next(iter(element_types))()
        else:
            # Mixed types, use dynamic
            element_type = CtyDynamic()
            
        return CtyList(element_type=element_type, elements=elements)
        
    elif isinstance(value, dict):
        # Convert all values
        elements = {k: python_to_cty(v) for k, v in value.items()}
        
        # Try to determine a common value type
        value_types = set(type(v) for v in elements.values() if v is not None)
        if len(value_types) == 1:
            # All values have the same type
            value_type = next(iter(value_types))()
        else:
            # Mixed types, use dynamic
            value_type = CtyDynamic()
                
        return CtyMap(key_type=CtyString(), value_type=value_type, elements=elements)
        
    elif isinstance(value, set):
        # Similar to list handling
        if not value:
            return CtySet(element_type=CtyDynamic())
            
        # Convert all elements
        elements = {python_to_cty(elem) for elem in value}
        
        # Try to determine a common element type
        element_types = set(type(elem) for elem in elements if elem is not None)
        if len(element_types) == 1:
            # All elements have the same type
            element_type = next(iter(element_types))()
        else:
            # Mixed types, use dynamic
            element_type = CtyDynamic()
            
        return CtySet(element_type=element_type, elements=elements)
    
    # Default to wrapping in CtyDynamic
    logger.warning(f"🧰⚠️⚠️ No direct CTY equivalent for {type(value).__name__}, wrapping as CtyDynamic")
    return CtyDynamic(value)

def parse_json_if_needed(value: Any) -> Any:
    """Parse JSON strings to Python structures when detected."""
    logger.debug(f"🧰🔍🔄 Checking if value is JSON: {type(value).__name__}")
    
    if not isinstance(value, str):
        return value
        
    # Check if it looks like JSON
    value_str = value.strip()
    if (value_str.startswith('{') and value_str.endswith('}')) or \
       (value_str.startswith('[') and value_str.endswith(']')):
        try:
            return json.loads(value_str)
        except json.JSONDecodeError:
            # Not valid JSON after all
            return value
            
    return value

def ensure_glom_compatible(value: Any) -> Any:
    """
    Ensure a value is compatible with glom operations by converting as needed.
    
    This function:
    1. Converts CTY types to Python native types
    2. Parses JSON strings if detected
    3. Handles special case conversions
    """
    logger.debug(f"🧰🔍🔄 Preparing value for glom: {type(value).__name__}")
    
    # First convert from CTY to Python
    py_value = cty_to_python(value)
    
    # Then check for JSON strings
    parsed_value = parse_json_if_needed(py_value)
    
    # Special case for int indices in a string path
    if isinstance(parsed_value, str) and '.' in parsed_value:
        path_parts = parsed_value.split('.')
        # Convert numeric path parts to integers for list indexing
        converted_parts = []
        for part in path_parts:
            if part.isdigit():
                converted_parts.append(int(part))
            else:
                converted_parts.append(part)
        
        # If we made any conversions, return a Path object instead
        if any(isinstance(part, int) for part in converted_parts):
            logger.debug(f"🧰🔍🔄 Converting string path with numeric indices to Path: {parsed_value}")
            return Path(*converted_parts)
    
    return parsed_value

def format_glom_error(error: Exception) -> str:
    """Format a glom error for better readability and context."""
    if isinstance(error, PathAccessError):
        # Format: "Could not access 'x.y.z' (failed at [2])"
        path_str = '.'.join(str(p) for p in error.path)
        part_idx = error.part_idx if hasattr(error, 'part_idx') else '?'
        failing_part = error.path[error.part_idx] if hasattr(error, 'part_idx') and error.part_idx < len(error.path) else '?'
        return f"Could not access '{path_str}' (failed at position {part_idx}: '{failing_part}')"
        
    elif isinstance(error, PathAssignError):
        # Format: "Could not assign to 'x.y.z' (failed at [2])"
        path_str = '.'.join(str(p) for p in error.path)
        part_idx = error.part_idx if hasattr(error, 'part_idx') else '?'
        failing_part = error.path[error.part_idx] if hasattr(error, 'part_idx') and error.part_idx < len(error.path) else '?'
        return f"Could not assign to '{path_str}' (failed at position {part_idx}: '{failing_part}')"
        
    elif isinstance(error, GlomError):
        return f"Glom operation failed: {error}"
        
    # Generic error handling with more context
    return f"Error during glom operation: {error} ({type(error).__name__})"

def create_glom_spec(path_or_spec: Any) -> Any:
    """Create a proper glom Spec object from the given path or spec."""
    logger.debug(f"🧰🔍🔄 Creating glom spec from: {type(path_or_spec).__name__}")
    
    # If it's already a Path or Spec object, return as is
    if isinstance(path_or_spec, (Path, Spec, T.__class__)):
        return path_or_spec
        
    # If it's a string, convert to Path
    if isinstance(path_or_spec, str):
        # Empty path means the target itself
        if not path_or_spec:
            return T
            
        # Handle dot notation
        if '.' in path_or_spec:
            path_parts = []
            for part in path_or_spec.split('.'):
                # Convert numeric parts to integers for list access
                if part.isdigit():
                    path_parts.append(int(part))
                else:
                    path_parts.append(part)
            return Path(*path_parts)
            
        # Single segment path
        return path_or_spec
    
    # For other types, return as is
    return path_or_spec

def extract_value(target: Any, path_or_spec: Any, default: Any = None) -> Any:
    """
    Extract a value from a nested structure.
    
    Args:
        target: The data structure to extract from
        path_or_spec: Path string with dot notation or glom spec
        default: Value to return if path doesn't exist
        
    Returns:
        The extracted value or default
    """
    logger.debug(f"🧰📝🔄 Extracting value with path: {path_or_spec}")
    
    try:
        # Handle different path formats
        if isinstance(path_or_spec, str):
            # Convert string path to Path object with proper int indices
            parts = []
            for part in path_or_spec.split('.'):
                if part.isdigit():
                    parts.append(int(part))
                else:
                    parts.append(part)
            spec = Path(*parts)
        else:
            spec = path_or_spec
            
        # Handle different target types
        if not isinstance(target, (dict, list)) and target is not None:
            if hasattr(target, "value"):  # Handle CTY types
                target = cty_to_python(target)
            else:
                raise FunctionError(f"Invalid data structure: {type(target).__name__}")
        
        # Use glom to extract the value
        if default is not None:
            result = glom_extract(target, spec, default=default)
        else:
            result = glom_extract(target, spec)
            
        logger.debug(f"🧰📝✅ Extracted value of type: {type(result).__name__}")
        return result
        
    except Exception as e:
        if default is not None:
            logger.debug(f"🧰📝⚠️ Extraction failed, returning default: {e}")
            return default
        logger.error(f"🧰📝❌ Extraction failed: {e}")
        raise FunctionError(f"Failed to extract value: {e}")

def transform_data(target: Any, spec: Any) -> dict:
    """
    Transform data using a glom specification.
    
    Args:
        target: The data structure to transform
        spec: Transformation specification (dict mapping output keys to input paths)
        
    Returns:
        Transformed data structure
    """
    logger.debug(f"🧰📝🔄 Transforming data with spec: {spec}")
    
    try:
        # Ensure target is a compatible type
        if not isinstance(target, (dict, list)) and target is not None:
            if hasattr(target, "value"):  # Handle CTY types
                target = cty_to_python(target)
            else:
                raise FunctionError(f"Invalid data structure: {type(target).__name__}")
                
        # Ensure spec is a valid transformation spec
        if not isinstance(spec, (dict, tuple, list)):
            raise FunctionError(f"Invalid transformation spec: {type(spec).__name__}")
            
        # Apply the transformation
        result = glom_extract(target, spec)
        logger.debug(f"🧰📝✅ Transformation complete: {type(result).__name__}")
        return result
        
    except Exception as e:
        logger.error(f"🧰📝❌ Transformation failed: {e}")
        raise FunctionError(f"Failed to apply transformation: {e}")

def flatten_structure(target: Any, separator: str = ".") -> dict:
    """
    Flatten a nested structure into a single-level dictionary with path keys.
    
    Args:
        target: The nested structure to flatten
        separator: String to use between path segments (default: ".")
        
    Returns:
        Flattened dictionary with path keys
    """
    logger.debug(f"🧰📝🔄 Flattening structure with separator: {separator}")
    
    # Convert target if it's a CTY type
    if hasattr(target, "value"):
        target = cty_to_python(target)
        
    # Ensure we have a compatible structure
    if not isinstance(target, (dict, list)):
        logger.warning(f"🧰📝⚠️ Cannot flatten {type(target).__name__}, returning empty dict")
        return {}
        
    # Recursive function to flatten
    def _flatten(obj, parent_key="", sep=separator):
        items = []
        if isinstance(obj, dict):
            for k, v in obj.items():
                new_key = f"{parent_key}{sep}{k}" if parent_key else str(k)
                if isinstance(v, (dict, list)) and v:  # Skip empty containers
                    items.extend(_flatten(v, new_key, sep).items())
                else:
                    items.append((new_key, v))
        elif isinstance(obj, list):
            for i, v in enumerate(obj):
                new_key = f"{parent_key}{sep}{i}" if parent_key else str(i)
                if isinstance(v, (dict, list)) and v:  # Skip empty containers
                    items.extend(_flatten(v, new_key, sep).items())
                else:
                    items.append((new_key, v))
        return dict(items)
    
    result = _flatten(target)
    logger.debug(f"🧰📝✅ Flattening complete: {len(result)} keys")
    return result

def path_exists(target: Any, path: str) -> bool:
    """
    Check if a path exists in a structure.
    
    Args:
        target: The data structure to check
        path: Path string with dot notation
        
    Returns:
        True if path exists, False otherwise
    """
    logger.debug(f"🧰📝🔄 Checking if path exists: {path}")
    
    try:
        # Use a sentinel value to distinguish between None and not found
        sentinel = object()
        result = extract_value(target, path, default=sentinel)
        exists = result is not sentinel
        logger.debug(f"🧰📝✅ Path {'exists' if exists else 'does not exist'}")
        return exists
    except Exception:
        logger.debug(f"🧰📝⚠️ Error checking path, assuming it doesn't exist")
        return False

def _resolve_path(path: Any) -> Union[list, Path]:
    """
    Resolve a path to a list of path segments or Path object.
    
    Args:
        path: String path, list of segments, or Path object
        
    Returns:
        Resolved path as list or Path object
    """
    logger.debug(f"🧰🔄🔍 Resolving path: {path}")
    
    if isinstance(path, str):
        # Convert numeric parts to integers
        parts = []
        for part in path.split('.'):
            if part.isdigit():
                parts.append(int(part))
            else:
                parts.append(part)
        return parts
        
    elif isinstance(path, (list, tuple)):
        return list(path)
        
    # Return as is for Path objects
    return path

def validate_structure(target: Any, schema: Any, exact_match: bool = False, 
                      return_errors: bool = False) -> Union[bool, tuple[bool, list[str]]]:
    """
    Validate a structure against a schema.
    
    Args:
        target: The structure to validate
        schema: Schema defining expected types
        exact_match: Whether to require exact key matching
        return_errors: Whether to return list of errors
        
    Returns:
        Bool or (bool, list) if return_errors is True
    """
    logger.debug(f"🧰📝🔄 Validating structure (exact_match={exact_match})")
    
    errors = []
    
    def _validate_dict(data, schema_dict, path=""):
        valid = True
        
        # Convert data if it's a CTY type
        if hasattr(data, "value"):
            data = cty_to_python(data)
            
        # Check required fields in schema
        for key, expected_type in schema_dict.items():
            key_path = f"{path}.{key}" if path else key
            
            if isinstance(expected_type, dict):
                # Nested schema
                if key not in data:
                    errors.append(f"Missing required field: {key_path}")
                    valid = False
                    continue
                    
                if not isinstance(data[key], dict):
                    errors.append(f"{key_path} should be a dict, got {type(data[key]).__name__}")
                    valid = False
                    continue
                    
                # Recursive validation
                valid = _validate_dict(data[key], expected_type, key_path) and valid
                
            else:
                # Type checking
                if key not in data:
                    errors.append(f"Missing required field: {key_path}")
                    valid = False
                    continue
                
                if not isinstance(data[key], expected_type):
                    errors.append(f"{key_path} should be {expected_type.__name__}, got {type(data[key]).__name__}")
                    valid = False
        
        # Check for unexpected fields if exact match required
        if exact_match:
            for key in data:
                key_path = f"{path}.{key}" if path else key
                if key not in schema_dict:
                    errors.append(f"Unexpected field: {key_path}")
                    valid = False
        
        return valid
    
    if not isinstance(target, dict) or not isinstance(schema, dict):
        errors.append("Both target and schema must be dictionaries")
        if return_errors:
            return False, errors
        return False
    
    valid = _validate_dict(target, schema)
    
    if return_errors:
        return valid, errors
    return valid

def merge_structures(*structures, overwrite: bool = True, append_lists: bool = False) -> dict:
    """
    Merge multiple structures.
    
    Args:
        *structures: Structures to merge
        overwrite: Whether to overwrite existing keys
        append_lists: Whether to append lists instead of replacing
        
    Returns:
        Merged structure
    """
    logger.debug(f"🧰📝🔄 Merging {len(structures)} structures")
    
    if not structures:
        raise FunctionError("At least one structure is required for merging")
    
    # Ensure all structures are dictionaries
    for i, struct in enumerate(structures):
        if not isinstance(struct, dict):
            raise FunctionError(f"All structures must be dictionaries, got {type(struct).__name__} for structure {i}")
    
    # Helper for deep merge
    def deep_merge(a, b):
        result = a.copy()
        for k, v in b.items():
            if k in result and isinstance(result[k], dict) and isinstance(v, dict):
                # Recursive merge for nested dicts
                result[k] = deep_merge(result[k], v)
            elif k in result and isinstance(result[k], list) and isinstance(v, list) and append_lists:
                # Append lists instead of replacing
                result[k] = result[k] + v
            elif k not in result or overwrite:
                # Add new keys or overwrite existing values
                result[k] = v
        return result
    
    # Apply merges sequentially
    result = structures[0].copy()
    for source in structures[1:]:
        result = deep_merge(result, source)
    
    logger.debug(f"🧰📝✅ Structures merged successfully")
    return result

def filter_structure(target: Any, include_keys: List[str] = None, exclude_keys: List[str] = None,
                    value_types: List[type] = None, predicate: Callable = None) -> Any:
    """
    Filter a structure based on various criteria.
    
    Args:
        target: Structure to filter
        include_keys: Keys to include (others excluded)
        exclude_keys: Keys to exclude
        value_types: Types of values to include
        predicate: Custom filtering function
        
    Returns:
        Filtered structure
    """
    logger.debug(f"🧰📝🔄 Filtering structure")
    
    include_keys = include_keys or []
    exclude_keys = exclude_keys or []
    value_types = value_types or []
    
    # Convert target if it's a CTY type
    if hasattr(target, "value"):
        target = cty_to_python(target)
    
    def filter_node(obj, path=None):
        path = path or []
        
        if isinstance(obj, dict):
            result = {}
            for k, v in obj.items():
                # Skip excluded keys
                if k in exclude_keys:
                    continue
                
                # Check include keys if specified
                if include_keys and k not in include_keys:
                    # Still check nested structures for included keys
                    if isinstance(v, (dict, list)):
                        filtered = filter_node(v, path + [k])
                        if filtered: # Not empty
                            result[k] = filtered
                    continue
                
                # Type checking
                if value_types and not any(isinstance(v, t) for t in value_types):
                    continue
                
                # Check predicate
                if predicate and not predicate(path + [k], v):
                    continue
                
                # Recursive filtering for collections
                if isinstance(v, (dict, list)):
                    filtered = filter_node(v, path + [k])
                    if filtered is not None:  # Could be empty dict/list
                        result[k] = filtered
                else:
                    result[k] = v
                    
            return result
            
        elif isinstance(obj, list):
            result = []
            for i, v in enumerate(obj):
                # Type checking
                if value_types and not any(isinstance(v, t) for t in value_types):
                    continue
                
                # Check predicate
                if predicate and not predicate(path + [i], v):
                    continue
                
                # Recursive filtering
                if isinstance(v, (dict, list)):
                    filtered = filter_node(v, path + [i])
                    if filtered is not None:
                        result.append(filtered)
                else:
                    result.append(v)
                    
            return result
            
        else:
            # Simple value - already passed checks
            return obj
    
    result = filter_node(target)
    logger.debug(f"🧰📝✅ Filtering complete")
    return result

def convert_to_terraform_value(value: Any, cty_type: Any) -> Any:
    """
    Convert a Python value to a Terraform CTY value.
    
    Args:
        value: Python value to convert
        cty_type: Target CTY type
        
    Returns:
        Converted CTY value
    """
    logger.debug(f"🧰📝🔄 Converting to Terraform type: {cty_type.__class__.__name__}")
    
    if value is None:
        return None
        
    # Handle primitive types
    if isinstance(cty_type, CtyString):
        return CtyString(str(value))
    elif isinstance(cty_type, CtyNumber):
        return CtyNumber(float(value))
    elif isinstance(cty_type, CtyBool):
        return CtyBool(bool(value))
        
    # Handle collections
    elif isinstance(cty_type, CtyList):
        element_type = getattr(cty_type, "element_type", None)
        if not isinstance(value, list):
            value = [value]
        elements = [convert_to_terraform_value(v, element_type) for v in value]
        return CtyList(element_type=element_type, elements=elements)
        
    elif isinstance(cty_type, CtyMap):
        if not isinstance(value, dict):
            logger.warning(f"🧰📝⚠️ Expected dict for CtyMap, got {type(value).__name__}")
            return CtyMap(key_type=CtyString(), value_type=getattr(cty_type, "value_type", None))
            
        key_type = getattr(cty_type, "key_type", CtyString())
        value_type = getattr(cty_type, "value_type", None)
        
        elements = {}
        for k, v in value.items():
            key = convert_to_terraform_value(k, key_type)
            val = convert_to_terraform_value(v, value_type)
            elements[key] = val
            
        return CtyMap(key_type=key_type, value_type=value_type, elements=elements)
        
    # Default to using constructor
    try:
        return cty_type.__class__(value)
    except Exception as e:
        logger.error(f"🧰📝❌ Failed to convert to {cty_type.__class__.__name__}: {e}")
        return None
# --- Terraform Functions ---

@register_function(
    name="pyvider_glom",
    summary="Extract data using glom specifications",
    description="""
    Extract and transform data from complex nested structures using glom specifications.
    
    This is a powerful function for accessing nested data, similar to jq but with Python semantics.
    The specification can be a simple path string, a list for iteration, or a dict for restructuring.
    
    Examples:
    ```hcl
    # Extract a simple value
    output "username" {
      value = provider::pyvider_glom(var.user_data, "user.name")
    }
    
    # Extract and transform a list
    output "emails" {
      value = provider::pyvider_glom(var.users, [{"email": "email", "name": "name"}])
    }
    
    # Extract with a default value
    output "settings" {
      value = provider::pyvider_glom(var.config, "settings.theme", "default-theme")
    }
    ```
    
    For more complex specifications, see: https://glom.readthedocs.io/
    """,
    param_descriptions={
        "target": "The data structure to extract data from (object, list, or JSON string)",
        "spec": "Glom specification (path string, list, dict, etc.)",
        "default": "Default value to return if path doesn't exist"
    },
    allow_null=["default"]
)
def glom_extract(target: CtyDynamic, spec: CtyDynamic, default: CtyDynamic = None) -> CtyDynamic:
    """Extract data from a complex nested structure using glom specifications."""
    logger.debug(f"🧰📝🔄 Glom extract called with spec: {spec}")
    
    try:
        # Convert inputs to Python types glom can work with
        py_target = ensure_glom_compatible(target)
        py_spec = ensure_glom_compatible(spec)
        py_default = ensure_glom_compatible(default) if default is not None else None
        
        logger.debug(f"🧰🔍🔄 Converted target type: {type(py_target).__name__}")
        logger.debug(f"🧰🔍🔄 Converted spec type: {type(py_spec).__name__}")
        
        # Create a proper glom spec
        glom_spec = create_glom_spec(py_spec)
        logger.debug(f"🧰🔍🔄 Created glom spec: {glom_spec}")
        
        # Apply glom with the appropriate args
        if py_default is not None:
            result = glom_extract(py_target, glom_spec, default=py_default)
            logger.debug(f"🧰📝✅ Glom extract with default successful: {type(result).__name__}")
        else:
            result = glom_extract(py_target, glom_spec)
            logger.debug(f"🧰📝✅ Glom extract successful: {type(result).__name__}")
        
        # Convert result back to CTY
        cty_result = python_to_cty(result)
        logger.debug(f"🧰🔍✅ Converted result to CTY: {type(cty_result).__name__}")
        
        return cty_result
        
    except GlomError as e:
        error_msg = format_glom_error(e)
        ### logger.error(f"🧰📝❌ Glom extract failed: {error_msg}")
        raise FunctionError(error_msg)
    except Exception as e:
        error_msg = f"Extraction failed: {e} ({type(e).__name__})"
        ###logger.error(f"🧰📝❌ Glom extract failed: {error_msg}")
        raise FunctionError(error_msg)

@register_function(
    name="pyvider_glom_path",
    summary="Extract data by path",
    description="""
    Extract a value from a nested structure using a path with dot notation.
    This is a simplified version of pyvider_glom focused on path-based extraction.
    
    Example:
    ```hcl
    # Extract nested values with dot notation
    output "user_city" {
      value = provider::pyvider_glom_path(var.user_data, "address.city", "Unknown")
    }
    ```
    """,
    param_descriptions={
        "target": "The data structure to extract data from (object, list, or JSON string)",
        "path": "Path to the value using dot notation (e.g., 'user.address.street')",
        "default": "Default value to return if path doesn't exist"
    },
    allow_null=["default"]
)
def glom_path(target: CtyDynamic, path: CtyString, default: CtyDynamic = None) -> CtyDynamic:
    """Extract a value from a nested structure using a path with dot notation."""
    logger.debug(f"🧰📝🔄 Glom path called with path: {path}")
    
    try:
        # Convert inputs to Python types glom can work with
        py_target = ensure_glom_compatible(target)
        py_path = cty_to_python(path)
        py_default = ensure_glom_compatible(default) if default is not None else None
        
        logger.debug(f"🧰🔍🔄 Converted target type: {type(py_target).__name__}")
        
        # Create Path object for the dot notation
        if isinstance(py_path, str):
            glom_path = Path(*py_path.split('.'))
            logger.debug(f"🧰🔍🔄 Converted string path to Path: {glom_path}")
        else:
            glom_path = py_path
        
        # Apply glom with the appropriate args
        if py_default is not None:
            result = glom_extract(py_target, glom_path, default=py_default)
            logger.debug(f"🧰📝✅ Glom path with default successful: {type(result).__name__}")
        else:
            result = glom_extract(py_target, glom_path)
            logger.debug(f"🧰📝✅ Glom path successful: {type(result).__name__}")
        
        # Convert result back to CTY
        cty_result = python_to_cty(result)
        logger.debug(f"🧰🔍✅ Converted result to CTY: {type(cty_result).__name__}")
        
        return cty_result
        
    except Exception as e:
        error_msg = format_glom_error(e)
        logger.error(f"🧰📝❌ Glom path failed: {error_msg}")
        raise FunctionError(error_msg)

@register_function(
    name="pyvider_glom_assign",
    summary="Assign values in nested structures",
    description="""
    Update a nested data structure by assigning a value at a specified path.
    Returns a new copy of the structure with the update (does not modify the original).
    
    Example:
    ```hcl
    # Update nested user data
    output "updated_user" {
      value = provider::pyvider_glom_assign(var.user, "preferences.theme", "dark")
    }
    
    # Update in a list
    output "updated_users" {
      value = provider::pyvider_glom_assign(var.users, "1.active", true)
    }
    ```
    """,
    param_descriptions={
        "target": "The data structure to update (object, list, or JSON string)",
        "path": "Path to assign to using dot notation (e.g., 'user.preferences.theme')",
        "value": "Value to assign at the path"
    }
)
def glom_assign(target: CtyDynamic, path: CtyString, value: CtyDynamic) -> CtyDynamic:
    """Update a nested data structure by assigning a value at a specified path."""
    logger.debug(f"🧰📝🔄 Glom assign called with path: {path}")
    
    try:
        # Convert inputs to Python types glom can work with
        py_target = ensure_glom_compatible(target)
        py_path = cty_to_python(path)
        py_value = ensure_glom_compatible(value)
        
        logger.debug(f"🧰🔍🔄 Converted target type: {type(py_target).__name__}")
        logger.debug(f"🧰🔍🔄 Converted value type: {type(py_value).__name__}")
        
        # Make a deep copy to avoid modifying the original
        import copy
        target_copy = copy.deepcopy(py_target)
        
        # Create Path object and Assign spec
        path_parts = py_path.split('.') if isinstance(py_path, str) else py_path
        assign_spec = Assign(Path(*path_parts), py_value)
        logger.debug(f"🧰🔍🔄 Created assign spec: {assign_spec}")
        
        # Apply the assignment
        result = glom_extract(target_copy, assign_spec)
        logger.debug(f"🧰📝✅ Glom assign successful: {type(result).__name__}")
        
        # Convert result back to CTY
        cty_result = python_to_cty(result)
        logger.debug(f"🧰🔍✅ Converted result to CTY: {type(cty_result).__name__}")
        
        return cty_result
        
    except Exception as e:
        error_msg = format_glom_error(e)
        logger.error(f"🧰📝❌ Glom assign failed: {error_msg}")
        raise FunctionError(error_msg)

@register_function(
    name="pyvider_glom_flatten",
    summary="Flatten nested structures",
    description="""
    Flatten a nested data structure into a single-level dictionary with dot-separated keys.
    This is useful for transforming complex nested objects into simpler key-value pairs.
    
    Example:
    ```hcl
    # Flatten user data
    output "flat_user" {
      value = provider::pyvider_glom_flatten(var.user)
      # Converts {user: {name: "John", address: {city: "NY"}}} 
      # to {"user.name": "John", "user.address.city": "NY"}
    }
    
    # Flatten with custom separator
    output "flat_config" {
      value = provider::pyvider_glom_flatten(var.config, "_")
      # Uses underscore instead of dot for keys
    }
    ```
    """,
    param_descriptions={
        "target": "The nested data structure to flatten (object, list, or JSON string)",
        "separator": "Character(s) to use between nested keys (default: '.')"
    },
    allow_null=["separator"]
)
def glom_flatten(target: CtyDynamic, separator: CtyString = None) -> CtyDynamic:
    """Flatten a nested data structure into a single-level dictionary with path keys."""
    logger.debug(f"🧰📝🔄 Glom flatten called")
    
    try:
        # Convert inputs to Python types
        py_target = ensure_glom_compatible(target)
        py_separator = cty_to_python(separator) if separator is not None else "."
        
        logger.debug(f"🧰🔍🔄 Converted target type: {type(py_target).__name__}")
        logger.debug(f"🧰🔍🔄 Using separator: {py_separator}")
        
        # Recursive function to flatten the structure
        def flatten_dict(d, parent_key="", sep=py_separator):
            items = []
            for k, v in d.items() if isinstance(d, dict) else enumerate(d) if isinstance(d, list) else []:
                new_key = f"{parent_key}{sep}{k}" if parent_key else str(k)
                
                if isinstance(v, (dict, list)) and v:  # Skip empty containers
                    items.extend(flatten_dict(v, new_key, sep=sep).items())
                else:
                    items.append((new_key, v))
            return dict(items)
        
        # Apply flattening
        if not isinstance(py_target, (dict, list)):
            logger.warning(f"🧰⚠️⚠️ Cannot flatten non-container type: {type(py_target).__name__}")
            return python_to_cty({})
            
        result = flatten_dict(py_target)
        logger.debug(f"🧰📝✅ Flattening successful: {len(result)} keys")
        
        # Convert result back to CTY
        cty_result = python_to_cty(result)
        logger.debug(f"🧰🔍✅ Converted result to CTY: {type(cty_result).__name__}")
        
        return cty_result
        
    except Exception as e:
        error_msg = f"Failed to flatten structure: {e}"
        logger.error(f"🧰📝❌ Glom flatten failed: {error_msg}")
        raise FunctionError(error_msg)

@register_function(
    name="pyvider_glom_transform",
    summary="Transform extracted data",
    description="""
    Extract and transform data using a series of operations.
    This function allows you to chain multiple transformations on extracted data.
    
    Available transformations:
    - "lower": Convert string to lowercase
    - "upper": Convert string to uppercase
    - "title": Convert string to title case
    - "length": Get length of string or list
    - "keys": Get keys from a map/object
    - "values": Get values from a map/object
    - "sort": Sort a list
    - "reverse": Reverse a list or string
    - "sum": Sum numbers in a list
    - "join:<sep>": Join a list of strings with separator
    - "split:<sep>": Split a string by separator
    
    Example:
    ```hcl
    # Get uppercase username
    output "username" {
      value = provider::pyvider_glom_transform(var.user, "name", "upper")
    }
    
    # Sort a list
    output "sorted_items" {
      value = provider::pyvider_glom_transform(var.items, "", "sort")
    }
    
    # Extract, join and uppercase
    output "tags" {
      value = provider::pyvider_glom_transform(var.resources, "tags", "join:,|upper")
    }
    ```
    """,
    param_descriptions={
        "target": "The data structure to extract data from",
        "path": "Path to extract (empty string for the whole target)",
        "transforms": "Transformation to apply, or pipe-separated list (e.g., 'lower|length')",
        "default": "Default value if path not found"
    },
    allow_null=["default"]
)
def glom_transform(target: CtyDynamic, path: CtyString, transforms: CtyString, default: CtyDynamic = None) -> CtyDynamic:
    """
    Extract and transform data using a series of operations.
    
    This function extracts data from a path and applies a sequence of
    transformations specified as a pipe-separated string. Each transformation
    is applied in order, with the output of one feeding into the next.
    """
    logger.debug(f"🧰📝🔄 Glom transform called with path: {path}, transforms: {transforms}")
    
    try:
        # Convert inputs to Python types
        py_target = ensure_glom_compatible(target)
        py_path = cty_to_python(path)
        py_transforms = cty_to_python(transforms)
        py_default = ensure_glom_compatible(default) if default is not None else None
        
        logger.debug(f"🧰🔍🔄 Converted target type: {type(py_target).__name__}")
        
        # Extract initial value
        if py_path:
            glom_path = Path(*py_path.split('.'))
            if py_default is not None:
                value = glom_extract(py_target, glom_path, default=py_default)
            else:
                value = glom_extract(py_target, glom_path)
        else:
            value = py_target
            
        logger.debug(f"🧰🔍🔄 Extracted initial value: {type(value).__name__}")
        
        # Split transforms by pipe
        transform_list = py_transforms.split('|')
        logger.debug(f"🧰🔍🔄 Transform list: {transform_list}")
        
        # Apply each transformation
        for t in transform_list:
            t = t.strip()
            
            if t == "lower" and isinstance(value, str):
                value = value.lower()
            elif t == "upper" and isinstance(value, str):
                value = value.upper()
            elif t == "title" and isinstance(value, str):
                value = value.title()
            elif t == "length":
                value = len(value) if hasattr(value, "__len__") else 0
            elif t == "keys" and isinstance(value, dict):
                value = list(value.keys())
            elif t == "values" and isinstance(value, dict):
                value = list(value.values())
            elif t == "sort" and isinstance(value, list):
                try:
                    value = sorted(value)
                except TypeError:
                    logger.error(f"🧰📝❌ Cannot sort heterogeneous list")
                    raise FunctionError("Cannot sort list with mixed types")
            elif t == "reverse":
                if isinstance(value, list):
                    value = list(reversed(value))
                elif isinstance(value, str):
                    value = value[::-1]
                else:
                    logger.error(f"🧰📝❌ Cannot reverse {type(value).__name__}")
                    raise FunctionError(f"Cannot reverse {type(value).__name__}")
            elif t == "sum" and isinstance(value, list):
                try:
                    value = sum(value)
                except TypeError:
                    logger.error(f"🧰📝❌ Cannot sum non-numeric list")
                    raise FunctionError("Cannot sum list with non-numeric elements")
            elif t.startswith("join:") and isinstance(value, list):
                separator = t[5:]
                try:
                    value = separator.join(str(v) for v in value)
                except Exception as e:
                    logger.error(f"🧰📝❌ Join failed: {e}")
                    raise FunctionError(f"Join failed: {e}")
            elif t.startswith("split:") and isinstance(value, str):
                separator = t[6:] 
                value = value.split(separator)
            else:
                logger.error(f"🧰📝❌ Unknown or incompatible transform: {t}")
                raise FunctionError(f"Unknown or incompatible transform: {t} for {type(value).__name__}")
                
            logger.debug(f"🧰🔍🔄 After '{t}': {type(value).__name__}")
        
        # Convert result back to CTY
        cty_result = python_to_cty(value)
        logger.debug(f"🧰🔍✅ Converted result to CTY: {type(cty_result).__name__}")
        
        return cty_result
        
    except GlomError as e:
        error_msg = format_glom_error(e)
        logger.error(f"🧰📝❌ Glom transform failed: {error_msg}")
        raise FunctionError(error_msg)
    except Exception as e:
        error_msg = f"Transform failed: {e}"
        logger.error(f"🧰📝❌ Glom transform failed: {error_msg}")
        raise FunctionError(error_msg)

@register_function(
    name="pyvider_glom_merge",
    summary="Merge multiple structures",
    description="""
    Merge multiple nested structures into a single structure.
    Later values override earlier values when keys conflict.
    
    For dictionaries/maps, this performs a deep merge, preserving nested structures.
    For non-dictionary values, later values completely replace earlier values.
    
    Example:
    ```hcl
    # Merge default settings with user settings
    output "settings" {
      value = provider::pyvider_glom_merge(var.default_settings, var.user_settings)
    }
    
    # Merge multiple configurations
    output "config" {
      value = provider::pyvider_glom_merge(
        var.base_config,
        var.environment_config,
        var.instance_config
      )
    }
    ```
    """,
    param_descriptions={
        "target": "First data structure to merge",
        "sources": "Additional data structures to merge into the target"
    }
)
def glom_merge(target: CtyDynamic, *sources: CtyDynamic) -> CtyDynamic:
    """
    Merge multiple nested structures into a single structure.
    
    This function performs a deep merge of dictionaries and maps. When keys
    conflict, later sources take precedence over earlier ones. For non-dict
    values, later sources completely replace earlier values.
    """
    logger.debug(f"🧰📝🔄 Glom merge called with {len(sources)} sources")
    
    try:
        # Convert inputs to Python types
        py_target = ensure_glom_compatible(target)
        py_sources = [ensure_glom_compatible(s) for s in sources]
        
        logger.debug(f"🧰🔍🔄 Converted target type: {type(py_target).__name__}")
        
        # Helper function for deep merge
        def deep_merge(a, b):
            """Deep merge dictionaries - b takes precedence"""
            if isinstance(a, dict) and isinstance(b, dict):
                result = a.copy()
                for k, v in b.items():
                    if k in result and isinstance(result[k], dict) and isinstance(v, dict):
                        result[k] = deep_merge(result[k], v)
                    else:
                        result[k] = v
                return result
            return b  # If not both dicts, b wins
        
        # Start with the target
        if not isinstance(py_target, dict):
            if not py_sources:
                return python_to_cty(py_target)
                
            # If target isn't a dict but we have sources, use first source as base
            result = py_sources[0]
            remaining_sources = py_sources[1:]
        else:
            result = py_target
            remaining_sources = py_sources
            
        # Apply merges sequentially
        for source in remaining_sources:
            if isinstance(result, dict) and isinstance(source, dict):
                result = deep_merge(result, source)
            elif isinstance(source, dict):
                # If result is not a dict but source is, convert result to dict
                result = deep_merge({}, source)
            else:
                # If neither is a dict, source replaces result
                result = source
        
        logger.debug(f"🧰📝✅ Merge successful: {type(result).__name__}")
        
        # Convert result back to CTY
        cty_result = python_to_cty(result)
        logger.debug(f"🧰🔍✅ Converted result to CTY: {type(cty_result).__name__}")
        
        return cty_result
        
    except Exception as e:
        error_msg = f"Merge failed: {e}"
        logger.error(f"🧰📝❌ Glom merge failed: {error_msg}")
        raise FunctionError(error_msg)

@register_function(
    name="pyvider_glom_unflatten",
    summary="Unflatten dot-notation keys to nested structure",
    description="""
    Convert a flat dictionary with dot-separated keys into a nested structure.
    This is the inverse operation of pyvider_glom_flatten.
    
    Example:
    ```hcl
    # Unflatten a flat structure
    output "nested_data" {
      value = provider::pyvider_glom_unflatten({
        "user.name": "John",
        "user.address.city": "New York",
        "user.address.zip": "10001"
      })
      # Results in nested structure: {user: {name: "John", address: {city: "New York", zip: "10001"}}}
    }
    
    # Unflatten with custom separator
    output "nested_config" {
      value = provider::pyvider_glom_unflatten(var.flat_config, "_")
      # Uses underscore instead of dot for key separation
    }
    ```
    """,
    param_descriptions={
        "flat_dict": "The flat dictionary with path-based keys",
        "separator": "Character(s) used between nested keys (default: '.')"
    },
    allow_null=["separator"]
)
def glom_unflatten(flat_dict: CtyDynamic, separator: CtyString = None) -> CtyDynamic:
    """Convert a flat dictionary with dot-notation keys into a nested structure."""
    logger.debug(f"🧰📝🔄 Glom unflatten called")
    
    try:
        # Convert inputs to Python types
        py_flat_dict = ensure_glom_compatible(flat_dict)
        py_separator = cty_to_python(separator) if separator is not None else "."
        
        logger.debug(f"🧰🔍🔄 Converted flat_dict type: {type(py_flat_dict).__name__}")
        logger.debug(f"🧰🔍🔄 Using separator: {py_separator}")
        
        # Ensure we have a dictionary
        if not isinstance(py_flat_dict, dict):
            logger.error(f"🧰📝❌ Input must be a dictionary/map")
            raise FunctionError("Input must be a dictionary/map")
        
        # Helper function to set nested value
        def set_nested(d, key_parts, value):
            """Set a value in a nested dict by following key_parts."""
            # Last key part gets the value directly
            if len(key_parts) == 1:
                d[key_parts[0]] = value
                return
                
            # Create intermediate dicts as needed
            current_key = key_parts[0]
            # If key is a number string and we need a list
            if current_key.isdigit() and isinstance(d, list):
                idx = int(current_key)
                # Expand list if needed
                while len(d) <= idx:
                    d.append({} if len(key_parts) > 1 else None)
                # Ensure we have a dict or list at this position
                if len(key_parts) > 1:
                    if not isinstance(d[idx], (dict, list)):
                        d[idx] = {} if not key_parts[1].isdigit() else []
                    
                # Recurse
                set_nested(d[idx], key_parts[1:], value)
            else:
                # Create dict/list as appropriate
                if current_key not in d:
                    # If next part is a number, create list, else dict
                    d[current_key] = [] if len(key_parts) > 1 and key_parts[1].isdigit() else {}
                elif not isinstance(d[current_key], (dict, list)):
                    # Convert to dict/list if needed
                    d[current_key] = {} if not (len(key_parts) > 1 and key_parts[1].isdigit()) else []
                
                # Recurse
                set_nested(d[current_key], key_parts[1:], value)
        
        # Start with empty result
        result = {}
        
        # Process each flattened key
        for flat_key, value in py_flat_dict.items():
            # Skip empty keys
            if not flat_key:
                continue
                
            # Split key into parts
            key_parts = flat_key.split(py_separator)
            
            # Set the value at this path
            set_nested(result, key_parts, value)
            
        logger.debug(f"🧰📝✅ Unflatten successful")
        
        # Convert result back to CTY
        cty_result = python_to_cty(result)
        logger.debug(f"🧰🔍✅ Converted result to CTY: {type(cty_result).__name__}")
        
        return cty_result
        
    except Exception as e:
        error_msg = f"Failed to unflatten structure: {e}"
        logger.error(f"🧰📝❌ Glom unflatten failed: {error_msg}")
        raise FunctionError(error_msg)

@register_function(
    name="pyvider_glom_filter",
    summary="Filter elements in a collection",
    description="""
    Filter elements in a list or map/object based on a condition path.
    Returns only elements where the condition evaluates to true.
    
    Example:
    ```hcl
    # Filter active users
    output "active_users" {
      value = provider::pyvider_glom_filter(var.users, "active")
    }
    
    # Filter items with quantity > 0
    output "in_stock" {
      value = provider::pyvider_glom_filter(var.inventory, "quantity", true_condition="T > 0")
    }
    
    # Filter by complex condition
    output "valid_records" {
      value = provider::pyvider_glom_filter(
        var.records, 
        "validation_date", 
        true_condition="T != null", 
        key_path="id"
      )
    }
    ```
    """,
    param_descriptions={
        "target": "The collection to filter (list or map/object)",
        "condition_path": "Path to value used for condition checking",
        "true_condition": "Expression that evaluates to true (default: 'T' for truthy value)",
        "key_path": "For objects, path to extract keys in the result (default: original keys)"
    },
    allow_null=["true_condition", "key_path"]
)
def glom_filter(
    target: CtyDynamic, 
    condition_path: CtyString, 
    true_condition: CtyString = None, 
    key_path: CtyString = None
) -> CtyDynamic:
    """Filter elements in a collection based on a condition."""
    logger.debug(f"🧰📝🔄 Glom filter called with condition_path: {condition_path}")
    
    try:
        # Convert inputs to Python types
        py_target = ensure_glom_compatible(target)
        py_condition_path = cty_to_python(condition_path)
        py_true_condition = cty_to_python(true_condition) if true_condition is not None else "T"
        py_key_path = cty_to_python(key_path) if key_path is not None else None
        
        logger.debug(f"🧰🔍🔄 Converted target type: {type(py_target).__name__}")
        logger.debug(f"🧰🔍🔄 Condition: {py_true_condition}")
        
        # Ensure target is a collection
        if not isinstance(py_target, (list, dict)):
            logger.error(f"🧰📝❌ Cannot filter non-collection type: {type(py_target).__name__}")
            raise FunctionError(f"Cannot filter non-collection type: {type(py_target).__name__}")
        
        # Create a condition checker based on the true_condition string
        def check_condition(value):
            """Check if a value satisfies the condition."""
            # Replace 'T' with the actual value in the condition string
            # This is a simple approach - for complex cases we'd need a proper parser
            try:
                # Set up the evaluation context with the value as T
                T = value  # Local variable for the condition
                # Direct evaluation for common cases
                if py_true_condition == "T":
                    # Truthy check - empty collections are falsy
                    if isinstance(T, (list, dict)):
                        return bool(T)
                    # None is falsy
                    if T is None:
                        return False
                    # Bool is directly usable
                    if isinstance(T, bool):
                        return T
                    # Numbers - 0 is falsy
                    if isinstance(T, (int, float)):
                        return T != 0
                    # Strings - empty is falsy
                    if isinstance(T, str):
                        return bool(T)
                    # Default to truthy
                    return True
                elif py_true_condition == "T != null":
                    return T is not None
                elif py_true_condition == "T == null":
                    return T is None
                elif py_true_condition == "T > 0":
                    return isinstance(T, (int, float)) and T > 0
                elif py_true_condition == "T >= 0":
                    return isinstance(T, (int, float)) and T >= 0
                elif py_true_condition == "T < 0":
                    return isinstance(T, (int, float)) and T < 0
                elif py_true_condition == "T <= 0":
                    return isinstance(T, (int, float)) and T <= 0
                # For more complex conditions, use eval - ONLY with trusted input
                # Normally we'd use a proper parser for expressions
                else:
                    return eval(py_true_condition)
            except Exception as e:
                logger.error(f"🧰📝❌ Error evaluating condition: {e}")
                return False
        
        # Path object for condition checking
        condition_path_obj = Path(*py_condition_path.split('.')) if py_condition_path else None
        
        # Path object for key extraction (if specified)
        key_path_obj = Path(*py_key_path.split('.')) if py_key_path else None
            
        # Filter based on collection type
        if isinstance(py_target, list):
            # Filter list elements
            result = []
            for item in py_target:
                try:
                    # Extract the condition value
                    if condition_path_obj:
                        condition_value = glom_extract(item, condition_path_obj)
                    else:
                        condition_value = item
                        
                    # Check condition
                    if check_condition(condition_value):
                        result.append(item)
                except Exception as e:
                    # Skip items that cause errors
                    logger.debug(f"🧰📝⚠️ Skipping item due to error: {e}")
                    continue
                    
            logger.debug(f"🧰📝✅ Filtered list from {len(py_target)} to {len(result)} items")
            
        else:  # Dictionary/map
            # Filter dictionary items
            result = {}
            for key, item in py_target.items():
                try:
                    # Extract the condition value
                    if condition_path_obj:
                        condition_value = glom_extract(item, condition_path_obj)
                    else:
                        condition_value = item
                        
                    # Check condition
                    if check_condition(condition_value):
                        # Determine the result key
                        if key_path_obj:
                            try:
                                result_key = glom_extract(item, key_path_obj)
                                # Ensure key is hashable
                                if not isinstance(result_key, (str, int, float, bool, tuple)):
                                    result_key = str(result_key)
                            except Exception:
                                # Fall back to original key on extraction error
                                result_key = key
                        else:
                            result_key = key
                            
                        result[result_key] = item
                except Exception as e:
                    # Skip items that cause errors
                    logger.debug(f"🧰📝⚠️ Skipping key '{key}' due to error: {e}")
                    continue
                    
            logger.debug(f"🧰📝✅ Filtered dict from {len(py_target)} to {len(result)} items")
        
        # Convert result back to CTY
        cty_result = python_to_cty(result)
        logger.debug(f"🧰🔍✅ Converted result to CTY: {type(cty_result).__name__}")
        
        return cty_result
        
    except Exception as e:
        error_msg = f"Filter failed: {e}"
        logger.error(f"🧰📝❌ Glom filter failed: {error_msg}")
        raise FunctionError(error_msg)

@register_function(
    name="pyvider_glom_coalesce",
    summary="Try multiple paths and return first successful result",
    description="""
    Try extracting values from multiple paths and return the first successful match.
    This is useful for fallback values and handling optional nested structures.
    
    Example:
    ```hcl
    # Try multiple paths for a setting
    output "theme" {
      value = provider::pyvider_glom_coalesce(
        var.config,
        ["user.preferences.theme", "site.theme", "default.theme"],
        "light"  # Default if all paths fail
      )
    }
    ```
    """,
    param_descriptions={
        "target": "The data structure to extract from",
        "paths": "List of paths to try in order",
        "default": "Default value if all paths fail"
    },
    allow_null=["default"]
)
def glom_coalesce(target: CtyDynamic, paths: CtyDynamic, default: CtyDynamic = None) -> CtyDynamic:
    """Try multiple paths and return the first successful result."""
    logger.debug(f"🧰📝🔄 Glom coalesce called")
    
    try:
        # Convert inputs to Python types
        py_target = ensure_glom_compatible(target)
        py_paths = ensure_glom_compatible(paths)
        py_default = ensure_glom_compatible(default) if default is not None else None
        
        logger.debug(f"🧰🔍🔄 Converted target type: {type(py_target).__name__}")
        logger.debug(f"🧰🔍🔄 Paths to try: {py_paths}")
        
        # Ensure paths is a list
        if not isinstance(py_paths, list):
            logger.error(f"🧰📝❌ Paths must be a list, got {type(py_paths).__name__}")
            raise FunctionError(f"Paths must be a list, got {type(py_paths).__name__}")
        
        # Create a proper Coalesce spec
        path_specs = []
        for path in py_paths:
            # Convert path string to Path object
            if isinstance(path, str):
                # Handle dot notation
                if '.' in path:
                    path_parts = []
                    for part in path.split('.'):
                        # Convert numeric indices
                        if part.isdigit():
                            path_parts.append(int(part))
                        else:
                            path_parts.append(part)
                    path_specs.append(Path(*path_parts))
                else:
                    path_specs.append(path)
            else:
                path_specs.append(path)
                
        # Add default to the end of specs if provided
        if py_default is not None:
            path_specs.append(Literal(py_default))
            
        logger.debug(f"🧰🔍🔄 Created {len(path_specs)} path specs")
        
        # Create and apply Coalesce spec
        spec = Coalesce(*path_specs)
        result = glom_extract(py_target, spec)
        
        logger.debug(f"🧰📝✅ Found result of type: {type(result).__name__}")
        
        # Convert result back to CTY
        cty_result = python_to_cty(result)
        logger.debug(f"🧰🔍✅ Converted result to CTY: {type(cty_result).__name__}")
        
        return cty_result
        
    except Exception as e:
        error_msg = f"Coalesce failed: {e}"
        logger.error(f"🧰📝❌ Glom coalesce failed: {error_msg}")
        raise FunctionError(error_msg)

@register_function(
    name="pyvider_glom_pick",
    summary="Create a new object with only specified paths",
    description="""
    Pick specific paths from a source object to create a new object.
    This is useful for selecting specific fields from a larger structure.
    
    Example:
    ```hcl
    # Pick specific user fields
    output "user_summary" {
      value = provider::pyvider_glom_pick(
        var.user,
        {
          "name": "name",
          "email": "contact.email",
          "city": "address.city"
        }
      )
    }
    ```
    """,
    param_descriptions={
        "target": "The source data structure",
        "spec": "Map of output keys to input paths"
    }
)
def glom_pick(target: CtyDynamic, spec: CtyDynamic) -> CtyDynamic:
    """Create a new object with only the specified paths from the source."""
    logger.debug(f"🧰📝🔄 Glom pick called")
    
    try:
        # Convert inputs to Python types
        py_target = ensure_glom_compatible(target)
        py_spec = ensure_glom_compatible(spec)
        
        logger.debug(f"🧰🔍🔄 Converted target type: {type(py_target).__name__}")
        
        # Ensure spec is a dictionary
        if not isinstance(py_spec, dict):
            logger.error(f"🧰📝❌ Spec must be a map/dictionary, got {type(py_spec).__name__}")
            raise FunctionError(f"Spec must be a map/dictionary")
        
        # Extract each specified path
        result = {}
        for output_key, input_path in py_spec.items():
            try:
                # Convert path to appropriate spec
                path_spec = create_glom_spec(input_path)
                
                # Extract value
                value = glom_extract(py_target, path_spec)
                
                # Add to result
                result[output_key] = value
                
            except Exception as e:
                # Log error but continue with other paths
                logger.debug(f"🧰📝⚠️ Error extracting path '{input_path}': {e}")
                # Skip this path
                continue
                
        logger.debug(f"🧰📝✅ Picked {len(result)} fields")
        
        # Convert result back to CTY
        cty_result = python_to_cty(result)
        logger.debug(f"🧰🔍✅ Converted result to CTY: {type(cty_result).__name__}")
        
        return cty_result
        
    except Exception as e:
        error_msg = f"Pick failed: {e}"
        logger.error(f"🧰📝❌ Glom pick failed: {error_msg}")
        raise FunctionError(error_msg)

@register_function(
    name="pyvider_glom_convert",
    summary="Convert between data formats",
    description="""
    Convert data between different formats (JSON, HCL, YAML, etc.).
    This is useful for working with different data representations.
    
    Available conversions:
    - "to_json": Convert to JSON string
    - "from_json": Parse JSON string
    - "to_yaml": Convert to YAML string
    - "from_yaml": Parse YAML string
    - "to_base64": Encode to base64
    - "from_base64": Decode from base64
    
    Example:
    ```hcl
    # Convert object to JSON
    output "user_json" {
      value = provider::pyvider_glom_convert(var.user, "to_json")
    }
    
    # Parse JSON to object
    output "config" {
      value = provider::pyvider_glom_convert(data.http.api_response.body, "from_json")
    }
    
    # Convert to YAML (requires PyYAML)
    output "yaml_config" {
      value = provider::pyvider_glom_convert(var.config, "to_yaml")
    }
    ```
    """,
    param_descriptions={
        "data": "The data to convert",
        "format": "Conversion format (to_json, from_json, etc.)",
        "options": "Optional formatting parameters as a map"
    },
    allow_null=["options"]
)
def glom_convert(data: CtyDynamic, format: CtyString, options: CtyDynamic = None) -> CtyDynamic:
    """Convert data between different formats."""
    logger.debug(f"🧰📝🔄 Glom convert called with format: {format}")
    
    try:
        # Convert inputs to Python types
        py_data = ensure_glom_compatible(data)
        py_format = cty_to_python(format)
        py_options = ensure_glom_compatible(options) if options is not None else {}
        
        logger.debug(f"🧰🔍🔄 Converting data of type: {type(py_data).__name__}")
        logger.debug(f"🧰🔍🔄 Format: {py_format}")
        
        # Process based on format
        if py_format == "to_json":
            # Get formatting options
            indent = py_options.get("indent", 2)
            sort_keys = py_options.get("sort_keys", False)
            
            # Convert to JSON
            result = json.dumps(py_data, indent=indent, sort_keys=sort_keys)
            logger.debug(f"🧰📝✅ Converted to JSON: {len(result)} characters")
            
        elif py_format == "from_json":
            # Parse JSON
            if not isinstance(py_data, str):
                logger.error(f"🧰📝❌ Input must be a string for JSON parsing")
                raise FunctionError("Input must be a string for JSON parsing")
                
            result = json.loads(py_data)
            logger.debug(f"🧰📝✅ Parsed JSON to {type(result).__name__}")
            
        elif py_format == "to_yaml":
            # Check for PyYAML
            try:
                import yaml
                
                # Get formatting options
                default_flow_style = py_options.get("default_flow_style", False)
                indent = py_options.get("indent", 2)
                
                # Convert to YAML
                result = yaml.dump(py_data, default_flow_style=default_flow_style, indent=indent)
                logger.debug(f"🧰📝✅ Converted to YAML: {len(result)} characters")
                
            except ImportError:
                logger.error(f"🧰📝❌ PyYAML is required for YAML conversion")
                raise FunctionError("PyYAML is required for YAML conversion")
                
        elif py_format == "from_yaml":
            # Check for PyYAML
            try:
                import yaml
                
                # Parse YAML
                if not isinstance(py_data, str):
                    logger.error(f"🧰📝❌ Input must be a string for YAML parsing")
                    raise FunctionError("Input must be a string for YAML parsing")
                    
                result = yaml.safe_load(py_data)
                logger.debug(f"🧰📝✅ Parsed YAML to {type(result).__name__}")
                
            except ImportError:
                logger.error(f"🧰📝❌ PyYAML is required for YAML conversion")
                raise FunctionError("PyYAML is required for YAML conversion")
                
        elif py_format == "to_base64":
            # Ensure string input
            if not isinstance(py_data, str):
                py_data = json.dumps(py_data)
                
            # Encode to base64
            import base64
            result = base64.b64encode(py_data.encode('utf-8')).decode('utf-8')
            logger.debug(f"🧰📝✅ Encoded to base64: {len(result)} characters")
            
        elif py_format == "from_base64":
            # Ensure string input
            if not isinstance(py_data, str):
                logger.error(f"🧰📝❌ Input must be a string for base64 decoding")
                raise FunctionError("Input must be a string for base64 decoding")
                
            # Decode from base64
            import base64
            try:
                # First decode the base64
                decoded = base64.b64decode(py_data).decode('utf-8')
                
                # Then try to parse as JSON if it looks like JSON
                if decoded.strip().startswith('{') or decoded.strip().startswith('['):
                    try:
                        result = json.loads(decoded)
                        logger.debug(f"🧰📝✅ Decoded base64 to JSON object")
                    except json.JSONDecodeError:
                        # Not valid JSON, return the string
                        result = decoded
                        logger.debug(f"🧰📝✅ Decoded base64 to string: {len(result)} characters")
                else:
                    # Not JSON-like, return the string
                    result = decoded
                    logger.debug(f"🧰📝✅ Decoded base64 to string: {len(result)} characters")
                    
            except Exception as e:
                logger.error(f"🧰📝❌ Failed to decode base64: {e}")
                raise FunctionError(f"Failed to decode base64: {e}")
                
        else:
            logger.error(f"🧰📝❌ Unsupported format: {py_format}")
            raise FunctionError(f"Unsupported format: {py_format}")
        
        # Convert result back to CTY
        cty_result = python_to_cty(result)
        logger.debug(f"🧰🔍✅ Converted result to CTY: {type(cty_result).__name__}")
        
        return cty_result
        
    except Exception as e:
        error_msg = f"Conversion failed: {e}"
        logger.error(f"🧰📝❌ Glom convert failed: {error_msg}")
        raise FunctionError(error_msg)

@register_function(
    name="pyvider_glom_validate",
    summary="Validate data against specifications",
    description="""
    Validate data against a set of rules and return validation results.
    This function helps ensure data quality and structure.
    
    Example:
    ```hcl
    # Validate user data
    locals {
      validation_rules = {
        "name": {"required": true, "type": "string"},
        "age": {"type": "number", "min": 0, "max": 120},
        "email": {"type": "string", "pattern": "^[^@]+@[^@]+\\.[^@]+$"}
      }
    }
    
    output "validation_result" {
      value = provider::pyvider_glom_validate(var.user, local.validation_rules)
    }
    ```
    
    Each rule can have:
    - required: true/false - whether the field is required
    - type: string/number/boolean/array/object - expected type
    - min/max: for numbers and string/array length
    - pattern: regex pattern for strings
    - values: list of allowed values
    - custom: custom validation expression
    """,
    param_descriptions={
        "data": "The data to validate",
        "rules": "Map of field paths to validation rules",
        "strict": "Whether to return errors for fields not in rules (default: false)"
    },
    allow_null=["strict"]
)
def glom_validate(data: CtyDynamic, rules: CtyDynamic, strict: CtyBool = None) -> CtyDynamic:
    """Validate data against a set of rules and return validation results."""
    logger.debug(f"🧰📝🔄 Glom validate called")
    
    try:
        # Convert inputs to Python types
        py_data = ensure_glom_compatible(data)
        py_rules = ensure_glom_compatible(rules)
        py_strict = cty_to_python(strict) if strict is not None else False
        
        logger.debug(f"🧰🔍🔄 Validating data of type: {type(py_data).__name__}")
        logger.debug(f"🧰🔍🔄 With {len(py_rules)} rules, strict={py_strict}")
        
        # Initialize results
        validation_result = {
            "valid": True,
            "errors": {},
            "warnings": {}
        }
        
        # Helper for type checking
        def check_type(value, expected_type):
            """Check if value matches expected type."""
            if expected_type == "string":
                return isinstance(value, str)
            elif expected_type == "number":
                return isinstance(value, (int, float))
            elif expected_type == "boolean":
                return isinstance(value, bool)
            elif expected_type == "array":
                return isinstance(value, list)
            elif expected_type == "object":
                return isinstance(value, dict)
            elif expected_type == "null":
                return value is None
            elif expected_type == "any":
                return True
            return False
        
        # Helper for regex pattern validation
        def check_pattern(value, pattern):
            """Check if string matches regex pattern."""
            import re
            try:
                return bool(re.match(pattern, value))
            except:
                return False
        
        # Process each rule
        for field_path, rule_set in py_rules.items():
            # Skip invalid rules
            if not isinstance(rule_set, dict):
                logger.warning(f"🧰📝⚠️ Rule for '{field_path}' is not a dictionary, skipping")
                continue
                
            # Extract field value
            try:
                # Use dot notation for path
                path_obj = Path(*field_path.split('.'))
                field_value = glom_extract(py_data, path_obj, default=None)
                field_exists = True
            except Exception:
                field_value = None
                field_exists = False
            
            # Check required fields
            if rule_set.get("required", False) and (not field_exists or field_value is None):
                validation_result["valid"] = False
                validation_result["errors"][field_path] = "Field is required"
                continue
                
            # Skip validation for non-existent optional fields
            if not field_exists:
                continue
                
            # Type validation
            if "type" in rule_set and field_value is not None:
                expected_type = rule_set["type"]
                if not check_type(field_value, expected_type):
                    validation_result["valid"] = False
                    validation_result["errors"][field_path] = f"Expected type '{expected_type}', got '{type(field_value).__name__}'"
                    continue
            
            # Min/max for numbers
            if isinstance(field_value, (int, float)):
                if "min" in rule_set and field_value < rule_set["min"]:
                    validation_result["valid"] = False
                    validation_result["errors"][field_path] = f"Value {field_value} is less than minimum {rule_set['min']}"
                    continue
                    
                if "max" in rule_set and field_value > rule_set["max"]:
                    validation_result["valid"] = False
                    validation_result["errors"][field_path] = f"Value {field_value} is greater than maximum {rule_set['max']}"
                    continue
            
            # Min/max length for strings and arrays
            if isinstance(field_value, (str, list)):
                if "min" in rule_set and len(field_value) < rule_set["min"]:
                    validation_result["valid"] = False
                    validation_result["errors"][field_path] = f"Length {len(field_value)} is less than minimum {rule_set['min']}"
                    continue
                    
                if "max" in rule_set and len(field_value) > rule_set["max"]:
                    validation_result["valid"] = False
                    validation_result["errors"][field_path] = f"Length {len(field_value)} is greater than maximum {rule_set['max']}"
                    continue
            
            # Pattern validation for strings
            if isinstance(field_value, str) and "pattern" in rule_set:
                if not check_pattern(field_value, rule_set["pattern"]):
                    validation_result["valid"] = False
                    validation_result["errors"][field_path] = f"Value doesn't match pattern '{rule_set['pattern']}'"
                    continue
            
            # Enum validation - check against allowed values
            if "values" in rule_set and field_value is not None:
                allowed_values = rule_set["values"]
                if isinstance(allowed_values, list) and field_value not in allowed_values:
                    validation_result["valid"] = False
                    validation_result["errors"][field_path] = f"Value '{field_value}' not in allowed values: {allowed_values}"
                    continue
            
            # Custom validation
            if "custom" in rule_set and field_value is not None:
                custom_expr = rule_set["custom"]
                try:
                    # Setup context for validation
                    value = field_value  # Local variable for validation expression
                    
                    # Evaluate the expression
                    if not eval(custom_expr):
                        validation_result["valid"] = False
                        validation_result["errors"][field_path] = f"Failed custom validation: {custom_expr}"
                        continue
                except Exception as e:
                    logger.error(f"🧰📝❌ Error in custom validation for '{field_path}': {e}")
                    validation_result["warnings"][field_path] = f"Invalid validation rule: {e}"
            
        # Strict mode - check for extra fields
        if py_strict:
            # Flatten the data
            flat_data = {}
            
            def flatten_dict(d, parent_key="", sep="."):
                """Flatten nested dictionary to dot notation."""
                items = []
                for k, v in d.items() if isinstance(d, dict) else enumerate(d) if isinstance(d, list) else []:
                    new_key = f"{parent_key}{sep}{k}" if parent_key else str(k)
                    
                    if isinstance(v, (dict, list)) and v:  # Skip empty containers
                        items.extend(flatten_dict(v, new_key, sep=sep).items())
                    else:
                        items.append((new_key, v))
                return dict(items)
            
            if isinstance(py_data, (dict, list)):
                flat_data = flatten_dict(py_data)
                
                # Check for extra fields
                rule_paths = set(py_rules.keys())
                data_paths = set(flat_data.keys())
                
                # Find paths in data but not in rules
                extra_paths = data_paths - rule_paths
                
                # Add warnings for extra fields
                for path in extra_paths:
                    validation_result["warnings"][path] = "Field not defined in rules"
        
        logger.debug(f"🧰📝✅ Validation complete: valid={validation_result['valid']}")
        logger.debug(f"🧰📝✅ Errors: {len(validation_result['errors'])}, Warnings: {len(validation_result['warnings'])}")
        
        # Convert result back to CTY
        cty_result = python_to_cty(validation_result)
        logger.debug(f"🧰🔍✅ Converted result to CTY: {type(cty_result).__name__}")
        
        return cty_result
        
    except Exception as e:
        error_msg = f"Validation failed: {e}"
        logger.error(f"🧰📝❌ Glom validate failed: {error_msg}")
        raise FunctionError(error_msg)
