
# pyvider/cty/types/structural/object.py

"""
CtyObject implementation for Terraform object values.

The CtyObject type represents a complex value with a fixed set of attributes,
each having its own type. Unlike maps, objects have a predefined schema that
validates attribute types and presence/absence of required attributes.
"""

import logging
from typing import Any, Dict, FrozenSet, Optional, Set, Type, Union, cast

import attrs

from pyvider.cty.logger import logger
from pyvider.cty.types.base import CtyType
from pyvider.cty.types.structural.dynamic import CtyDynamic
from pyvider.cty.exceptions import (
    AttributeValidationError,
    InvalidTypeError,
    SchemaValidationError,
    ValidationError,
)


@attrs.define(frozen=True, slots=True)
class CtyObject(CtyType[Dict[str, Any]]):
    """
    Represents a Terraform object type with a fixed set of attributes.
    
    An object has a predefined schema with strictly typed attributes.
    Unlike maps, objects have known attribute names and types at definition time,
    and can have attributes of different types.
    
    Attributes:
        attribute_types: Dictionary mapping attribute names to their types
        optional_attributes: Set of attribute names that are optional
        computed_attributes: Set of attribute names computed by the provider
        block_attributes: Set of attribute names that represent blocks
        sensitive_attributes: Set of attribute names containing sensitive data
    """
    attribute_types: Dict[str, CtyType] = attrs.field(factory=dict)
    optional_attributes: FrozenSet[str] = attrs.field(factory=frozenset)
    computed_attributes: FrozenSet[str] = attrs.field(factory=frozenset)
    block_attributes: FrozenSet[str] = attrs.field(factory=frozenset)
    sensitive_attributes: FrozenSet[str] = attrs.field(factory=frozenset)
    
    def __attrs_post_init__(self) -> None:
        """Validate object type configuration."""
        logger.debug("🧩🔍 Validating CtyObject configuration on initialization")
        
        # Validate attribute_types is a dictionary
        if not isinstance(self.attribute_types, dict):
            error_msg = f"Expected dict for attribute_types, got {type(self.attribute_types).__name__}"
            logger.error(f"🧩❌ {error_msg}")
            raise InvalidTypeError(error_msg)
        
        # Validate all types are CtyType instances
        invalid_types = [
            name for name, type_ in self.attribute_types.items()
            if not isinstance(type_, CtyType)
        ]
        if invalid_types:
            error_msg = f"Invalid types for attributes: {', '.join(invalid_types)}"
            logger.error(f"🧩❌ {error_msg}")
            raise AttributeValidationError(error_msg)
        
        # Validate optional attributes exist in the type definition
        unknown_optional = set(self.optional_attributes) - set(self.attribute_types)
        if unknown_optional:
            error_msg = f"Unknown optional attributes: {', '.join(unknown_optional)}"
            logger.error(f"🧩❌ {error_msg}")
            raise AttributeValidationError(error_msg)
        
        # Validate computed attributes exist
        unknown_computed = set(self.computed_attributes) - set(self.attribute_types)
        if unknown_computed:
            error_msg = f"Unknown computed attributes: {', '.join(unknown_computed)}"
            logger.error(f"🧩❌ {error_msg}")
            raise AttributeValidationError(error_msg)
        
        # Validate block attributes exist
        unknown_blocks = set(self.block_attributes) - set(self.attribute_types)
        if unknown_blocks:
            error_msg = f"Unknown block attributes: {', '.join(unknown_blocks)}"
            logger.error(f"🧩❌ {error_msg}")
            raise AttributeValidationError(error_msg)
        
        # Validate sensitive attributes exist
        unknown_sensitive = set(self.sensitive_attributes) - set(self.attribute_types)
        if unknown_sensitive:
            error_msg = f"Unknown sensitive attributes: {', '.join(unknown_sensitive)}"
            logger.error(f"🧩❌ {error_msg}")
            raise AttributeValidationError(error_msg)
        
        logger.debug("🧩✅ CtyObject configuration validated successfully")
    
    def validate(self, value: Any) -> Dict[str, Any]:
        """
        Validate a value against this object type.
        
        Args:
            value: Value to validate (dictionary or None)
        
        Returns:
            Dict[str, Any]: The validated value
        
        Raises:
            ValidationError: If the value doesn't match this type
        """
        logger.debug(f"🧩🔍 Validating value against CtyObject: {value}")
        
        # Handle null values as None
        if value is None:
            logger.debug("🧩🔍 Received null value, returning None")
            return None
        
        # Value must be a dictionary
        if not isinstance(value, dict):
            type_name = type(value).__name__
            error_msg = f"Expected a dictionary, got {type_name}: {value}"
            logger.error(f"🧩🔍❌ {error_msg}")
            raise ValidationError(error_msg)
        
        # Check for required attributes
        required_attrs = self.required_attributes()
        logger.debug(f"🧩🔍 Required attributes: {required_attrs}")
        for name in required_attrs:
            if name not in value:
                error_msg = f"Missing required attribute: {name}"
                logger.error(f"🧩🔍❌ {error_msg}")
                raise ValidationError(error_msg)
        
        # Check for unknown attributes
        unknown_attrs = set(value.keys()) - set(self.attribute_types.keys())
        if unknown_attrs:
            error_msg = f"Unknown attributes: {', '.join(unknown_attrs)}"
            logger.warning(f"🧩🔍⚠️ {error_msg}")
            if len(unknown_attrs) > 0:
                # In this implementation, we'll raise an error for unknown attributes
                # This can be changed to just warn if needed
                logger.error(f"🧩🔍❌ {error_msg}")
                raise ValidationError(error_msg)
        
        # Validate each attribute
        validated = {}
        
        # Process each attribute
        for name, attr_type in self.attribute_types.items():
            logger.debug(f"🧩🔍 Validating attribute {name} with type {attr_type}")
            
            # Skip if attribute isn't present and is optional
            if name not in value:
                if name in self.optional_attributes or name in self.computed_attributes:
                    logger.debug(f"🧩🔍 Attribute {name} is optional/computed and not provided")
                    validated[name] = None
                continue
            
            # Get the attribute value
            attr_value = value[name]
            
            try:
                # Validate the attribute
                validated_value = attr_type.validate(attr_value)
                logger.debug(f"🧩🔍✅ Validated attribute {name}: {validated_value}")
                validated[name] = validated_value
            except ValidationError as e:
                error_msg = f"Invalid value for attribute '{name}': {e}"
                logger.error(f"🧩🔍❌ {error_msg}")
                raise ValidationError(error_msg) from e
            except Exception as e:
                error_msg = f"Error validating attribute '{name}': {e}"
                logger.error(f"🧩🔍❌ {error_msg}")
                raise ValidationError(error_msg) from e
        
        logger.debug(f"🧩🔍✅ Successfully validated object: {validated}")
        return validated
    
    def required_attributes(self) -> FrozenSet[str]:
        """
        Get the set of required attribute names.
        
        Returns:
            FrozenSet[str]: Names of all required attributes
        """
        return frozenset(
            name for name in self.attribute_types
            if name not in self.optional_attributes
            and name not in self.computed_attributes
        )
    
    def get_attribute(self, value: Dict[str, Any], name: str) -> Any:
        """
        Get an attribute value by name.
        
        Args:
            value: Object value to access
            name: Name of attribute to get
        
        Returns:
            The attribute value
        
        Raises:
            AttributeValidationError: If attribute doesn't exist
            ValidationError: If value is not a valid object
        """
        logger.debug(f"🧩🔍 Getting attribute {name} from object")
        
        # Validate input
        if not isinstance(value, dict):
            type_name = type(value).__name__
            error_msg = f"Expected a dictionary, got {type_name}: {value}"
            logger.error(f"🧩🔍❌ {error_msg}")
            raise ValidationError(error_msg)
        
        # Check attribute exists in schema
        if name not in self.attribute_types:
            error_msg = f"Unknown attribute: {name}"
            logger.error(f"🧩🔍❌ {error_msg}")
            raise AttributeValidationError(error_msg)
        
        # Return attribute value (may be None)
        attr_value = value.get(name)
        logger.debug(f"🧩🔍✅ Found attribute {name}: {attr_value}")
        return attr_value
    
    def has_attribute(self, name: str) -> bool:
        """
        Check if an attribute exists in this object type.
        
        Args:
            name: Attribute name to check
        
        Returns:
            bool: True if the attribute exists
        """
        result = name in self.attribute_types
        logger.debug(f"🧩🔍 Checking if attribute {name} exists: {result}")
        return result
    
    def with_optional_attributes(self, *names: str) -> "CtyObject":
        """
        Create a new object type with additional optional attributes.
        
        Args:
            *names: Names of attributes to mark as optional
        
        Returns:
            CtyObject: New object type with updated optional attributes
        
        Raises:
            SchemaValidationError: If any name is not a valid attribute
        """
        logger.debug(f"🧩🔧 Creating new object type with optional attributes: {names}")
        
        # Validate all names exist in attribute_types
        unknown = set(names) - set(self.attribute_types)
        if unknown:
            error_msg = f"Unknown attributes: {', '.join(unknown)}"
            logger.error(f"🧩🔧❌ {error_msg}")
            raise SchemaValidationError(error_msg)
        
        # Create new optional set
        new_optional = frozenset(set(self.optional_attributes) | set(names))
        
        # Create new object type
        new_obj = CtyObject(
            attribute_types=self.attribute_types,
            optional_attributes=new_optional,
            computed_attributes=self.computed_attributes,
            block_attributes=self.block_attributes,
            sensitive_attributes=self.sensitive_attributes
        )
        
        logger.debug(f"🧩🔧✅ Created new object type with optional attributes: {new_optional}")
        return new_obj
    
    def with_required_attributes(self, *names: str) -> "CtyObject":
        """
        Create a new object type with additional required attributes.
        
        Args:
            *names: Names of attributes to mark as required
        
        Returns:
            CtyObject: New object type with updated required attributes
        
        Raises:
            SchemaValidationError: If any name is not a valid attribute
        """
        logger.debug(f"🧩🔧 Creating new object type with required attributes: {names}")
        
        # Validate all names exist in attribute_types and are currently optional
        unknown = set(names) - set(self.attribute_types)
        if unknown:
            error_msg = f"Unknown attributes: {', '.join(unknown)}"
            logger.error(f"🧩🔧❌ {error_msg}")
            raise SchemaValidationError(error_msg)
        
        not_optional = set(names) - set(self.optional_attributes)
        if not_optional:
            error_msg = f"Attributes already required: {', '.join(not_optional)}"
            logger.error(f"🧩🔧❌ {error_msg}")
            raise SchemaValidationError(error_msg)
        
        # Create new optional set
        new_optional = frozenset(set(self.optional_attributes) - set(names))
        
        # Create new object type
        new_obj = CtyObject(
            attribute_types=self.attribute_types,
            optional_attributes=new_optional,
            computed_attributes=self.computed_attributes,
            block_attributes=self.block_attributes,
            sensitive_attributes=self.sensitive_attributes
        )
        
        logger.debug(f"🧩🔧✅ Created new object type with required attributes: {names}")
        return new_obj
    
    def with_computed_attributes(self, *names: str) -> "CtyObject":
        """
        Create a new object type with additional computed attributes.
        
        Args:
            *names: Names of attributes to mark as computed
        
        Returns:
            CtyObject: New object type with updated computed attributes
        
        Raises:
            SchemaValidationError: If any name is not a valid attribute
        """
        logger.debug(f"🧩🔧 Creating new object type with computed attributes: {names}")
        
        # Validate all names exist in attribute_types
        unknown = set(names) - set(self.attribute_types)
        if unknown:
            error_msg = f"Unknown attributes: {', '.join(unknown)}"
            logger.error(f"🧩🔧❌ {error_msg}")
            raise SchemaValidationError(error_msg)
        
        # Create new computed set
        new_computed = frozenset(set(self.computed_attributes) | set(names))
        
        # Create new object type
        new_obj = CtyObject(
            attribute_types=self.attribute_types,
            optional_attributes=self.optional_attributes,
            computed_attributes=new_computed,
            block_attributes=self.block_attributes,
            sensitive_attributes=self.sensitive_attributes
        )
        
        logger.debug(f"🧩🔧✅ Created new object type with computed attributes: {new_computed}")
        return new_obj
    
    def with_block_attributes(self, *names: str) -> "CtyObject":
        """
        Create a new object type with additional block attributes.
        
        Args:
            *names: Names of attributes to mark as blocks
        
        Returns:
            CtyObject: New object type with updated block attributes
        
        Raises:
            SchemaValidationError: If any name is not a valid attribute
        """
        logger.debug(f"🧩🔧 Creating new object type with block attributes: {names}")
        
        # Validate all names exist in attribute_types
        unknown = set(names) - set(self.attribute_types)
        if unknown:
            error_msg = f"Unknown attributes: {', '.join(unknown)}"
            logger.error(f"🧩🔧❌ {error_msg}")
            raise SchemaValidationError(error_msg)
        
        # Create new block set
        new_blocks = frozenset(set(self.block_attributes) | set(names))
        
        # Create new object type
        new_obj = CtyObject(
            attribute_types=self.attribute_types,
            optional_attributes=self.optional_attributes,
            computed_attributes=self.computed_attributes,
            block_attributes=new_blocks,
            sensitive_attributes=self.sensitive_attributes
        )
        
        logger.debug(f"🧩🔧✅ Created new object type with block attributes: {new_blocks}")
        return new_obj
    
    def with_sensitive_attributes(self, *names: str) -> "CtyObject":
        """
        Create a new object type with additional sensitive attributes.
        
        Args:
            *names: Names of attributes to mark as sensitive
        
        Returns:
            CtyObject: New object type with updated sensitive attributes
        
        Raises:
            SchemaValidationError: If any name is not a valid attribute
        """
        logger.debug(f"🧩🔧 Creating new object type with sensitive attributes: {names}")
        
        # Validate all names exist in attribute_types
        unknown = set(names) - set(self.attribute_types)
        if unknown:
            error_msg = f"Unknown attributes: {', '.join(unknown)}"
            logger.error(f"🧩🔧❌ {error_msg}")
            raise SchemaValidationError(error_msg)
        
        # Create new sensitive set
        new_sensitive = frozenset(set(self.sensitive_attributes) | set(names))
        
        # Create new object type
        new_obj = CtyObject(
            attribute_types=self.attribute_types,
            optional_attributes=self.optional_attributes,
            computed_attributes=self.computed_attributes,
            block_attributes=self.block_attributes,
            sensitive_attributes=new_sensitive
        )
        
        logger.debug(f"🧩🔧✅ Created new object type with sensitive attributes: {new_sensitive}")
        return new_obj
    
    def with_attribute(self, name: str, type_: CtyType, *, 
                      optional: bool = False, computed: bool = False, 
                      block: bool = False, sensitive: bool = False) -> "CtyObject":
        """
        Create a new object type with an additional attribute.
        
        Args:
            name: Name of the new attribute
            type_: Type of the new attribute
            optional: Whether the attribute is optional
            computed: Whether the attribute is computed
            block: Whether the attribute is a block
            sensitive: Whether the attribute is sensitive
        
        Returns:
            CtyObject: New object type with the additional attribute
        
        Raises:
            SchemaValidationError: If the name already exists
        """
        logger.debug(f"🧩🔧 Creating new object type with attribute: {name} ({type_.__class__.__name__})")
        
        # Validate attribute doesn't already exist
        if name in self.attribute_types:
            error_msg = f"Attribute already exists: {name}"
            logger.error(f"🧩🔧❌ {error_msg}")
            raise SchemaValidationError(error_msg)
        
        # Create new attribute_types dict
        new_attrs = dict(self.attribute_types)
        new_attrs[name] = type_
        
        # Update attribute sets based on flags
        new_optional = set(self.optional_attributes)
        new_computed = set(self.computed_attributes)
        new_blocks = set(self.block_attributes)
        new_sensitive = set(self.sensitive_attributes)
        
        if optional:
            new_optional.add(name)
        if computed:
            new_computed.add(name)
        if block:
            new_blocks.add(name)
        if sensitive:
            new_sensitive.add(name)
        
        # Create new object type
        new_obj = CtyObject(
            attribute_types=new_attrs,
            optional_attributes=frozenset(new_optional),
            computed_attributes=frozenset(new_computed),
            block_attributes=frozenset(new_blocks),
            sensitive_attributes=frozenset(new_sensitive)
        )
        
        logger.debug(f"🧩🔧✅ Created new object type with attribute: {name}")
        return new_obj
    
    def equal(self, other: CtyType) -> bool:
        """
        Check if this type equals another type.
        
        Args:
            other: Another type to compare
        
        Returns:
            bool: True if the types are equal
        """
        logger.debug(f"🧩🔍 Checking equality with {other.__class__.__name__}")
        
        # Must be a CtyObject
        if not isinstance(other, CtyObject):
            logger.debug(f"🧩🔍❌ Not equal: {other.__class__.__name__} is not CtyObject")
            return False
        
        # Must have same attribute names
        if set(self.attribute_types) != set(other.attribute_types):
            logger.debug("🧩🔍❌ Not equal: attribute names differ")
            return False
        
        # Must have same attribute types
        for name, type_ in self.attribute_types.items():
            other_type = other.attribute_types[name]
            if not type_.equal(other_type):
                logger.debug(f"🧩🔍❌ Not equal: attribute {name} types differ")
                return False
        
        # Must have same optional attributes
        if self.optional_attributes != other.optional_attributes:
            logger.debug("🧩🔍❌ Not equal: optional attributes differ")
            return False
        
        # Must have same computed attributes
        if self.computed_attributes != other.computed_attributes:
            logger.debug("🧩🔍❌ Not equal: computed attributes differ")
            return False
        
        # Must have same block attributes
        if self.block_attributes != other.block_attributes:
            logger.debug("🧩🔍❌ Not equal: block attributes differ")
            return False
        
        # Must have same sensitive attributes
        if self.sensitive_attributes != other.sensitive_attributes:
            logger.debug("🧩🔍❌ Not equal: sensitive attributes differ")
            return False
        
        logger.debug("🧩🔍✅ Objects are equal")
        return True
    
    def usable_as(self, other: CtyType) -> bool:
        """
        Check if this type can be used as another type.
        
        Args:
            other: Target type to check
        
        Returns:
            bool: True if usable as the target type
        """
        logger.debug(f"🧩🔍 Checking usability as {other.__class__.__name__}")
        
        # Must be a CtyObject
        if not isinstance(other, CtyObject):
            logger.debug(f"🧩🔍❌ Not usable as {other.__class__.__name__}")
            return False
        
        # Other type must not have attributes that we don't have
        other_attrs = set(other.attribute_types)
        self_attrs = set(self.attribute_types)
        missing_attrs = other_attrs - self_attrs
        if missing_attrs:
            logger.debug(f"🧩🔍❌ Not usable: missing attributes {missing_attrs}")
            return False
        
        # For attributes in both, our type must be usable as other's type
        for name in other_attrs:
            self_type = self.attribute_types[name]
            other_type = other.attribute_types[name]
            if not self_type.equal(other_type):
                logger.debug(f"🧩🔍❌ Not usable: attribute {name} type not compatible")
                return False
        
        # Required attributes: other's required must be subset of ours
        other_required = other.required_attributes()
        self_required = self.required_attributes()
        if not other_required.issubset(self_required):
            extra_required = other_required - self_required
            logger.debug(f"🧩🔍❌ Not usable: other requires attributes we don't: {extra_required}")
            return False
        
        logger.debug("🧩🔍✅ Object is usable as target type")
        return True
    
    def __str__(self) -> str:
        """Get string representation of the type."""
        parts = []
        for name, type_ in sorted(self.attribute_types.items()):
            part = f"{name}: {type_}"
            
            flags = []
            if name in self.optional_attributes:
                flags.append("optional")
            if name in self.computed_attributes:
                flags.append("computed")
            if name in self.block_attributes:
                flags.append("block")
            if name in self.sensitive_attributes:
                flags.append("sensitive")
            
            if flags:
                part += f" ({', '.join(flags)})"
            
            parts.append(part)
        
        return f"object({{{ ', '.join(parts) }}})"


def create_object(**kwargs) -> CtyObject:
    """
    Create a CtyObject with attribute types specified as keyword arguments.
    
    This is a convenience function for creating CtyObject instances with a
    more readable syntax. Each keyword argument is an attribute name with
    its value being the attribute type.
    
    Example:
        person_type = create_object(
            name=CtyString(),
            age=CtyNumber(),
            is_active=CtyBool(),
            optional=["is_active"]
        )
    
    Args:
        **kwargs: Attribute types and configuration
        
    Returns:
        CtyObject: The created object type
    """
    # Extract special configuration parameters
    optional = kwargs.pop("optional", [])
    computed = kwargs.pop("computed", [])
    blocks = kwargs.pop("blocks", [])
    sensitive = kwargs.pop("sensitive", [])
    
    # Create CtyObject with remaining kwargs as attribute_types
    return CtyObject(
        attribute_types=kwargs,
        optional_attributes=frozenset(optional),
        computed_attributes=frozenset(computed),
        block_attributes=frozenset(blocks),
        sensitive_attributes=frozenset(sensitive)
    )
