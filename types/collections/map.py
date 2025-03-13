import copy
from collections.abc import Mapping
from dataclasses import field
from types import MappingProxyType
from typing import Any, ClassVar, Generic, Optional, TypeVar, final

import attrs

from pyvider.cty.exceptions import ValidationError

from pyvider.cty.types.primitives import (
    CtyBool,
    CtyNumber,
    CtyString,
)
from pyvider.cty.types.base import CtyType

T = TypeVar("T")

@final
@attrs.define(frozen=True, slots=True)
class CtyMap(CtyType[dict[str, T]], Generic[T]):
    ctype: ClassVar[str] = "map"
    key_type: Optional[CtyType] = None
    value_type: CtyType = field()
    metadata: Optional[Mapping[str, Any]] = field(default=None)
    default: Optional[Mapping[str, T]] = field(default=None)
    mutable: bool = field(default=False)  # Default to immutable

    def __attrs_post_init__(self):
        if not isinstance(self.value_type, CtyType):
            raise ValidationError("Expected a valid CtyType for value_type")

        if self.key_type is not None and not isinstance(self.key_type, CtyType):
            raise ValidationError("Expected a valid CtyType for key_type")

        if self.key_type is None:
            from pyvider.cty import CtyString
            object.__setattr__(self, "key_type", CtyString())

        if isinstance(self.metadata, dict):
            metadata = copy.deepcopy(self.metadata)
            if not self.mutable:
                metadata = MappingProxyType(metadata)
            object.__setattr__(self, "metadata", metadata)

        if isinstance(self.default, dict):
            default = copy.deepcopy(self.default)
            if not self.mutable:
                default = MappingProxyType(default)
            object.__setattr__(self, "default", default)

    def validate(self, value: Any) -> dict[str, T]:
        if not isinstance(value, dict):
            raise ValidationError(f"Expected dict, got {type(value).__name__}")

        validated = {}
        for k, v in value.items():
            validated_key = self.key_type.validate(k)
            validated_value = self._wrap_value(v)

            if isinstance(validated_value, list):
                validated_value = [
                    item.value if isinstance(item, CtyType) else item for item in validated_value
                ]
            else:
                validated_value = (
                    validated_value.value if isinstance(validated_value, CtyType) else validated_value
                )

            validated[validated_key.value if isinstance(validated_key, CtyType) else validated_key] = validated_value

        return MappingProxyType(validated) if not self.mutable else validated

    def _wrap_value(self, value: Any) -> CtyType:
        if isinstance(value, CtyType):
            return value

        if isinstance(value, list):
            return [self._wrap_value(item) for item in value]

        if isinstance(value, dict):
            if isinstance(self.value_type, CtyMap):
                nested_map = CtyMap(
                    key_type=self.key_type,
                    value_type=self.value_type.value_type,
                    metadata=self.metadata,
                    mutable=self.mutable,
                )
                return nested_map.validate(value)
            raise ValidationError(f"Invalid type for nested dict. Expected {self.value_type.__class__.__name__}.")

        if isinstance(value, str) and isinstance(self.value_type, CtyMap):
            # Convert string to single-entry map if expected type is CtyMap
            return self.value_type.validate({value: value})

        if isinstance(value, bool) and isinstance(self.value_type, CtyBool):
            return CtyBool(value)
        if isinstance(value, (int, float)) and isinstance(self.value_type, CtyNumber):
            return CtyNumber(value)
        if isinstance(value, str) and isinstance(self.value_type, CtyString):
            return CtyString(value)

        raise ValidationError(
            f"Invalid type for map value: {type(value).__name__}. Expected {self.value_type.__class__.__name__}."
        )

    def equal(self, other: "CtyType") -> bool:
        return (
            isinstance(other, CtyMap)
            and self.key_type.equal(other.key_type)
            and self.value_type.equal(other.value_type)
            and self.metadata == other.metadata
            and self.default == other.default
            and self.mutable == other.mutable
        )

    def usable_as(self, other: "CtyType") -> bool:
        return isinstance(other, CtyMap) and self.value_type.usable_as(other.value_type)

    def __eq__(self, other):
        if not isinstance(other, CtyMap):
            return False
        return (
            self.key_type.equal(other.key_type)
            and self.value_type.equal(other.value_type)
            and self.metadata == other.metadata
            and self.default == other.default
        )
