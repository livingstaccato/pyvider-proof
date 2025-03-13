from typing import Any, ClassVar

from attrs import define, field

from pyvider.cty.exceptions import ValidationError

from ..base import CtyType


@define(frozen=True, slots=True)
class CtyBool(CtyType[bool]):
    ctype: ClassVar[str] = "bool"
    value: bool = field(default=False)

    def validate(self, value: Any) -> "CtyBool":
        if isinstance(value, bool):
            return CtyBool(value=value)
        raise ValidationError("Value must be a boolean.")

    def equal(self, other: "CtyType[bool]") -> bool:
        """Check equality with another type."""
        return isinstance(other, CtyBool)

    def usable_as(self, other: "CtyType[bool]") -> bool:
        """Check if this type can be used as another type."""
        return isinstance(other, CtyBool)

    def __eq__(self, other):
        return super().__eq__(other)

    def __repr__(self):
        return f"{self.__class__.__name__}(value={self.value})"

    def __hash__(self):
        return hash(self.value)
