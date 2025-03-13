from abc import ABC, abstractmethod
from typing import (
    Any,
    ClassVar,
    Generic,
    Optional,
    TypeVar,
)

from attrs import define

from pyvider.cty.exceptions import ValidationError

T = TypeVar("T")

@define(slots=True)
class CtyType(ABC, Generic[T]):
    """Generic abstract base class for all Terraform types."""
    ctype: ClassVar[Optional[str]] = None  # Abstract class - no ctype by default

    @classmethod
    def from_raw(cls, value: Any) -> "CtyType":
        """Convert raw Python types to CtyType instances."""
        if isinstance(value, cls):
            return value
        raise ValidationError(
            f"Cannot convert {type(value).__name__} to {cls.__name__}."
        )

    @abstractmethod
    def validate(self, value: Any) -> T:
        """Validate and coerce the value to this type."""

    @abstractmethod
    def equal(self, other: "CtyType[T]") -> bool:
        """Check equality between this type and another."""

    @abstractmethod
    def usable_as(self, other: "CtyType[T]") -> bool:
        """Determine if this type can be used as another."""

    def __eq__(self, other: "CtyType[T]") -> bool:
        return isinstance(other, CtyType)

    def __str__(self) -> str:
        return f"{self.__class__.__name__}"

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}()"
