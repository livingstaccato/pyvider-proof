"""
CtyList type implementation for Terraform.

Represents an ordered sequence of values of the same type. CtyLists in Terraform
maintain order and allow duplicate values, similar to Python lists.
"""

from dataclasses import dataclass
from typing import Any, ClassVar, Generic, TypeVar, final

from pyvider.cty.exceptions import PyviderError, ValidationError

from pyvider.cty.types.base import CtyType

T = TypeVar('T')

@final
@dataclass(frozen=True, slots=True)
class CtyList(CtyType[list[T]], Generic[T]):
    """
    CtyList type for Terraform values.

    A CtyList represents an ordered sequence of values, all having the same type.
    """

    ctype: ClassVar[str] = "list"
    element_type: CtyType[T]

    def __post_init__(self) -> None:
        if not isinstance(self.element_type, CtyType):
            raise PyviderError(
                f"Expected CtyType for element_type, got {type(self.element_type)}"
            )

    def validate(self, value: Any) -> list[T]:
        if value is None:
            return []  # Treat None as an empty list to avoid raising errors

        if not isinstance(value, (list, tuple)):
            raise PyviderError(
                f"Expected list or tuple, got {type(value).__name__}"
            )

        validated: list[T] = []
        for i, item in enumerate(value):
            try:
                validated.append(self.element_type.validate(item))
            except Exception as e:
                raise ValidationError(
                    f"Invalid element at index {i}: {e!s}"
                ) from e

        return validated

    def element_at(self, value: list[T], index: int) -> T:
        if not isinstance(value, list):
            raise ValidationError(f"Expected list, got {type(value).__name__}")

        try:
            return value[index]
        except IndexError:
            raise IndexError(
                f"CtyList index {index} out of range for list of length {len(value)}"
            )

    def equal(self, other: "CtyType") -> bool:
        """Check if types are equal."""
        if not isinstance(other, CtyList):
            return False
        return self.element_type.equal(other.element_type)

    def usable_as(self, other: "CtyType") -> bool:
        """
        Checks if this tuple type can be used as another type.

        Args:
            other (CtyType): Another type to check compatibility with.

        Returns:
            bool: True if compatible, False otherwise.
        """
        return isinstance(other, CtyList) and all(
            issubclass(self_type, other_type)
            for self_type, other_type in zip(self.types, other.types)
        )

    def __str__(self) -> str:
        """Get string representation of the type."""
        return f"list({self.element_type})"

    def __eq__(self, other):
        if not isinstance(other, CtyList):
            return False
        return self.element_type == other.element_type

    def __repr__(self):
        return f"{self.__class__.__name__}()"
