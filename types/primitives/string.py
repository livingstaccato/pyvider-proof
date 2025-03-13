from typing import Any, ClassVar, TypeVar

from attrs import define, evolve, field

from pyvider.cty.exceptions import ValidationError

from ..base import CtyType

T = TypeVar('T', bound=str)

@define(frozen=True, slots=True)
class CtyString(CtyType[str]):
    ctype: ClassVar[str] = "string"
    value: str = field(default="")

    def validate(self, value):
        if not isinstance(value, str):
            raise ValidationError("Value must be a string.")
        # Evolve and return the new instance
        return evolve(self, value=value)

    def equal(self, other: CtyType[Any]) -> bool:
        """
        Check equality with another type.

        Args:
            other: The type to compare with.

        Returns:
            bool: True if the other type is a CtyString type.
        """
        return isinstance(other, CtyString)

    def usable_as(self, other: CtyType[Any]) -> bool:
        """
        Check if this type can be used as another type.

        Args:
            other: The type to check compatibility with.

        Returns:
            bool: True if this type can be used as the other type.
        """
        return isinstance(other, CtyString)

    def __eq__(self, other):
        return isinstance(other, CtyString) and self.value == other.value

    def __hash__(self) -> int:
        return hash((self.__class__,))

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}()"

    def __str__(self) -> str:
        return "CtyString"
