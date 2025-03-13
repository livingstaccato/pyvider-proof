
from typing import Any, Generic, TypeVar

from pyvider.cty.exceptions import ValidationError

from pyvider.cty.types.base import CtyType

# Define generic type variables
T = TypeVar("T")
U = TypeVar("U")


class CtyTuple(CtyType[tuple[T, U]], Generic[T, U]):
    """
    Represents a Terraform tuple type with a fixed structure.
    """

    def __init__(self, types: tuple[Any, ...]):
        """
        Initializes the tuple type with the expected element types.

        Args:
            types (CtyTuple[Any, ...]): A tuple of types defining the expected structure.
        """
        self.types = types

    def validate(self, value: tuple[T, U]) -> None:
        """
        Validates that the value matches the expected tuple structure.

        Args:
            value (CtyTuple): The value to validate.

        Raises:
            ValidationError: If the value does not match the structure.
        """
        if not isinstance(value, tuple):
            raise ValidationError(f"Expected a tuple, got {type(value).__name__}: {value}")
        if len(value) != len(self.types):
            raise ValidationError(f"Expected {len(self.types)} elements, got {len(value)}: {value}")
        for i, (item, expected_type) in enumerate(zip(value, self.types)):
            if not isinstance(item, expected_type):
                raise ValidationError(
                    f"Element {i} expected type {expected_type.__name__}, got {type(item).__name__}: {item}"
                )

    def equal(self, other: "CtyType") -> bool:
        """
        Checks if this tuple type is equal to another type.

        Args:
            other (CtyType): Another type to compare.

        Returns:
            bool: True if the types are equal, False otherwise.
        """
        return isinstance(other, CtyTuple) and self.types == other.types

    def usable_as(self, other: "CtyType") -> bool:
        """
        Checks if this tuple type can be used as another type.

        Args:
            other (CtyType): Another type to check compatibility with.

        Returns:
            bool: True if compatible, False otherwise.
        """
        return isinstance(other, CtyTuple) and all(
            issubclass(self_type, other_type)
            for self_type, other_type in zip(self.types, other.types)
        )

    def __repr__(self):
        return f"{self.__class__.__name__}()"

    def __eq__(self, other):
        return super().__eq__(other)

    def __str__(self) -> str:
        """
        Returns a string representation of the tuple type.

        Returns:
            str: The string representation.
        """
        return f"CtyTuple({', '.join(t.__name__ for t in self.types)})"
