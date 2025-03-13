# pyvider/cty/types/simple.py

from typing import ClassVar

from attrs import define, evolve, field

from pyvider.cty.exceptions import ValidationError

from ..base import CtyType


@define(frozen=True, slots=True)
class CtyNumber(CtyType[float]):
    ctype: ClassVar[str] = "number"
    value: float = field(default=0.0)

    def validate(self, value):
        if not isinstance(value, (int, float)):
            raise ValidationError("Value must be a number.")
        return evolve(self, value=value)

    def equal(self, other: "CtyType[float]") -> bool:
        return isinstance(other, CtyNumber)

    def usable_as(self, other: "CtyType[float]") -> bool:
        return isinstance(other, CtyNumber)

    # this needs to get fixed.
    #def __eq__(self, other):
    #    return super().__eq__(other)
    def __eq__(self, other):
        return isinstance(other, CtyNumber) and self.value == other.value

    def __lt__(self, other):
        return self.value < other.value

    def __gt__(self, other):
        return self.value > other.value

    def __repr__(self):
        return f"{self.__class__.__name__}()"

    def __hash__(self):
        return hash(self.value)
