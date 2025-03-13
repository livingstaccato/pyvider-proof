from typing import Any, ClassVar, Generic, TypeVar, final
from typing import Set as PySet

from attrs import define, evolve, field

from pyvider.cty.exceptions import ValidationError

from pyvider.cty.types.base import CtyType

T = TypeVar('T')

@final
@define(frozen=True, slots=True)
class CtySet(CtyType[PySet[T]], Generic[T]):
    ctype: ClassVar[str] = "set"
    element_type: CtyType[T] = field(kw_only=True)  # Mandatory as keyword-only
    value: PySet[T] = field(factory=set, kw_only=True)  # Allow passing value via kw_only

    def __attrs_post_init__(self) -> None:
        if not isinstance(self.element_type, CtyType):
            raise ValidationError(
                f"Expected CtyType for element_type, got {type(self.element_type)}"
            )

    def validate(self, value: Any) -> "CtySet":
        if value is None:
            return evolve(self, value=set())
        if not hasattr(value, '__iter__') or isinstance(value, (str, bytes)):
            raise ValidationError(f"Expected iterable, got {type(value).__name__}")
        if not value:
            return evolve(self, value=set())
        validated = set()
        validation_errors = []

        def freeze_nested_sets(item):
            if isinstance(item, (set, frozenset)):
                raise ValidationError("Nested sets are not allowed in CtySet.")
            return self.element_type.validate(item)

        for i, item in enumerate(value):
            try:
                validated_item = freeze_nested_sets(item)
                validated.add(validated_item)
            except Exception as e:
                validation_errors.append(f"Item {i}: {item} -> {e!s}")

        if validation_errors:
            raise ValidationError("CtySet validation failed:\n" + "\n".join(validation_errors))

        return evolve(self, value=validated)

    def add(self, element):
        try:
            self.validate(self.value | {element})
            self.value.add(element)
        except ValidationError as e:
            raise ValidationError(f"Failed to add element: {e}")

    def remove(self, item: T) -> "CtySet":
        try:
            validated_item = self.element_type.validate(item)
            new_set = {x for x in self.value if x != validated_item}
            return evolve(self, value=new_set)
        except Exception as e:
            raise ValidationError(f"Failed to remove item: {e}")

    def usable_as(self, other: "CtyType") -> bool:
        return isinstance(other, CtySet) and self.element_type.usable_as(other.element_type)

    def equal(self, other: "CtyType") -> bool:
        if not isinstance(other, CtySet):
            return False
        return self.element_type.equal(other.element_type)

    def __eq__(self, other):
        if not isinstance(other, CtySet):
            return False
        return (
            self.element_type == other.element_type
            and self.value == other.value
        )

    def __iter__(self):
        return iter(self.value)
