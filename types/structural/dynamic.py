
from pyvider.cty.exceptions import ValidationError
from pyvider.cty.types.base import CtyType

class CtyDynamic(CtyType):
    """
    CtyDynamic represents a dynamic Terraform type that can accept any value.
    This type acts as a catch-all during schema validation, allowing flexibility 
    for attributes whose structure or type cannot be determined at schema definition time.
    """

    def validate(self, value: object) -> None:
        """
        Validation for CtyDynamic is a no-op since it accepts any value.

        Args:
            value (object): Any value to validate.

        Raises:
            ValidationError: If the value is explicitly set to an unsupported form.
        """
        if isinstance(value, (dict, list, int, float, bool, str, type(None))):
            return  # All standard types are acceptable

        raise ValidationError("Unsupported value for CtyDynamic. Acceptable types are primitive types, dict, list, or None.")

    def equal(self, other: CtyType) -> bool:
        """
        CtyDynamic instances are considered equal to any other instance of CtyDynamic.

        Args:
            other (CtyType): Another CtyType instance.

        Returns:
            bool: True if the types are compatible, otherwise False.
        """
        return isinstance(other, CtyDynamic)

    def usable_as(self, other: CtyType) -> bool:
        """
        CtyDynamic can be used interchangeably with any other CtyDynamic.

        Args:
            other (CtyType): Target CtyType to compare against.

        Returns:
            bool: True if usable as the target type.
        """
        return isinstance(other, CtyDynamic)

    def __str__(self) -> str:
        return "CtyDynamic"

    def __repr__(self) -> str:
        return "CtyDynamic()"

# Factory function for schema definition

def tfdynamic(**kwargs) -> 'AttributeValue':
    """
    Factory method for creating a CtyDynamic attribute in schema definitions.

    Returns:
        AttributeValue: An attribute containing CtyDynamic as its type.
    """
    from pyvider.schema.attributes import AttributeMetadata, AttributeValue
    meta = AttributeMetadata(**kwargs)
    return AttributeValue(ctype=CtyDynamic(), metadata=meta)
