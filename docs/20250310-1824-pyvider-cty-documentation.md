---
timestamp: 2025-03-10 18:24
---

# pyvider.cty Documentation

## Introduction

`pyvider.cty` is a robust Python implementation of a type system inspired by Terraform's Go-based "cty" (Custom TYpe) library. This module provides the foundational type system that powers Pyvider's schema validation, data transformation, and interoperability with Terraform.

```python
from pyvider.cty import (
    CtyString, CtyNumber, CtyBool,
    CtyList, CtyMap, CtySet,
    CtyObject, CtyDynamic
)
```

### Key Features

- **Strong, dynamic typing** with comprehensive validation
- **Special value states** (null, unknown) with proper propagation
- **Type-safe operations** that respect type constraints
- **Value marks** for metadata tracking through operations
- **Type conversion system** with safe and unsafe conversions
- **Path navigation** for complex nested structures
- **Serialization** to/from JSON, MessagePack, and Protobuf
- **Function system** with extensible registry

## Core Concepts

### Types and Values

The `pyvider.cty` module is built around two fundamental concepts:

1. **Types**: Define the structure and operations of data
2. **Values**: Combine data with type information

```python
# Creating types
string_type = CtyString()
number_type = CtyNumber()
list_type = CtyList(element_type=CtyString())

# Creating values
from pyvider.cty.values import CtyValue
string_value = CtyValue(string_type, "hello")
number_value = CtyValue(number_type, 42)
list_value = CtyValue(list_type, ["a", "b", "c"])
```

### Type Hierarchy

The type system is organized into three categories:

1. **Primitive Types**: Base data types
   - `CtyString`: Text values
   - `CtyNumber`: Numeric values (using Python's Decimal)
   - `CtyBool`: Boolean values

2. **Collection Types**: Homogeneous collections
   - `CtyList`: Ordered sequence of same-type values
   - `CtySet`: Unordered collection of unique values
   - `CtyMap`: Key-value mapping (string keys)

3. **Structural Types**: Heterogeneous collections
   - `CtyObject`: Named attributes with varying types
   - `CtyTuple`: Positional elements with varying types
   - `CtyDynamic`: Special type for unknown/dynamic values

### Special Value States

Values can exist in three states:

1. **Known**: Normal value with both type and data
2. **Unknown**: Has type but the value isn't known yet
3. **Null**: Has type but is explicitly null

```python
# Known value
normal_value = CtyValue(CtyString(), "hello")

# Unknown value
unknown_value = CtyValue.unknown(CtyString())

# Null value
null_value = CtyValue.null(CtyString())
```

Operations on values respect these states, propagating unknown and null appropriately through calculations.

## Type System Reference

### Creating and Using Primitive Types

```python
# String type
string_type = CtyString()
string_value = CtyValue(string_type, "hello world")

# Number type
number_type = CtyNumber()
number_value = CtyValue(number_type, 42.5)

# Boolean type
bool_type = CtyBool()
bool_value = CtyValue(bool_type, True)
```

### Creating and Using Collection Types

```python
# List of strings
string_list_type = CtyList(element_type=CtyString())
string_list_value = CtyValue(string_list_type, ["a", "b", "c"])

# Set of numbers
number_set_type = CtySet(element_type=CtyNumber())
number_set_value = CtyValue(number_set_type, {1, 2, 3})

# Map from strings to booleans
bool_map_type = CtyMap(key_type=CtyString(), value_type=CtyBool())
bool_map_value = CtyValue(bool_map_type, {"enabled": True, "visible": False})
```

### Creating and Using Structural Types

```python
# Object type with mixed attributes
person_type = CtyObject(
    attribute_types={
        "name": CtyString(),
        "age": CtyNumber(),
        "is_active": CtyBool()
    },
    optional_attributes=frozenset(["is_active"])
)

# Create a person value
person_value = CtyValue(person_type, {
    "name": "Alice",
    "age": 30,
    "is_active": True
})

# Tuple type with mixed elements
coordinate_type = CtyTuple(types=(CtyNumber(), CtyNumber(), CtyString()))
coordinate_value = CtyValue(coordinate_type, (10.5, 20.3, "degrees"))

# Dynamic type (can hold any value)
dynamic_type = CtyDynamic()
dynamic_value = CtyValue(dynamic_type, {"any": "value"})
```

## Value Operations

### Basic Operations

The `pyvider.cty.values.operations` module provides functions for operating on values:

```python
from pyvider.cty.values.operations import (
    equals, add, subtract, multiply, divide, negate
)

# Addition with type checking
result = add(CtyValue(CtyNumber(), 5), CtyValue(CtyNumber(), 3))  # 8
text_result = add(CtyValue(CtyString(), "Hello, "), CtyValue(CtyString(), "World!"))  # "Hello, World!"

# Comparison 
are_equal = equals(string1, string2)  # Returns CtyValue with bool type

# Arithmetic operations
sum_value = add(num1, num2)
diff_value = subtract(num1, num2)
product_value = multiply(num1, num2)
quotient_value = divide(num1, num2)
negated = negate(num1)
```

### Accessing Nested Data

```python
from pyvider.cty.values.operations import get_attribute, get_element

# Get an attribute from an object
name = get_attribute(person_value, "name")

# Get an element from a list or map
first_item = get_element(list_value, CtyValue(CtyNumber(), 0))
map_item = get_element(map_value, CtyValue(CtyString(), "key"))
```

### Collection Operations

```python
from pyvider.cty.values.operations import (
    length, contains, concat_lists, merge_maps, 
    slice_string, slice_list
)

# Get collection length
size = length(list_value)  # Returns CtyValue with number type

# Check if a collection contains a value
has_item = contains(list_value, string_value)

# Concatenate lists
combined = concat_lists(list1, list2)

# Merge maps
merged = merge_maps(map1, map2)

# Slicing
substring = slice_string(string_value, CtyValue(CtyNumber(), 1), CtyValue(CtyNumber(), 5))
sublist = slice_list(list_value, CtyValue(CtyNumber(), 0), CtyValue(CtyNumber(), 2))
```

## Path Navigation

The `pyvider.cty.path` module allows navigation through nested structures:

```python
from pyvider.cty.path.path import Path

# Create a path to a nested attribute
path = Path.empty().child("person").child("address").child("city")

# Apply the path to a value
city_value = await path.apply_path(complex_value)

# Create a path to a list element
element_path = Path.empty().child("items").index_step(0)

# Create a path to a map element
map_element_path = Path.empty().child("settings").key_step("theme")
```

## Type Conversion

The `pyvider.cty.convert` module provides a system for converting between types:

```python
from pyvider.cty.convert.convert import convert, convert_unsafe, can_convert

# Check if conversion is possible
convertible = await can_convert(string_value.type, number_value.type)

# Convert a value (only safe conversions)
converted = await convert(bool_value, CtyString())  # "true" or "false"

# Convert a value (allowing unsafe conversions)
unsafe_converted = await convert_unsafe(string_value, CtyNumber())  # May raise ConversionError
```

## Value Marks

Marks are metadata that propagate through operations:

```python
# Add a mark to a value
marked_value = value.mark("sensitive")

# Check if a value has a mark
is_sensitive = value.has_mark("sensitive")

# Remove marks
unmarked_value, marks = value.unmark()
```

## Encoding and Serialization

`pyvider.cty` supports various serialization formats:

```python
from pyvider.cty.encoding.json import marshal, unmarshal

# Convert to JSON
json_bytes = await marshal(value)

# Load from JSON
value = await unmarshal(json_bytes, CtyString())

# MessagePack support
from pyvider.cty.encoding.msgpack import encode_value, decode_value

# Protobuf support
from pyvider.cty.encoding.protobuf import encode_value, decode_value
```

## Function System

The function system allows defining and calling functions on values:

```python
from pyvider.cty.function.base import (
    FunctionSpec, Parameter, Function, registry
)

# Define a function
def upper_impl(args, return_type):
    if args[0].is_null or args[0].is_unknown:
        return CtyValue(return_type, is_null=args[0].is_null, is_unknown=args[0].is_unknown)
    return CtyValue(return_type, str(args[0].value).upper())

upper_fn = Function(FunctionSpec(
    name="upper",
    params=[Parameter(name="str", type=CtyString())],
    return_type_fn=lambda _: CtyString(),
    implementation=upper_impl,
    description="Converts a string to uppercase"
))

# Register the function
registry.register(upper_fn)

# Call a function
result = await upper_fn(CtyValue(CtyString(), "hello"))  # "HELLO"
```

## Schema Integration

`pyvider.cty` types can be used with the schema system:

```python
from pyvider.schema import (
    SchemaType, StringKind, Attribute, AttributeType,
    AttributeMetadata, AttributeValue
)

# Create a schema attribute
name_attr = Attribute(
    name="name",
    type=AttributeType(type_name=SchemaType.STRING),
    required=True
)

# Create a schema object
person_schema = Schema(
    block=Block(
        attributes=[name_attr, age_attr, active_attr],
        description="Person definition",
        description_kind=StringKind.PLAIN
    )
)

# Factory functions for cty-based schema attributes
from pyvider.schema.attributes import tfstr, tfnum, tfbool, tfobj

# Create schema with cty types
person_attrs = {
    "name": tfstr(required=True, description="Person's name"),
    "age": tfnum(description="Person's age"),
    "is_active": tfbool(default=True)
}

person_schema = tfobj(person_attrs, description="Person definition")
```

## Best Practices

### Type Creation

- Create types once and reuse them throughout your code
- Use factory functions for common types
- Define a type registry for your application

```python
# Type registry pattern
TYPES = {
    "string": CtyString(),
    "number": CtyNumber(),
    "bool": CtyBool(),
    "string_list": CtyList(element_type=CtyString()),
    "person": CtyObject(
        attribute_types={
            "name": CtyString(),
            "age": CtyNumber()
        }
    )
}
```

### Value Handling

- Always check for unknown and null states when processing values
- Use marks to track metadata about values (e.g., "sensitive")
- Consider using the path system for nested data access

```python
def process_value(value):
    if not value.is_known:
        return "Value is unknown"
    if value.is_null:
        return "Value is null"
    if value.has_mark("sensitive"):
        return "Value is sensitive: [redacted]"
    return f"Value: {value.value}"
```

### Error Handling

- Use specific exception types for better error handling
- Catch `ValidationError` for validation failures
- Catch `ConversionError` for conversion failures
- Catch `CtyError` as a general fallback

```python
from pyvider.cty.exceptions import ValidationError, ConversionError, CtyError

try:
    # Attempt validation
    validated = my_type.validate(input_value)
except ValidationError as e:
    print(f"Invalid input: {e}")
except CtyError as e:
    print(f"General error: {e}")
```

## Advanced Topics

### Custom Types

Create custom types by subclassing `CtyType`:

```python
from pyvider.cty.types.base import CtyType
from typing import ClassVar

class CtyEmail(CtyType[str]):
    ctype: ClassVar[str] = "email"
    
    def validate(self, value):
        if not isinstance(value, str):
            raise ValidationError("Email must be a string")
        if "@" not in value:
            raise ValidationError("Email must contain @")
        return value
    
    def equal(self, other):
        return isinstance(other, CtyEmail)
    
    def usable_as(self, other):
        return isinstance(other, (CtyEmail, CtyString))
```

### Value Refinements

Refine unknown values with constraints:

```python
from pyvider.cty.values.refinement import ValueRefinementBuilder

# Create an unknown number with constraints
unknown_age = CtyValue.unknown(CtyNumber())
refined_age = unknown_age.refine().number_range_inclusive(0, 120).new_value()
```

### Performance Considerations

- Create types once and reuse them
- Avoid unnecessary validation when values are already validated
- Use the appropriate serialization format for your needs

## Common Patterns and Examples

### Configuration Validation

```python
# Define a configuration schema
config_type = CtyObject(
    attribute_types={
        "server": CtyString(),
        "port": CtyNumber(),
        "features": CtyList(element_type=CtyString()),
        "logging": CtyObject(
            attribute_types={
                "level": CtyString(),
                "file": CtyString()
            },
            optional_attributes=frozenset(["file"])
        )
    }
)

# Validate configuration
try:
    validated_config = config_type.validate(user_config)
    config_value = CtyValue(config_type, validated_config)
except ValidationError as e:
    print(f"Configuration error: {e}")
```

### Data Transformation

```python
# Transform data between formats
def transform_data(input_data):
    # Validate against input schema
    validated = input_schema.validate(input_data)
    input_value = CtyValue(input_schema, validated)
    
    # Transform to output format
    output = {
        "name": get_attribute(input_value, "full_name").value,
        "metadata": {
            "age": int(get_attribute(input_value, "age").value),
            "active": bool(get_attribute(input_value, "is_active").value)
        }
    }
    
    return output
```

## Migration from Go's cty

If you're familiar with Go's cty library, here are key differences:

1. **Type Handling**: Python's dynamic typing allows for simpler interfaces
2. **Value Creation**: Explicit `CtyValue` constructor vs. Go's type-specific constructors
3. **Operations**: Function-based vs. method-based in Go
4. **Error Handling**: Python exceptions vs. Go's error returns
5. **Asynchronous Support**: Many operations support async/await

## Conclusion

The `pyvider.cty` module provides a solid foundation for type-safe data manipulation, schema validation, and interoperability with Terraform. By understanding its core concepts and proper usage patterns, you can leverage its capabilities effectively within your Pyvider applications.

For further assistance, refer to the inline documentation in the code or contact the Pyvider team for support.
