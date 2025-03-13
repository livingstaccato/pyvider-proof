
# components/functions/string_manipulation.py

"""
String Manipulation Functions

This module implements basic string manipulation functions for use in Terraform configurations.
These functions demonstrate how to implement and register Terraform functions in Pyvider.
"""

from pyvider.hub import register_function
from pyvider.telemetry import logger
from pyvider.cty import CtyString, CtyList
from pyvider.exceptions import FunctionError

@register_function(
    name="pyvider_upper",
    summary="Convert a string to uppercase",
    description="""
    Converts all characters in a string to uppercase.

    Example:
    ```hcl
    output "upper_example" {
      value = provider::pyvider_upper("hello world")
    }
    ```
    """,
    param_descriptions={"input": "The string to convert to uppercase"}
)
def upper(input: CtyString) -> CtyString:
    """
    Convert a string to uppercase.

    Args:
        input: The string to convert

    Returns:
        The input string converted to uppercase
    """
    logger.debug(f"🧰📝🔄 Converting string to uppercase: {input}")
    if not input:
        return ""
    return input.upper()

@register_function(
    name="pyvider_lower",
    summary="Convert a string to lowercase",
    description="""
    Converts all characters in a string to lowercase.

    Example:
    ```hcl
    output "lower_example" {
      value = provider::pyvider_lower("HELLO WORLD")
    }
    ```
    """,
    param_descriptions={"input": "The string to convert to lowercase"}
)
def lower(input: CtyString) -> CtyString:
    """
    Convert a string to lowercase.

    Args:
        input: The string to convert

    Returns:
        The input string converted to lowercase
    """
    logger.debug(f"🧰📝🔄 Converting string to lowercase: {input}")
    if not input:
        return ""
    return input.lower()

# Fix for components/functions/string_manipulation.py
@register_function(
    name="pyvider_format",
    summary="Format a string with variables",
    description="""
    Formats a string by replacing placeholders with values.
    Uses Python's string formatting with curly braces.

    Example:
    ```hcl
    output "format_example" {
      value = provider::pyvider_format("Hello, {0}!", ["World"])
    }
    ```
    """,
    param_descriptions={
        "template": "String template with {0}, {1}, etc. placeholders",
        "values": "List of values to insert into the template"
    },
    allow_null=["values"]
)
def format_string(template: CtyString, values: list) -> CtyString:
    """Format a string by replacing placeholders with values."""
    logger.debug(f"🧰📝🔄 Formatting string: template='{template}', values={values}")

    try:
        # Handle empty/null values
        if values is None:
            values = []

        # Log actual type information
        logger.debug(f"🧰📝🔍 Values type: {type(values)}")
        if isinstance(values, (list, tuple)):
            for i, v in enumerate(values):
                logger.debug(f"🧰📝🔍 Value {i} type: {type(v)}, value: {v}")

        # Extract actual values from the list
        extracted_values = []
        if isinstance(values, (list, tuple)):
            for v in values:
                # If it's a CTY type with value attribute, extract it
                if hasattr(v, "value") and not isinstance(v, (str, int, float, bool)):
                    extracted_values.append(v.value)
                else:
                    extracted_values.append(v)
        else:
            # Handle single value
            if hasattr(values, "value") and not isinstance(values, (str, int, float, bool)):
                extracted_values = [values.value]
            else:
                extracted_values = [values]

        # Convert all values to strings for safe formatting
        str_values = [str(v) for v in extracted_values]
        
        logger.debug(f"🧰📝🔍 Extracted values for formatting: {extracted_values}")

        # Format the string
        formatted = template.format(*str_values)
        logger.debug(f"🧰📝✅ Formatted result: '{formatted}'")
        return formatted

    except IndexError:
        error_msg = f"Not enough values provided. Template needs {template.count('{')} values, but got {len(values or [])}."
        logger.error(f"🧰📝❌ {error_msg}")
        raise FunctionError(error_msg)
    except Exception as e:
        error_msg = f"String formatting failed: {e}"
        logger.error(f"🧰📝❌ {error_msg}", exc_info=True)
        raise FunctionError(error_msg)

def X1_format_string(template: CtyString, values: list) -> CtyString:
    """
    Format a string by replacing placeholders with values.

    Args:
        template: String template with {0}, {1}, etc. placeholders
        values: List of values to insert into the template

    Returns:
        The formatted string

    Raises:
        FunctionError: If formatting fails due to invalid input
    """
    logger.debug(f"🧰📝🔄 Formatting string: template='{template}', values={values}")

    try:
        # Handle empty/null values
        if values is None:
            values = []

        # Convert all values to strings for safe formatting
        if isinstance(values, list):
            str_values = [str(v) for v in values]
        else:
            # Handle single value
            str_values = [str(values)]

        # Format the string
        formatted = template.format(*str_values)
        logger.debug(f"🧰📝✅ Formatted result: '{formatted}'")
        return formatted

    except IndexError:
        error_msg = f"Not enough values provided. Template needs at least {template.count('{')} values, but got {len(values or [])}."
        logger.error(f"🧰📝❌ {error_msg}")
        raise FunctionError(error_msg)
    except KeyError as e:
        error_msg = f"Missing named parameter: {e}"
        logger.error(f"🧰📝❌ {error_msg}")
        raise FunctionError(error_msg)
    except Exception as e:
        error_msg = f"String formatting failed: {e}"
        logger.error(f"🧰📝❌ {error_msg}", exc_info=True)
        raise FunctionError(error_msg)

@register_function(
    name="pyvider_join",
    summary="Join strings with a delimiter",
    description="""
    Joins a list of strings with the specified delimiter.

    Example:
    ```hcl
    output "join_example" {
      value = provider::pyvider_join("-", ["foo", "bar", "baz"])
    }
    ```
    """,
    param_descriptions={
        "delimiter": "The delimiter to use when joining strings",
        "strings": "List of strings to join"
    },
    allow_null=["strings"]
)
def join(delimiter: CtyString, strings: list) -> CtyString:
    """
    Join strings with a delimiter.

    Args:
        delimiter: The delimiter to use when joining strings
        strings: List of strings to join

    Returns:
        The joined string
    """
    logger.debug(f"🧰📝🔄 Joining strings with delimiter '{delimiter}': {strings}")

    # Handle empty/null values
    if strings is None:
        return ""

    # Convert all values to strings for safe joining
    str_values = [str(s) for s in strings]

    # Join the strings
    joined = delimiter.join(str_values)
    logger.debug(f"🧰📝✅ Joined result: '{joined}'")
    return joined

# In components/functions/string_manipulation.py

@register_function(
    name="pyvider_split",
    summary="Split a string into a list",
    description="""
    Splits a string into a list of substrings using the specified delimiter.

    Example:
    ```hcl
    output "split_example" {
      value = provider::pyvider_split(",", "foo,bar,baz")
    }
    ```
    """,
    param_descriptions={
        "delimiter": "The delimiter to use when splitting the string",
        "string": "The string to split"
    }
)
def split(delimiter: CtyString, string: CtyString) -> list:
    """
    Split a string into a list.

    Args:
        delimiter: The delimiter to use when splitting the string
        string: The string to split

    Returns:
        List of substrings
    """
    logger.debug(f"🧰📝🔄 Splitting string '{string}' with delimiter '{delimiter}'")

    # Handle empty strings
    if not string:
        return []

    # Split the string
    parts = string.split(delimiter)
    logger.debug(f"🧰📝✅ Split result: {parts}")
    return parts


@register_function(
    name="pyvider_replace",
    summary="Replace occurrences of a substring",
    description="""
    Replaces all occurrences of a search string with a replacement string.

    Example:
    ```hcl
    output "replace_example" {
      value = provider::pyvider_replace("Hello, World!", "World", "Terraform")
    }
    ```
    """,
    param_descriptions={
        "string": "The input string",
        "search": "The substring to search for",
        "replacement": "The string to replace with"
    }
)
def replace(string: CtyString, search: CtyString, replacement: CtyString) -> CtyString:
    """
    Replace occurrences of a substring.

    Args:
        string: The input string
        search: The substring to search for
        replacement: The string to replace with

    Returns:
        The string with replacements
    """
    logger.debug(f"🧰📝🔄 Replacing '{search}' with '{replacement}' in '{string}'")

    # Perform the replacement
    result = string.replace(search, replacement)
    logger.debug(f"🧰📝✅ Replacement result: '{result}'")
    return result
