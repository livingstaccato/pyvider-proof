
# components/functions/numeric_functions.py

"""
Numeric Functions

This module implements basic mathematical functions for use in Terraform configurations.
These functions demonstrate how to implement and register numeric Terraform functions in Pyvider.
"""

import math
from decimal import Decimal, InvalidOperation

from pyvider.hub import register_function
from pyvider.telemetry import logger
from pyvider.cty import CtyNumber
from pyvider.exceptions import FunctionError

@register_function(
    name="pyvider_add",
    summary="Add two numbers",
    description="""
    Adds two numbers together.

    Example:
    ```hcl
    output "add_example" {
      value = provider::pyvider_add(5, 3)
    }
    ```
    """,
    param_descriptions={
        "a": "First number",
        "b": "Second number"
    }
)
def add(a: CtyNumber, b: CtyNumber) -> CtyNumber:
    """
    Add two numbers.

    Args:
        a: First number
        b: Second number

    Returns:
        The sum of a and b
    """
    logger.debug(f"🧰📝🔄 Adding numbers: {a} + {b}")
    result = a + b
    logger.debug(f"🧰📝✅ Addition result: {result}")
    return result

@register_function(
    name="pyvider_subtract",
    summary="Subtract two numbers",
    description="""
    Subtracts the second number from the first.

    Example:
    ```hcl
    output "subtract_example" {
      value = provider::pyvider_subtract(5, 3)
    }
    ```
    """,
    param_descriptions={
        "a": "First number",
        "b": "Second number"
    }
)
def subtract(a: CtyNumber, b: CtyNumber) -> CtyNumber:
    """
    Subtract two numbers.

    Args:
        a: First number
        b: Second number

    Returns:
        The result of a - b
    """
    logger.debug(f"🧰📝🔄 Subtracting numbers: {a} - {b}")
    result = a - b
    logger.debug(f"🧰📝✅ Subtraction result: {result}")
    return result

@register_function(
    name="pyvider_multiply",
    summary="Multiply two numbers",
    description="""
    Multiplies two numbers together.

    Example:
    ```hcl
    output "multiply_example" {
      value = provider::pyvider_multiply(5, 3)
    }
    ```
    """,
    param_descriptions={
        "a": "First number",
        "b": "Second number"
    }
)
def multiply(a: CtyNumber, b: CtyNumber) -> CtyNumber:
    """
    Multiply two numbers.

    Args:
        a: First number
        b: Second number

    Returns:
        The product of a and b
    """
    logger.debug(f"🧰📝🔄 Multiplying numbers: {a} * {b}")
    result = a * b
    logger.debug(f"🧰📝✅ Multiplication result: {result}")
    return result

@register_function(
    name="pyvider_divide",
    summary="Divide two numbers",
    description="""
    Divides the first number by the second.

    Example:
    ```hcl
    output "divide_example" {
      value = provider::pyvider_divide(10, 2)
    }
    ```
    """,
    param_descriptions={
        "a": "Numerator",
        "b": "Denominator"
    }
)
def divide(a: CtyNumber, b: CtyNumber) -> CtyNumber:
    """
    Divide two numbers.

    Args:
        a: Numerator
        b: Denominator

    Returns:
        The result of a / b

    Raises:
        FunctionError: If division by zero is attempted
    """
    logger.debug(f"🧰📝🔄 Dividing numbers: {a} / {b}")

    try:
        # Check for division by zero
        if b == 0:
            raise ZeroDivisionError("Division by zero is not allowed")

        result = a / b
        logger.debug(f"🧰📝✅ Division result: {result}")
        return result

    except ZeroDivisionError as e:
        error_msg = str(e)
        logger.error(f"🧰📝❌ {error_msg}")
        raise FunctionError(error_msg)

@register_function(
    name="pyvider_min",
    summary="Find the minimum value in a list",
    description="""
    Returns the minimum value from a list of numbers.

    Example:
    ```hcl
    output "min_example" {
      value = provider::pyvider_min([8, 3, 12, 5, 2])
    }
    ```
    """,
    param_descriptions={
        "numbers": "List of numbers"
    }
)
def min_value(numbers: list) -> CtyNumber:
    """Find the minimum value in a list."""
    logger.debug(f"🧰📝🔄 Finding minimum value in list: {numbers}")

    try:
        # Check for empty list
        if not numbers:
            raise ValueError("Cannot find minimum of an empty list")

        # Fix: Ensure we're working with numbers
        # This ensures proper conversion of Terraform values to Python numbers
        num_list = []
        for n in numbers:
            if isinstance(n, (int, float)):
                num_list.append(n)
            elif isinstance(n, str) and n.replace('.', '', 1).isdigit():
                num_list.append(float(n))
            else:
                logger.error(f"🧰📝❌ Non-numeric value in list: {n}")
                raise ValueError(f"Non-numeric value in list: {n}")

        if not num_list:
            raise ValueError("No valid numeric values in list")

        # Find minimum
        result = min(num_list)
        logger.debug(f"🧰📝✅ Minimum value: {result}")
        return result

    except Exception as e:
        error_msg = f"Error finding minimum: {str(e)}"
        logger.error(f"🧰📝❌ {error_msg}")
        raise FunctionError(error_msg)

@register_function(
    name="pyvider_max",
    summary="Find the maximum value in a list",
    description="""
    Returns the maximum value from a list of numbers.

    Example:
    ```hcl
    output "max_example" {
      value = provider::pyvider_max([5, 3, 8, 1, 7])
    }
    ```
    """,
    param_descriptions={
        "numbers": "List of numbers"
    }
)
def max_value(numbers: list) -> CtyNumber:
    """
    Find the maximum value in a list.

    Args:
        numbers: List of numbers

    Returns:
        The maximum value

    Raises:
        FunctionError: If the list is empty or contains non-numeric values
    """
    logger.debug(f"🧰📝🔄 Finding maximum value in list: {numbers}")

    try:
        # Check for empty list
        if not numbers:
            raise ValueError("Cannot find maximum of an empty list")

        # Convert all values to numbers
        try:
            num_list = [float(n) for n in numbers]
        except (ValueError, TypeError):
            raise ValueError("List contains non-numeric values")

        # Find maximum
        result = max(num_list)
        logger.debug(f"🧰📝✅ Maximum value: {result}")
        return result

    except Exception as e:
        error_msg = f"Error finding maximum: {str(e)}"
        logger.error(f"🧰📝❌ {error_msg}")
        raise FunctionError(error_msg)

@register_function(
    name="pyvider_sum",
    summary="Sum a list of numbers",
    description="""
    Returns the sum of a list of numbers.

    Example:
    ```hcl
    output "sum_example" {
      value = provider::pyvider_sum([5, 3, 8, 1, 7])
    }
    ```
    """,
    param_descriptions={
        "numbers": "List of numbers to sum"
    }
)
def sum_list(numbers: list) -> CtyNumber:
    """
    Sum a list of numbers.

    Args:
        numbers: List of numbers to sum

    Returns:
        The sum of all numbers in the list

    Raises:
        FunctionError: If the list contains non-numeric values
    """
    logger.debug(f"🧰📝🔄 Summing list of numbers: {numbers}")

    try:
        # Handle empty list
        if not numbers:
            return 0

        # Convert all values to numbers
        try:
            num_list = [float(n) for n in numbers]
        except (ValueError, TypeError):
            raise ValueError("List contains non-numeric values")

        # Calculate sum
        result = sum(num_list)
        logger.debug(f"🧰📝✅ Sum result: {result}")
        return result

    except Exception as e:
        error_msg = f"Error calculating sum: {str(e)}"
        logger.error(f"🧰📝❌ {error_msg}")
        raise FunctionError(error_msg)

@register_function(
    name="pyvider_round",
    summary="Round a number to a specified precision",
    description="""
    Rounds a number to the specified number of decimal places.

    Example:
    ```hcl
    output "round_example" {
      value = provider::pyvider_round(3.14159, 2)
    }
    ```
    """,
    param_descriptions={
        "number": "The number to round",
        "precision": "Number of decimal places (default: 0)"
    },
    allow_null=["precision"]
)
def round_number(number: CtyNumber, precision: CtyNumber = None) -> CtyNumber:
    """
    Round a number to a specified precision.

    Args:
        number: The number to round
        precision: Number of decimal places (default: 0)

    Returns:
        The rounded number

    Raises:
        FunctionError: If precision is not a valid integer
    """
    logger.debug(f"🧰📝🔄 Rounding number {number} to precision {precision}")

    try:
        # Default precision to 0 if null
        if precision is None:
            precision = 0

        # Ensure precision is an integer
        if not isinstance(precision, (int, float)) or precision != int(precision):
            raise ValueError("Precision must be an integer")

        precision = int(precision)

        # Round the number
        result = round(float(number), precision)
        logger.debug(f"🧰📝✅ Rounded result: {result}")
        return result

    except Exception as e:
        error_msg = f"Error rounding number: {str(e)}"
        logger.error(f"🧰📝❌ {error_msg}")
        raise FunctionError(error_msg)