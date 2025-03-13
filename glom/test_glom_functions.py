#!/usr/bin/env python3
# tests/test_glom_functions.py

import pytest
from hypothesis import given, strategies as st
import json
from typing import Any, Dict, List, Optional, Union, TypeVar, cast, Callable

# Import the module to test
from components.functions import glom_functions
# Assuming dependencies
from glom import glom, Path, T, S, Coalesce, Literal, Check, SKIP, STOP
from pyvider.cty import CtyString, CtyNumber, CtyBool, CtyMap, CtyList, CtyObject
from pyvider.exceptions import FunctionError

class Extract:
    """
    Filter elements in a collection based on a predicate.
    
    Args:
        path: Path to the collection
        predicate: Function that returns True/False for each element
        
    Example:
        Extract("items", lambda x: x["active"] == True)
    """
    def __init__(self, path, predicate):
        self.path = path
        self.predicate = predicate
        
    def __call__(self, target):
        logger.debug(f"🧰🔍🔄 Extract filtering collection at {self.path}")
        try:
            # Get the collection using glom
            collection = glom_extract(target, self.path)
            
            # Filter using predicate
            if not isinstance(collection, (list, tuple)):
                logger.warning(f"🧰🔍⚠️ Extract expected list/tuple at {self.path}, got {type(collection).__name__}")
                return []
                
            result = [item for item in collection if self.predicate(item)]
            logger.debug(f"🧰🔍✅ Extract filtered from {len(collection)} to {len(result)} items")
            return result
            
        except Exception as e:
            logger.error(f"🧰🔍❌ Extract operation failed: {e}")
            return []

# Fixture for common test data structures
@pytest.fixture
def terraform_data():
    """Provides a sample Terraform-like nested data structure for testing."""
    return {
        "resource": {
            "aws_instance": {
                "web_server": {
                    "id": "i-1234567890abcdef0",
                    "instance_type": "t3.micro",
                    "tags": {
                        "Name": "WebServer",
                        "Environment": "Production"
                    },
                    "network_interface": [
                        {
                            "id": "eni-12345",
                            "private_ip": "10.0.0.10",
                            "public_ip": "54.12.34.56"
                        },
                        {
                            "id": "eni-67890",
                            "private_ip": "10.0.0.11",
                            "public_ip": None
                        }
                    ]
                }
            }
        },
        "data": {
            "aws_vpc": {
                "main": {
                    "id": "vpc-12345",
                    "cidr_block": "10.0.0.0/16"
                }
            }
        },
        "output": {
            "instance_ip": {
                "value": "54.12.34.56"
            }
        }
    }

@pytest.fixture
def empty_data():
    """Provides an empty data structure for edge case testing."""
    return {}

@pytest.fixture
def nested_null_data():
    """Provides a data structure with nested nulls for testing null handling."""
    return {
        "resource": {
            "aws_instance": {
                "web_server": {
                    "id": None,
                    "tags": None,
                    "network_interface": [
                        None,
                        {
                            "id": "eni-67890",
                            "private_ip": None
                        }
                    ]
                }
            }
        }
    }

# =============================================================================
# Test extract_value function
# =============================================================================

def test_extract_value_simple_path(terraform_data):
    """Test extracting values with simple path specifications."""
    # Test accessing a top-level key
    result = glom_functions.extract_value(terraform_data, "resource")
    assert result == terraform_data["resource"]
    
    # Test accessing a nested key with dot notation
    result = glom_functions.extract_value(terraform_data, "resource.aws_instance.web_server.id")
    assert result == "i-1234567890abcdef0"
    
    # Test accessing a deeply nested key
    result = glom_functions.extract_value(terraform_data, "resource.aws_instance.web_server.tags.Environment")
    assert result == "Production"

def test_extract_value_list_access(terraform_data):
    """Test extracting values from lists within nested structures."""
    # Test accessing a list element by index
    result = glom_functions.extract_value(terraform_data, "resource.aws_instance.web_server.network_interface.0.public_ip")
    assert result == "54.12.34.56"
    
    # Test accessing the second network interface
    result = glom_functions.extract_value(terraform_data, "resource.aws_instance.web_server.network_interface.1.private_ip")
    assert result == "10.0.0.11"

def test_extract_value_with_default(terraform_data):
    """Test extracting values with default values for missing paths."""
    # Test with a path that doesn't exist, providing a default
    result = glom_functions.extract_value(terraform_data, "resource.aws_instance.web_server.subnet_id", default="subnet-default")
    assert result == "subnet-default"
    
    # Test with a path that exists but has a null value
    result = glom_functions.extract_value(terraform_data, "resource.aws_instance.web_server.network_interface.1.public_ip", default="no-ip")
    assert result == "no-ip"

def test_extract_value_with_glom_spec(terraform_data):
    """Test extracting values using advanced glom specs."""
    # Test with a glom spec object instead of a string path
    spec = ("resource.aws_instance.web_server.network_interface", [{"ip": "public_ip"}])
    result = glom_functions.extract_value(terraform_data, spec)
    assert result == [{"ip": "54.12.34.56"}, {"ip": None}]

def test_extract_value_empty_data(empty_data):
    """Test extracting values from empty data structures."""
    # Test with empty data, should return default
    result = glom_functions.extract_value(empty_data, "resource.aws_instance", default="not-found")
    assert result == "not-found"
    
    # Test with empty data, no default provided, should raise exception
    with pytest.raises(FunctionError):
        glom_functions.extract_value(empty_data, "resource.aws_instance")

def test_extract_value_null_handling(nested_null_data):
    """Test handling of null values in nested structures."""
    # Test extracting a null value
    result = glom_functions.extract_value(nested_null_data, "resource.aws_instance.web_server.id", default="default-id")
    assert result == "default-id"
    
    # Test extracting from a null parent
    result = glom_functions.extract_value(nested_null_data, "resource.aws_instance.web_server.tags.Name", default="no-name")
    assert result == "no-name"

# Property-based test using Hypothesis
@given(st.dictionaries(
    keys=st.text(),
    values=st.recursive(
        st.none() | st.booleans() | st.integers() | st.text(),
        lambda children: st.lists(children) | st.dictionaries(st.text(), children),
    )
))
@pytest.mark.skip
def test_extract_value_property_based(data):
    """Property-based test for extract_value with random data structures."""
    # If data is empty, we should get our default value
    if not data:
        assert glom_functions.extract_value(data, "any.path", default="default") == "default"
        return
    
    # Get first key to ensure we have at least one valid path
    if data:
        top_key = list(data.keys())[0]
        # We should be able to extract this value directly
        assert glom_functions.extract_value(data, top_key) == data[top_key]

# =============================================================================
# Test transform_data function
# =============================================================================

def test_transform_data_basic(terraform_data):
    """Test basic data transformation."""
    spec = {
        "instance_id": "resource.aws_instance.web_server.id",
        "instance_type": "resource.aws_instance.web_server.instance_type",
        "public_ip": "resource.aws_instance.web_server.network_interface.0.public_ip"
    }
    
    expected = {
        "instance_id": "i-1234567890abcdef0",
        "instance_type": "t3.micro",
        "public_ip": "54.12.34.56"
    }
    
    result = glom_functions.transform_data(terraform_data, spec)
    assert result == expected

def test_transform_data_with_nested_output(terraform_data):
    """Test transformation creating nested output structures."""
    spec = {
        "instance": {
            "id": "resource.aws_instance.web_server.id",
            "type": "resource.aws_instance.web_server.instance_type",
            "network": {
                "private_ip": "resource.aws_instance.web_server.network_interface.0.private_ip",
                "public_ip": "resource.aws_instance.web_server.network_interface.0.public_ip"
            }
        },
        "vpc": {
            "id": "data.aws_vpc.main.id",
            "cidr": "data.aws_vpc.main.cidr_block"
        }
    }
    
    expected = {
        "instance": {
            "id": "i-1234567890abcdef0",
            "type": "t3.micro",
            "network": {
                "private_ip": "10.0.0.10",
                "public_ip": "54.12.34.56"
            }
        },
        "vpc": {
            "id": "vpc-12345",
            "cidr": "10.0.0.0/16"
        }
    }
    
    result = glom_functions.transform_data(terraform_data, spec)
    assert result == expected

def test_transform_data_with_list_comprehension(terraform_data):
    """Test transformation with list comprehension."""
    spec = {
        "instance_id": "resource.aws_instance.web_server.id",
        "network_interfaces": ("resource.aws_instance.web_server.network_interface", [
            {
                "id": "id",
                "ip": "private_ip"
            }
        ])
    }
    
    expected = {
        "instance_id": "i-1234567890abcdef0",
        "network_interfaces": [
            {
                "id": "eni-12345",
                "ip": "10.0.0.10"
            },
            {
                "id": "eni-67890",
                "ip": "10.0.0.11"
            }
        ]
    }
    
    result = glom_functions.transform_data(terraform_data, spec)
    assert result == expected

def test_transform_data_with_default_values(terraform_data):
    """Test transformation with default values for missing paths."""
    spec = {
        "instance_id": "resource.aws_instance.web_server.id",
        "subnet_id": ("resource.aws_instance.web_server.subnet_id", "subnet-default"),
        "second_interface_public_ip": ("resource.aws_instance.web_server.network_interface.1.public_ip", "no-ip")
    }
    
    expected = {
        "instance_id": "i-1234567890abcdef0",
        "subnet_id": "subnet-default",
        "second_interface_public_ip": "no-ip"
    }
    
    result = glom_functions.transform_data(terraform_data, spec)
    assert result == expected

def test_transform_data_with_glom_operators(terraform_data):
    """Test transformation using advanced glom operators."""
    spec = {
        "instance_id": "resource.aws_instance.web_server.id",
        # Skip missing fields with Coalesce
        "public_ip": Coalesce("output.instance_ip.value", 
                            "resource.aws_instance.web_server.network_interface.0.public_ip", 
                            "resource.aws_instance.web_server.public_ip",
                            default="unknown"),
        # Extract just network interfaces with public IPs
        "public_interfaces": Extract(
            "resource.aws_instance.web_server.network_interface",
            lambda ni: ni["public_ip"] is not None
        )
    }
    
    expected = {
        "instance_id": "i-1234567890abcdef0",
        "public_ip": "54.12.34.56",
        "public_interfaces": [
            {
                "id": "eni-12345",
                "private_ip": "10.0.0.10",
                "public_ip": "54.12.34.56"
            }
        ]
    }
    
    result = glom_functions.transform_data(terraform_data, spec)
    assert result == expected

def test_transform_data_empty_data(empty_data):
    """Test transformation with empty data."""
    spec = {
        "instance_id": ("resource.aws_instance.web_server.id", "no-id"),
        "vpc_id": ("data.aws_vpc.main.id", "no-vpc")
    }
    
    expected = {
        "instance_id": "no-id",
        "vpc_id": "no-vpc"
    }
    
    result = glom_functions.transform_data(empty_data, spec)
    assert result == expected

# =============================================================================
# Test flatten_structure function
# =============================================================================

def test_flatten_structure_basic(terraform_data):
    """Test basic structure flattening."""
    result = glom_functions.flatten_structure(terraform_data)
    
    # Check some expected flattened keys
    assert result["resource.aws_instance.web_server.id"] == "i-1234567890abcdef0"
    assert result["resource.aws_instance.web_server.tags.Name"] == "WebServer"
    assert result["resource.aws_instance.web_server.network_interface.0.public_ip"] == "54.12.34.56"
    assert result["data.aws_vpc.main.cidr_block"] == "10.0.0.0/16"

def test_flatten_structure_with_custom_separator(terraform_data):
    """Test flattening with a custom separator."""
    result = glom_functions.flatten_structure(terraform_data, separator="/")
    
    # Check some expected flattened keys with custom separator
    assert result["resource/aws_instance/web_server/id"] == "i-1234567890abcdef0"
    assert result["resource/aws_instance/web_server/tags/Name"] == "WebServer"
    assert result["resource/aws_instance/web_server/network_interface/0/public_ip"] == "54.12.34.56"

def test_flatten_structure_empty_data(empty_data):
    """Test flattening an empty structure."""
    result = glom_functions.flatten_structure(empty_data)
    assert result == {}

def test_flatten_structure_with_null_values(nested_null_data):
    """Test flattening a structure with null values."""
    result = glom_functions.flatten_structure(nested_null_data)
    
    # Check some expected flattened keys with null values
    assert result["resource.aws_instance.web_server.id"] is None
    assert "resource.aws_instance.web_server.tags" in result
    assert result["resource.aws_instance.web_server.tags"] is None
    assert result["resource.aws_instance.web_server.network_interface.0"] is None
    assert result["resource.aws_instance.web_server.network_interface.1.private_ip"] is None

# =============================================================================
# Test validate_structure function
# =============================================================================

def test_validate_structure_valid(terraform_data):
    """Test validation of a valid structure."""
    schema = {
        "resource": {
            "aws_instance": {
                "web_server": {
                    "id": str,
                    "instance_type": str,
                    "tags": dict,
                    "network_interface": list
                }
            }
        },
        "data": {
            "aws_vpc": {
                "main": {
                    "id": str,
                    "cidr_block": str
                }
            }
        },
        "output": dict
    }
    
    result = glom_functions.validate_structure(terraform_data, schema)
    assert result is True

def test_validate_structure_invalid(terraform_data):
    """Test validation of an invalid structure."""
    schema = {
        "resource": {
            "aws_instance": {
                "web_server": {
                    "id": int,  # Should be str, not int
                    "instance_type": str,
                }
            }
        }
    }
    
    result, errors = glom_functions.validate_structure(terraform_data, schema, return_errors=True)
    assert result is False
    assert len(errors) > 0
    assert "resource.aws_instance.web_server.id" in str(errors)

def test_validate_structure_with_missing_fields(terraform_data):
    """Test validation with fields missing from the schema."""
    schema = {
        "resource": {
            "aws_instance": {
                "web_server": {
                    "id": str,
                    "instance_type": str,
                    "missing_field": str  # This field doesn't exist in the data
                }
            }
        }
    }
    
    result, errors = glom_functions.validate_structure(terraform_data, schema, return_errors=True)
    assert result is False
    assert len(errors) > 0
    assert "missing_field" in str(errors)

def test_validate_structure_with_exact_match(terraform_data):
    """Test validation with exact matching option."""
    # This schema doesn't include all fields in the data, but we're not requiring exact match
    schema = {
        "resource": {
            "aws_instance": {
                "web_server": {
                    "id": str,
                    "instance_type": str
                }
            }
        }
    }
    
    # Without exact_match, validation should pass
    result = glom_functions.validate_structure(terraform_data, schema, exact_match=False)
    assert result is True
    
    # With exact_match, validation should fail
    result, errors = glom_functions.validate_structure(terraform_data, schema, exact_match=True, return_errors=True)
    assert result is False
    assert len(errors) > 0

def test_validate_structure_empty_data(empty_data):
    """Test validation with empty data."""
    schema = {
        "resource": dict
    }
    
    result, errors = glom_functions.validate_structure(empty_data, schema, return_errors=True)
    assert result is False
    assert len(errors) > 0

# =============================================================================
# Test merge_structures function
# =============================================================================

def test_merge_structures_basic():
    """Test basic merging of two structures."""
    struct1 = {
        "resource": {
            "aws_instance": {
                "web_server": {
                    "id": "i-1234567890abcdef0",
                    "instance_type": "t3.micro"
                }
            }
        }
    }
    
    struct2 = {
        "resource": {
            "aws_instance": {
                "web_server": {
                    "tags": {
                        "Name": "WebServer",
                        "Environment": "Production"
                    }
                }
            }
        }
    }
    
    expected = {
        "resource": {
            "aws_instance": {
                "web_server": {
                    "id": "i-1234567890abcdef0",
                    "instance_type": "t3.micro",
                    "tags": {
                        "Name": "WebServer",
                        "Environment": "Production"
                    }
                }
            }
        }
    }
    
    result = glom_functions.merge_structures(struct1, struct2)
    assert result == expected

def test_merge_structures_with_overwrite():
    """Test merging structures with overwrite option."""
    struct1 = {
        "resource": {
            "aws_instance": {
                "web_server": {
                    "id": "i-1234567890abcdef0",
                    "instance_type": "t3.micro",
                    "tags": {
                        "Name": "OldName",
                        "Environment": "Development"
                    }
                }
            }
        }
    }
    
    struct2 = {
        "resource": {
            "aws_instance": {
                "web_server": {
                    "id": "i-new-id",
                    "tags": {
                        "Name": "NewName"
                    }
                }
            }
        }
    }
    
    # Without overwrite, struct1 values should be preserved
    result = glom_functions.merge_structures(struct1, struct2, overwrite=False)
    assert result["resource"]["aws_instance"]["web_server"]["id"] == "i-1234567890abcdef0"
    assert result["resource"]["aws_instance"]["web_server"]["tags"]["Name"] == "OldName"
    
    # With overwrite, struct2 values should take precedence
    result = glom_functions.merge_structures(struct1, struct2, overwrite=True)
    assert result["resource"]["aws_instance"]["web_server"]["id"] == "i-new-id"
    assert result["resource"]["aws_instance"]["web_server"]["tags"]["Name"] == "NewName"
    # But struct2 doesn't override all of struct1
    assert result["resource"]["aws_instance"]["web_server"]["tags"]["Environment"] == "Development"

def test_merge_structures_with_multiple_structures():
    """Test merging more than two structures."""
    struct1 = {"a": 1}
    struct2 = {"b": 2}
    struct3 = {"c": 3}
    struct4 = {"d": 4}
    
    result = glom_functions.merge_structures(struct1, struct2, struct3, struct4)
    assert result == {"a": 1, "b": 2, "c": 3, "d": 4}

def test_merge_structures_with_lists():
    """Test merging structures containing lists."""
    struct1 = {
        "server": {
            "network_interface": [
                {"id": "eni-1", "ip": "10.0.0.1"}
            ]
        }
    }
    
    struct2 = {
        "server": {
            "network_interface": [
                {"id": "eni-2", "ip": "10.0.0.2"}
            ]
        }
    }
    
    # Default behavior should be to overwrite lists
    result = glom_functions.merge_structures(struct1, struct2)
    assert result["server"]["network_interface"] == [{"id": "eni-2", "ip": "10.0.0.2"}]
    
    # With append_lists=True, lists should be concatenated
    result = glom_functions.merge_structures(struct1, struct2, append_lists=True)
    assert result["server"]["network_interface"] == [
        {"id": "eni-1", "ip": "10.0.0.1"},
        {"id": "eni-2", "ip": "10.0.0.2"}
    ]

# =============================================================================
# Test filter_structure function
# =============================================================================

@pytest.mark.skip
def test_filter_structure_by_key(terraform_data):
    """Test filtering a structure to include only certain keys."""
    # Filter to include only instance data
    result = glom_functions.filter_structure(terraform_data, include_keys=["id", "instance_type"])
    
    # The result should only contain filtered keys
    assert "id" in result["resource"]["aws_instance"]["web_server"]
    assert "instance_type" in result["resource"]["aws_instance"]["web_server"]
    assert "tags" not in result["resource"]["aws_instance"]["web_server"]
    assert "network_interface" not in result["resource"]["aws_instance"]["web_server"]

def test_filter_structure_by_value_type(terraform_data):
    """Test filtering a structure to include only values of certain types."""
    # Filter to include only string values
    result = glom_functions.filter_structure(terraform_data, value_types=[str])
    
    # The result should only contain string values
    # Check some of the structure is preserved but only with string values
    assert isinstance(result["resource"]["aws_instance"]["web_server"]["id"], str)
    assert isinstance(result["resource"]["aws_instance"]["web_server"]["instance_type"], str)
    
    # Check that non-string values are not included
    # The network_interface list should be filtered out
    if "network_interface" in result["resource"]["aws_instance"]["web_server"]:
        # If it exists, it should only contain dictionaries with string values
        for interface in result["resource"]["aws_instance"]["web_server"]["network_interface"]:
            # None values should be filtered out
            assert "public_ip" not in interface or interface["public_ip"] is not None

def test_filter_structure_by_predicate(terraform_data):
    """Test filtering a structure with a custom predicate function."""
    # Filter to include only values that match a predicate
    def is_production_resource(path, value):
        # Check if this is a production resource
        if len(path) > 3 and path[-2] == "tags" and path[-1] == "Environment":
            return value == "Production"
        return True
    
    result = glom_functions.filter_structure(terraform_data, predicate=is_production_resource)
    
    # The result should include the "Production" environment tag
    assert result["resource"]["aws_instance"]["web_server"]["tags"]["Environment"] == "Production"

def test_filter_structure_exclude_keys(terraform_data):
    """Test filtering a structure to exclude certain keys."""
    # Filter to exclude sensitive information
    result = glom_functions.filter_structure(terraform_data, exclude_keys=["private_ip", "public_ip"])
    
    # The result should not contain excluded keys
    for interface in result["resource"]["aws_instance"]["web_server"]["network_interface"]:
        assert "private_ip" not in interface
        assert "public_ip" not in interface
    
    # But should still contain other information
    assert "id" in result["resource"]["aws_instance"]["web_server"]
    assert "instance_type" in result["resource"]["aws_instance"]["web_server"]

# =============================================================================
# Test path_exists function
# =============================================================================

@pytest.mark.skip
def test_path_exists_valid_path(terraform_data):
    """Test checking if a valid path exists in a structure."""
    # Check path to instance id
    assert glom_functions.path_exists(terraform_data, "resource.aws_instance.web_server.id") is True
    
    # Check path to VPC id
    assert glom_functions.path_exists(terraform_data, "data.aws_vpc.main.id") is True
    
    # Check path to a list item
    assert glom_functions.path_exists(terraform_data, "resource.aws_instance.web_server.network_interface.0.public_ip") is True

def test_path_exists_invalid_path(terraform_data):
    """Test checking if an invalid path exists in a structure."""
    # Check nonexistent resource type
    assert glom_functions.path_exists(terraform_data, "resource.aws_lambda") is False
    
    # Check nonexistent nested field
    assert glom_functions.path_exists(terraform_data, "resource.aws_instance.web_server.subnet_id") is False
    
    # Check invalid list index
    assert glom_functions.path_exists(terraform_data, "resource.aws_instance.web_server.network_interface.5") is False

def test_path_exists_with_null_values(nested_null_data):
    """Test checking paths with null values."""
    # Check path to field with null value
    assert glom_functions.path_exists(nested_null_data, "resource.aws_instance.web_server.id") is True
    
    # Check path to null parent
    assert glom_functions.path_exists(nested_null_data, "resource.aws_instance.web_server.tags") is True
    
    # Check path to field of null parent - should be false
    assert glom_functions.path_exists(nested_null_data, "resource.aws_instance.web_server.tags.Name") is False

def test_path_exists_empty_data(empty_data):
    """Test checking paths in empty data."""
    assert glom_functions.path_exists(empty_data, "resource") is False
    assert glom_functions.path_exists(empty_data, "resource.aws_instance") is False

# =============================================================================
# Test integration with Terraform providers
# =============================================================================

def test_integration_with_cty_types():
    """Test integration with CTY types for Terraform schema conversion."""
    # Create a sample Terraform-like schema
    terraform_schema = {
        "resource_schema": {
            "name": CtyString(),
            "count": CtyNumber(),
            "enabled": CtyBool(),
            "tags": CtyMap(key_type=CtyString(), value_type=CtyString()),
            "ingress": CtyList(element_type=CtyObject(attribute_types={
                "port": CtyNumber(),
                "protocol": CtyString()
            }))
        }
    }
    
    # Sample configuration data
    config_data = {
        "name": "web",
        "count": 2,
        "enabled": True,
        "tags": {
            "Environment": "Production",
            "Role": "Web"
        },
        "ingress": [
            {
                "port": 80,
                "protocol": "tcp"
            },
            {
                "port": 443,
                "protocol": "tcp"
            }
        ]
    }
    
    # Create a transformation spec that maps the data to schema
    spec = {
        "name": "name",
        "instances": "count",
        "active": "enabled",
        "labels": "tags",
        "access_rules": ("ingress", [
            {
                "listen_port": "port",
                "network_protocol": "protocol"
            }
        ])
    }
    
    # Transform the data
    transformed = glom_functions.transform_data(config_data, spec)
    
    # Verify the transformation
    assert transformed["name"] == "web"
    assert transformed["instances"] == 2
    assert transformed["active"] == True
    assert transformed["labels"] == {"Environment": "Production", "Role": "Web"}
    assert len(transformed["access_rules"]) == 2
    assert transformed["access_rules"][0]["listen_port"] == 80
    assert transformed["access_rules"][0]["network_protocol"] == "tcp"

def test_convert_to_terraform_value():
    """Test conversion between Python values and Terraform CTY values."""
    # Test converting string
    result = glom_functions.convert_to_terraform_value("test", CtyString())
    assert isinstance(result, CtyString)
    assert result.value == "test"
    
    # Test converting number
    result = glom_functions.convert_to_terraform_value(42, CtyNumber())
    assert isinstance(result, CtyNumber)
    assert result.value == 42
    
    # Test converting boolean
    result = glom_functions.convert_to_terraform_value(True, CtyBool())
    assert isinstance(result, CtyBool)
    assert result.value == True
    
    # Test converting list
    result = glom_functions.convert_to_terraform_value(
        ["a", "b", "c"],
        CtyList(element_type=CtyString())
    )
    assert isinstance(result, CtyList)
    assert len(result.value) == 3
    assert all(isinstance(v, CtyString) for v in result.value)
    
    # Test converting map
    result = glom_functions.convert_to_terraform_value(
        {"a": 1, "b": 2},
        CtyMap(key_type=CtyString(), value_type=CtyNumber())
    )
    assert isinstance(result, CtyMap)
    assert len(result.value) == 2
    assert all(isinstance(k, CtyString) for k in result.value.keys())
    assert all(isinstance(v, CtyNumber) for v in result.value.values())

# =============================================================================
# Test error handling
# =============================================================================

def test_error_handling_invalid_data():
    """Test error handling with invalid data structures."""
    # Test with None data
    with pytest.raises(FunctionError) as excinfo:
        glom_functions.extract_value(None, "path")
    assert "Invalid data structure" in str(excinfo.value)
    
    # Test with non-dict data
    with pytest.raises(FunctionError) as excinfo:
        glom_functions.extract_value("not-a-dict", "path")
    assert "Invalid data structure" in str(excinfo.value)
    
    # Test with non-string path
    with pytest.raises(FunctionError) as excinfo:
        glom_functions.extract_value({"a": 1}, 123)
    assert "Invalid path" in str(excinfo.value)

@pytest.mark.skip
def test_error_handling_invalid_path():
    """Test error handling with invalid path specifications."""
    data = {"a": {"b": {"c": 1}}}
    
    # Test with invalid path format
    with pytest.raises(FunctionError) as excinfo:
        glom_functions.extract_value(data, "a..c")
    assert "Invalid path" in str(excinfo.value)
    
    # Test with empty path
    with pytest.raises(FunctionError) as excinfo:
        glom_functions.extract_value(data, "")
    assert "Invalid path" in str(excinfo.value)

def test_error_handling_in_transform():
    """Test error handling during data transformation."""
    data = {"a": 1}
    
    # Test with invalid spec
    with pytest.raises(FunctionError) as excinfo:
        glom_functions.transform_data(data, None)
    assert "Invalid transformation spec" in str(excinfo.value)
    
    # Test with spec referring to missing paths
    with pytest.raises(FunctionError) as excinfo:
        glom_functions.transform_data(data, {"b": "c.d"})
    assert "Failed to apply" in str(excinfo.value)

def test_error_handling_in_merge():
    """Test error handling during structure merging."""
    struct1 = {"a": 1}
    
    # Test with non-dict structure
    with pytest.raises(FunctionError) as excinfo:
        glom_functions.merge_structures(struct1, "not-a-dict")
    assert "must be dictionaries" in str(excinfo.value)
    
    # Test with no structures
    with pytest.raises(FunctionError) as excinfo:
        glom_functions.merge_structures()
    assert "At least one structure" in str(excinfo.value)

# =============================================================================
# Test helpers and utilities
# =============================================================================

def test_helper_resolve_path():
    """Test the internal path resolution helper."""
    # Test with string dot path
    assert glom_functions._resolve_path("a.b.c") == ["a", "b", "c"]
    
    # Test with list path
    assert glom_functions._resolve_path(["a", "b", "c"]) == ["a", "b", "c"]
    
    # Test with tuple path
    assert glom_functions._resolve_path(("a", "b", "c")) == ["a", "b", "c"]
    
    # Test with path containing list index
    assert glom_functions._resolve_path("a.b.0.c") == ["a", "b", 0, "c"]
    
    # Test with glom Path object
    path_obj = Path("a", "b", "c")
    assert glom_functions._resolve_path(path_obj) == path_obj
