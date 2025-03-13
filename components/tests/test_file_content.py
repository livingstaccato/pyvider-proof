
# components/resources/test_file_content.py

import asyncio
import hashlib
import shutil

from pathlib import Path

import pytest

from components.resources.file_content import (
    FileContentConfig,
    FileContentResource,
    FileContentState,
)
from pyvider.telemetry import logger
from pyvider.exceptions import (
    ResourceError,
)
from pyvider.resources.context import ResourceContext


@pytest.fixture
def temp_file() -> Path:
    """Create a temporary file path in /tmp."""
    path = Path("/tmp/pyvider-test.txt")
    # Ensure clean state
    if path.exists():
        path.chmod(0o644)  # Make writable before deletion
        path.unlink()
    yield path
    # Cleanup
    if path.exists():
        path.chmod(0o644)  # Make writable before deletion
        path.unlink()


@pytest.fixture
def temp_dir() -> Path:
    """Create a temporary directory in /tmp."""
    path = Path("/tmp/pyvider-test-dir")
    path.mkdir(exist_ok=True)
    yield path
    # Cleanup - use rmtree to remove directory and contents
    if path.exists():
        shutil.rmtree(path, ignore_errors=True)

@pytest.fixture
def resource() -> FileContentResource:
    """Create a FileContentResource instance."""
    return FileContentResource()

@pytest.fixture
def config(temp_file) -> FileContentConfig:
    """Create a basic FileContentConfig."""
    return FileContentConfig(
        filename=str(temp_file),
        content="test content"
    )

@pytest.fixture
def context(config) -> ResourceContext[FileContentConfig, FileContentState]:
    """Create a ResourceContext."""
    return ResourceContext(config=config)

@pytest.mark.asyncio
async def test_create_file(resource, context, temp_file):
    """Test creating a new file."""
    # Ensure file doesn't exist
    if temp_file.exists():
        temp_file.unlink()

    # Plan creation
    planned_state, _ = await resource.plan(context)
    assert planned_state.exists is True
    assert planned_state.content == "test content"
    assert not temp_file.exists()

    # Apply creation
    context = ResourceContext(
        config=context.config,
        planned_state=planned_state
    )
    new_state, _ = await resource.apply(context)

    assert temp_file.exists()
    assert temp_file.read_text() == "test content"
    assert new_state.exists is True
    assert new_state.content == "test content"

@pytest.mark.asyncio
async def test_update_file(resource, context, temp_file):
    """Test updating an existing file."""
    # Create initial file
    temp_file.write_text("old content")

    # Read current state - should get just the state object, not a tuple
    current_state = await resource.read(context)
    assert current_state.exists
    assert current_state.content == "old content"

    # Plan update - returns (state, diagnostics)
    planned_state, diagnostics = await resource.plan(context)
    assert not diagnostics
    assert planned_state.exists
    assert planned_state.content == "test content"

    # Apply update - returns (state, diagnostics)
    new_context = ResourceContext(
        config=context.config,
        state=current_state,
        planned_state=planned_state
    )
    new_state, apply_diags = await resource.apply(new_context)
    assert not apply_diags
    assert new_state.exists
    assert new_state.content == "test content"
    assert temp_file.read_text() == "test content"

@pytest.mark.asyncio
async def test_file_permissions(resource, temp_file):
    """Test handling of file permission issues."""
    # Create file with read-only permissions
    temp_file.write_text("initial content")
    temp_file.chmod(0o444)  # Read-only

    config = FileContentConfig(
        filename=str(temp_file),
        content="new content"
    )
    context = ResourceContext(config=config)

    # Plan change
    planned_state, _ = await resource.plan(context)
    
    # Apply should fail due to permissions
    with pytest.raises(ResourceError, match="Failed to write file:.*Permission denied"):
        await resource.apply(ResourceContext(
            config=config,
            planned_state=planned_state
        ))

@pytest.mark.asyncio
async def test_symlink_handling(resource, temp_dir):
    """Test handling of symlinked files."""
    original = temp_dir / "original.txt"
    symlink = temp_dir / "link.txt"

    try:
        # Create initial content
        original.write_text("original")
        symlink.symlink_to(original)

        config = FileContentConfig(
            filename=str(symlink),
            content="new content"
        )
        context = ResourceContext(config=config)

        # Should read through symlink to original file
        curr_state = await resource.read(context)
        assert curr_state.exists
        assert curr_state.content == "original"

        # Plan change
        planned_state, _ = await resource.plan(context)
        assert planned_state.exists
        assert planned_state.content == "new content"

        # Apply change - should update through symlink
        new_state, _ = await resource.apply(ResourceContext(
            config=config,
            planned_state=planned_state
        ))

        assert new_state.exists
        assert new_state.content == "new content"
        assert original.read_text() == "new content"
        assert symlink.read_text() == "new content"

    finally:
        # Cleanup
        if symlink.exists():
            symlink.unlink()
        if original.exists():
            original.unlink()

@pytest.mark.asyncio
async def test_delete_file(resource, context, temp_file):
    """Test deleting a file."""
    # Create initial file
    temp_file.write_text("test content")

    # Delete file
    await resource.delete(context)
    assert not temp_file.exists()

@pytest.mark.asyncio
async def test_directory_validation(resource, temp_dir):
    # In test_file_content.py, within the test_directory_validation test case
    config = {
        "filename": "path/to/directory",  # Example directory path
        "content": "some content"
    }

    config_obj = FileContentConfig(**config)  # Create FileContentConfig object

    with pytest.raises(ResourceError):
        await resource.validate_config(config_obj)  # Pass the config object

@pytest.mark.asyncio
async def test_content_hash_calculation(resource):
    """Test content hash calculation."""
    content = "test content"
    expected_hash = hashlib.sha256(content.encode()).hexdigest()

    config = FileContentConfig(
        filename="/tmp/pyvider-hash-test.txt",
        content=content
    )
    context = ResourceContext(config=config)

    planned_state, _ = await resource.plan(context)
    assert planned_state.content_hash == expected_hash

@pytest.mark.asyncio
async def test_create_empty_content_file(resource, temp_file):
    """Test creating a file with empty content."""
    config = FileContentConfig(filename=str(temp_file), content="")
    context = ResourceContext(config=config)

    planned_state, _ = await resource.plan(context)
    assert planned_state.exists is True
    assert planned_state.content == ""

    context = ResourceContext(config=config, planned_state=planned_state)
    new_state, _ = await resource.apply(context)
    assert temp_file.exists()
    assert temp_file.read_text() == ""
    assert new_state.content == ""

@pytest.mark.asyncio
async def test_large_file_content(resource, temp_file):
    """Test handling of large file content."""
    large_content = "x" * 10**6  # 1 MB content
    config = FileContentConfig(filename=str(temp_file), content=large_content)
    context = ResourceContext(config=config)

    planned_state, _ = await resource.plan(context)
    assert planned_state.exists is True

    context = ResourceContext(config=config, planned_state=planned_state)
    new_state, _ = await resource.apply(context)
    assert temp_file.exists()
    assert temp_file.read_text() == large_content
    assert new_state.content == large_content

@pytest.mark.asyncio
async def test_concurrent_file_access(resource, temp_file):
    """Test concurrent access to the same file."""
    if temp_file.exists():
        temp_file.unlink()

    expected_contents = [f"content_{i}" for i in range(3)]
    configs = [
        FileContentConfig(filename=str(temp_file), content=content)
        for content in expected_contents
    ]

    async def apply_config(config):
        context = ResourceContext(config=config)
        planned_state, _ = await resource.plan(context)
        return await resource.apply(ResourceContext(
            config=config,
            planned_state=planned_state
        ))

    results = await asyncio.gather(*[apply_config(c) for c in configs])
    final_content = temp_file.read_text()

    # Debug output
    logger.debug(f"Expected one of: {expected_contents}")
    logger.debug(f"Got: {final_content}")

    assert final_content in expected_contents, \
        f"Final content '{final_content}' not in expected: {expected_contents}"

@pytest.mark.asyncio
async def test_special_character_filename(resource):
    """Test creating a file with a filename containing special characters."""
    special_filename = "/tmp/pyvider-@#$%^&*()-test.txt"
    config = FileContentConfig(filename=special_filename, content="special content")
    context = ResourceContext(config=config)

    planned_state, _ = await resource.plan(context)
    assert planned_state.exists is True

    context = ResourceContext(config=config, planned_state=planned_state)
    new_state, _ = await resource.apply(context)
    path = Path(special_filename)
    assert path.exists()
    assert path.read_text() == "special content"
    assert new_state.content == "special content"

@pytest.mark.asyncio
async def test_file_permissions(resource, temp_file):
    """Test handling of file permission issues."""
    # Create file with read-only permissions
    temp_file.write_text("initial content")
    temp_file.chmod(0o444)  # Read-only

    config = FileContentConfig(
        filename=str(temp_file),
        content="new content"
    )
    context = ResourceContext(config=config)

    # First we need to plan the change
    planned_state, _ = await resource.plan(context)
    context = ResourceContext(config=config, planned_state=planned_state)

    with pytest.raises(ResourceError, match="No write permission"):  # Match actual error message
        await resource.apply(context)

@pytest.mark.asyncio
async def test_content_encoding(resource, temp_file):
    """Test handling of different content encodings."""
    # Ensure file is writable if it exists
    if temp_file.exists():
        temp_file.chmod(0o644)
        temp_file.unlink()

    special_content = "Hello 世界 🌍"
    config = FileContentConfig(
        filename=str(temp_file),
        content=special_content
    )
    context = ResourceContext(config=config)

    planned_state, _ = await resource.plan(context)
    await resource.apply(ResourceContext(
        config=config,
        planned_state=planned_state
    ))

    assert temp_file.read_text(encoding='utf-8') == special_content

@pytest.mark.asyncio
async def test_zero_byte_file(resource, temp_file):
    """Test handling of empty content."""
    config = FileContentConfig(
        filename=str(temp_file),
        content=""
    )
    context = ResourceContext(config=config)

    planned_state, _ = await resource.plan(context)
    state, _ = await resource.apply(ResourceContext(
        config=config,
        planned_state=planned_state
    ))

    assert state.exists
    assert state.content == ""
    assert temp_file.stat().st_size == 0

@pytest.mark.asyncio
async def test_state_transitions(resource, temp_file):
    """Test complete lifecycle state transitions."""
    # Create
    config1 = FileContentConfig(filename=str(temp_file), content="initial")
    ctx = ResourceContext(config=config1)
    planned_state, _ = await resource.plan(ctx)
    state, _ = await resource.apply(ResourceContext(
        config=config1,
        planned_state=planned_state
    ))
    assert state.exists
    assert state.content == "initial"

    # Update
    config2 = FileContentConfig(filename=str(temp_file), content="updated")
    ctx = ResourceContext(config=config2, state=state)
    planned_state, _ = await resource.plan(ctx)
    state, _ = await resource.apply(ResourceContext(
        config=config2,
        state=state,
        planned_state=planned_state
    ))
    assert state.content == "updated"

    # Delete - fix this part
    ctx = ResourceContext(
        config=config2,  # Keep the last config for filename reference
        state=state
    )
    await resource.delete(ctx)
    assert not temp_file.exists()

@pytest.mark.asyncio
async def test_large_file_handling(resource, temp_file):
    """Test handling of large file content."""
    large_content = "x" * (1024 * 1024)  # 1MB of data
    config = FileContentConfig(
        filename=str(temp_file),
        content=large_content
    )
    context = ResourceContext(config=config)

    planned_state, _ = await resource.plan(context)
    state, _ = await resource.apply(ResourceContext(
        config=config,
        planned_state=planned_state
    ))

    assert state.content == large_content
    assert temp_file.stat().st_size == len(large_content)

# In test_file_content.py
@pytest.mark.asyncio
async def test_relative_path_handling(resource):
    """Test handling of relative paths."""
    relative_paths = [
        "./relative/path/file.txt",
        "relative/path/file.txt",
        "../outside/path/file.txt",
        "file.txt"
    ]

    for path in relative_paths:
        config = FileContentConfig(filename=path, content="content")
        with pytest.raises(
            ResourceError,
            match="File path must be an absolute path"
        ) as exc_info:
            await resource.validate_config(config)
        assert str(exc_info.value) == "File path must be an absolute path"

@pytest.mark.asyncio
async def test_invalid_file_path_characters(resource):
    """Test handling of invalid characters in file paths."""
    invalid_paths = [
        "/tmp/test\0file.txt",  # Null byte
        "/tmp/test\nfile.txt",  # Newline
        "/tmp/test:file.txt",   # Windows invalid char
        "/tmp/test*file.txt",   # Invalid wildcard
    ]

    for path in invalid_paths:
        config = FileContentConfig(filename=path, content="test")
        with pytest.raises(ResourceError, match="Invalid characters in filename"):
            await resource.validate_config(config)

@pytest.mark.asyncio
async def test_symlink_handling(resource, temp_dir):
    """Test handling of symlinked files."""
    original = temp_dir / "original.txt"
    symlink = temp_dir / "link.txt"

    try:
        original.write_text("original")
        symlink.symlink_to(original)

        config = FileContentConfig(
            filename=str(symlink),
            content="new content"
        )

        context = ResourceContext(config=config)
        planned_state, _ = await resource.plan(context)
        await resource.apply(ResourceContext(
            config=config,
            planned_state=planned_state
        ))

        assert original.read_text() == "new content"
    finally:
        # Cleanup
        if symlink.exists() and symlink.is_symlink():
            symlink.unlink()
        if original.exists():
            original.unlink()


@pytest.mark.asyncio
async def test_content_hash_stability(resource, temp_file):
    """Test that content hashes are stable for identical content."""
    content = "test content"
    config1 = FileContentConfig(filename=str(temp_file), content=content)
    config2 = FileContentConfig(filename=str(temp_file), content=content)

    state1, _ = await resource.plan(ResourceContext(config=config1))
    state2, _ = await resource.plan(ResourceContext(config=config2))

    assert state1.content_hash == state2.content_hash

#!/usr/bin/env python3
# components/resources/test_file_content.py

import pytest
from pathlib import Path
import os

# Add these new test cases:

@pytest.mark.asyncio
async def test_validate_directory_parent_access(resource):
    """Test validation of parent directory access."""
    nonexistent_path = "/nonexistent/directory/file.txt"
    config = FileContentConfig(filename=nonexistent_path, content="test")
    
    with pytest.raises(ResourceError, match="Parent directory does not exist"):
        await resource.validate_config(config)

@pytest.mark.asyncio
async def test_read_with_no_context(resource):
    """Test read operation with empty context."""
    context = ResourceContext(config=None, state=None)
    
    with pytest.raises(ResourceError, match="Neither state nor config provided"):
        await resource.read(context)

@pytest.mark.asyncio
async def test_read_nonexistent_file(resource, temp_file):
    """Test reading a file that doesn't exist."""
    config = FileContentConfig(filename=str(temp_file), content="")
    context = ResourceContext(config=config)
    
    state, _ = await resource.read(context)
    assert not state.exists
    assert state.content == ""

@pytest.mark.asyncio
async def test_plan_with_none_config(resource):
    """Test plan operation with None config."""
    context = ResourceContext(config=None)
    state, diagnostics = await resource.plan(context)
    
    assert state is None
    assert not diagnostics

@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_apply_error_handling(resource, temp_dir):
    """Test apply operation error handling."""
    test_file = temp_dir / "test.txt"

    # Make directory read-only
    original_mode = temp_dir.stat().st_mode
    try:
        temp_dir.chmod(0o555)
        
        config = FileContentConfig(filename=str(test_file), content="test")
        context = ResourceContext(
            config=config,
            planned_state=FileContentState(
                filename=str(test_file),
                content="test",
                exists=True
            )
        )
        
        state, diagnostics = await resource.apply(context)
        assert state is None
        assert any("Permission denied" in d for d in diagnostics)
    
    finally:
        # Restore original permissions
        temp_dir.chmod(original_mode)

@pytest.mark.asyncio
async def test_apply_with_readonly_directory(resource, temp_dir):
    """Test apply operation with read-only directory."""
    test_file = temp_dir / "test.txt"
    
    # Create file first
    test_file.write_text("initial content")
    
    # Make directory read-only
    original_mode = temp_dir.stat().st_mode
    try:
        temp_dir.chmod(0o555)
        
        config = FileContentConfig(filename=str(test_file), content="test")
        context = ResourceContext(
            config=config,
            planned_state=FileContentState(
                filename=str(test_file),
                content="test",
                exists=True
            )
        )
        
        with pytest.raises(ResourceError, match="Permission denied"):
            await resource.apply(context)
    
    finally:
        # Restore permissions
        temp_dir.chmod(original_mode)
        if test_file.exists():
            test_file.unlink()

@pytest.mark.asyncio
async def test_delete_nonexistent_file(resource, temp_file):
    """Test deleting a file that doesn't exist."""
    config = FileContentConfig(filename=str(temp_file), content="")
    context = ResourceContext(config=config)
    
    # Should not raise an error
    await resource.delete(context)

@pytest.mark.asyncio
async def test_validate_empty_filename(resource):
    """Test validation with empty filename."""
    config = FileContentConfig(filename="", content="test")
    
    with pytest.raises(ResourceError, match="filename.*empty"):
        await resource.validate_config(config)

@pytest.mark.asyncio
async def test_apply_with_readonly_directory(resource, temp_dir):
    """Test apply operation with read-only directory."""
    temp_file = temp_dir / "test.txt"
    temp_dir.chmod(0o555)  # Make directory read-only
    
    try:
        config = FileContentConfig(filename=str(temp_file), content="test")
        context = ResourceContext(
            config=config,
            planned_state=FileContentState(
                filename=str(temp_file),
                content="test",
                exists=True
            )
        )
        
        state, diagnostics = await resource.apply(context)
        assert state is None
        assert len(diagnostics) > 0
    
    finally:
        temp_dir.chmod(0o755)  # Restore permissions

@pytest.mark.asyncio
async def test_validate_path_exists_as_directory(resource, temp_dir):
    """Test validation when path exists as directory."""
    config = FileContentConfig(filename=str(temp_dir), content="test")
    
    with pytest.raises(ResourceError, match="exists and is a directory"):
        await resource.validate_config(config)

@pytest.mark.asyncio
async def test_schema_conversion(resource):
    """Test schema conversion functions."""
    schema = resource.get_schema()
    assert schema.version == 1
    assert len(schema.block.attributes) > 0
    
    # Test schema attributes
    attribute_names = {attr.name for attr in schema.block.attributes}
    assert "filename" in attribute_names
    assert "content" in attribute_names

@pytest.mark.asyncio
async def test_to_dict_conversion(resource):
    """Test dictionary conversion methods."""
    config = FileContentConfig(filename="/test/path", content="test")
    state = FileContentState(filename="/test/path", content="test", exists=True)
    
    config_dict = config.to_dict()
    assert config_dict["filename"] == "/test/path"
    assert config_dict["content"] == "test"
    
    state_dict = state.to_dict()
    assert state_dict["filename"] == "/test/path"
    assert state_dict["content"] == "test"
    assert state_dict["exists"] is True
