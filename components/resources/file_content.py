#!/usr/bin/env python3
# components/resources/file_content.py

import os
from pathlib import Path
from typing import Optional

import attrs

from pyvider.telemetry import logger
from pyvider.exceptions import ResourceError
from pyvider.hub import register_resource, requires_capability
from pyvider.protocols.tfprotov6.protobuf import Schema
from pyvider.resources.base import BaseResource
from pyvider.resources.context import ResourceContext
from pyvider.cty import CtyList, CtyString
from pyvider.schema.pvfactory import a_str, a_bool, s_resource

@attrs.define(frozen=True)
class FileContentConfig:
    """Configuration for file content resource."""
    filename: CtyString = attrs.field()
    content: CtyString = attrs.field()

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "filename": self.filename,
            "content": self.content,
        }

@attrs.define(frozen=True)
class FileContentState:
    """State representation of file content resource."""
    filename: CtyString = attrs.field()
    content: CtyString = attrs.field()
    exists: bool = attrs.field(default=False)
    content_hash: str = attrs.field(init=False)

    def __attrs_post_init__(self):
        """Compute hash after initialization."""
        import hashlib
        hash_value = (hashlib.sha256(self.content.encode()).hexdigest()
                     if self.content else "")
        # Use object.__setattr__ since the class is frozen
        object.__setattr__(self, "content_hash", hash_value)

    def to_dict(self) -> dict:
        """Convert state to dictionary."""
        return {
            "filename": self.filename,
            "content": self.content,
            "exists": self.exists,
            "content_hash": self.content_hash
        }

    @classmethod
    def from_dict(cls, data: dict) -> "FileContentState":
        """Create state from dictionary."""
        return cls(
            filename=data["filename"],
            content=data.get("content", ""),
            exists=data.get("exists", False)
        )

@register_resource("pyvider_file_content")
@requires_capability("fake_cloud")
class FileContentResource(BaseResource["pyvider_file_content", FileContentState, FileContentConfig]):
    """Resource for managing file content."""

    def __init__(self) -> None:
        schema = self.get_schema()
        logger.debug(f"Schema content: {schema}")
        super().__init__(schema)

    @staticmethod
    def get_schema() -> Schema:
        """Create the schema for the file content resource."""
        return s_resource({
            "filename": a_str(required=True, description="The name of the file to manage."),
            "content": a_str(required=True, description="The content to write to the file."),
            "exists": a_bool(computed=True, description="Whether the file exists."),
            "content_hash": a_str(computed=True, description="Hash of the file content.")
        })

    r""" #######################################################################
                        _
                       | |
     _ __ ___  __ _  __| |
    | '__/ _ \/ _` |/ _` |
    | | |  __/ (_| | (_| |
    |_|  \___|\__,_|\__,_|
    """ #read###################################################################
    #@trace_span("file_content_read")
    async def read(
        self, ctx: ResourceContext[FileContentConfig, FileContentState]
    ) -> FileContentState:  # Changed return type to just FileContentState
        """Read the current state of the file."""
        logger.debug(f"📖 Reading file content. Context: {ctx}")

        try:
            # Check if state is a dict and convert it to FileContentState
            if isinstance(ctx.state, dict):
                # Create a FileContentState from the dictionary
                state_dict = ctx.state
                filename = state_dict.get('filename')
                content = state_dict.get('content', '')
            else:
                # Handle missing or None state
                if ctx.state is None:
                    if ctx.config is None:
                        raise ResourceError("Neither state nor config provided")
                    filename = ctx.config.filename
                else:
                    filename = ctx.state.filename

            if not filename:
                raise ResourceError("The state must include a valid filename")

            path = Path(filename)
            exists = path.exists()
            content = path.read_text() if exists else ""

            state = FileContentState(
                filename=filename,
                content=content,
                exists=exists
            )
            logger.debug(f"📖 File read complete. State: {state}")
            return state

        except OSError as e:
            logger.error(f"❌ Error reading file '{filename}': {e}")
            raise ResourceError(f"Failed to read file: {e}") from e
        except Exception as e:
            logger.error(f"❌ Unexpected error reading file: {e}")
            raise ResourceError(f"Unexpected error: {e}") from e

    r""" #######################################################################
           _
          | |
     _ __ | | __ _ _ __
    | '_ \| |/ _` | '_ \
    | |_) | | (_| | | | |
    | .__/|_|\__,_|_| |_|
    | |
    |_|
    """ #plan###################################################################

    #@trace_span("file_content_plan")
    async def plan(
        self, ctx: ResourceContext[FileContentConfig, FileContentState]
    ) -> tuple[FileContentState, CtyList[CtyString]]:
        """Plan changes to the file content."""
        logger.debug(f"📋 Planning changes. Context: {ctx}")
        diagnostics = []

        try:
            if ctx.config is None:
                logger.debug("📋 Delete operation detected")
                return None, []

            # Handle config when it's a dictionary
            if isinstance(ctx.config, dict):
                config_filename = ctx.config.get('filename')
                config_content = ctx.config.get('content', '')
            else:
                config_filename = ctx.config.filename
                config_content = ctx.config.content

            # Handle state when it's a dictionary
            if isinstance(ctx.state, dict):
                current_state = FileContentState(
                    filename=ctx.state.get('filename', ''),
                    content=ctx.state.get('content', ''),
                    exists=ctx.state.get('exists', False)
                )
            else:
                current_state = ctx.state or FileContentState(
                    filename=config_filename,
                    content="",
                    exists=False
                )

            # Create planned state
            planned_state = FileContentState(
                filename=config_filename,
                content=config_content,
                exists=True
            )

            logger.debug(f"📋 Plan complete. State: {planned_state}")
            return planned_state, []

        except Exception as e:
            logger.error(f"❌ Error during planning: {e}")
            diagnostics.append(f"Planning failed: {e}")
            return None, diagnostics

    r""" #######################################################################
                       _
                      | |
      __ _ _ __  _ __ | |_   _
     / _` | '_ \| '_ \| | | | |
    | (_| | |_) | |_) | | |_| |
     \__,_| .__/| .__/|_|\__, |
          | |   | |       __/ |
          |_|   |_|      |___/
    """ #apply##################################################################
    #@trace_span("file_content_apply")
    async def apply(
        self, ctx: ResourceContext[FileContentConfig, FileContentState]
    ) -> tuple[Optional[FileContentState], CtyList[CtyString]]:
        """Apply changes to the file content."""
        logger.debug(f"🚀 📝 ✅ Entering apply() with context: {ctx}")
        operation = None
        diagnostics = []

        try:
            # Handle delete operation
            # Handle delete operation by delegating to delete method
            if ctx.planned_state is None:
                logger.debug("🚀 🗑️ ✅ Delete operation detected, delegating to delete()")
                await self.delete(ctx)
                return None, []

            # Get filename and content from planned_state
            if isinstance(ctx.planned_state, dict):
                filename = ctx.planned_state.get('filename')
                content = ctx.planned_state.get('content', '')
            else:
                filename = ctx.planned_state.filename
                content = ctx.planned_state.content

            path = Path(filename)

            # Ensure parent directory exists
            try:
                logger.debug(f"🚀 📁 🔄 Ensuring parent directory exists: {path.parent}")
                path.parent.mkdir(parents=True, exist_ok=True)
            except OSError as e:
                error_msg = f"Failed to create directory {path.parent}: {e}"
                logger.error(f"🚀 📁 ❌ {error_msg}")
                raise ResourceError(error_msg) from e

            # Determine operation type
            operation = "update" if path.exists() else "create"
            logger.debug(f"🚀 📝 ✅ Operation type determined: {operation}")

            # Check file permissions if updating
            if operation == "update" and not os.access(path, os.W_OK):
                error_msg = f"No write permission for file: {path}"
                logger.error(f"🚀 🔒 ❌ {error_msg}")
                raise ResourceError(error_msg)

            # Write content with detailed error handling
            try:
                logger.debug(f"🚀 📝 🔄 Writing content to file: {path}")
                path.write_text(content)
            except OSError as e:
                error_msg = f"Failed to write file {path}: {e}"
                logger.error(f"🚀 📝 ❌ {error_msg}")
                raise ResourceError(error_msg) from e
            except UnicodeEncodeError as e:
                error_msg = f"Failed to encode content for file {path}: {e}"
                logger.error(f"🚀 📝 ❌ {error_msg}")
                raise ResourceError(error_msg) from e

            # Verify the write was successful
            try:
                logger.debug(f"🚀 ✅ 🔄 Verifying file content: {path}")
                actual_content = path.read_text()
                if actual_content != content:
                    error_msg = "File content verification failed"
                    logger.error(f"🚀 ✅ ❌ {error_msg}")
                    raise ResourceError(error_msg)
            except OSError as e:
                error_msg = f"Failed to verify file content {path}: {e}"
                logger.error(f"🚀 ✅ ❌ {error_msg}")
                raise ResourceError(error_msg) from e

            # Create new state
            new_state = FileContentState(
                filename=filename,
                content=content,
                exists=True
            )

            logger.debug(f"🚀 ✅ ✅ {operation.title()} operation completed successfully")
            return new_state, []

        except ResourceError:
            # Re-raise ResourceError for expected error conditions
            raise
        except Exception as e:
            # Handle unexpected errors
            operation_msg = f" during {operation} operation" if operation else ""
            error_msg = f"Unexpected error{operation_msg}: {e}"
            logger.error(f"🚀 ❌ ❌ {error_msg}", exc_info=True)
            diagnostics.append(error_msg)
            return None, diagnostics
        finally:
            logger.debug("🚀 🔚 ✅ Exiting apply()")

    r""" #######################################################################
         _      _      _
        | |    | |    | |
      __| | ___| | ___| |_ ___
     / _` |/ _ \ |/ _ \ __/ _ \
    | (_| |  __/ |  __/ ||  __/
     \__,_|\___|_|\___|\__\___|
    """ #delete#################################################################

    #@trace_span("file_content_delete")  # Apply the span decorator to the delete method
    async def delete(
        self, ctx: ResourceContext[FileContentConfig, FileContentState]
    ) -> None:
        """Delete the file if it exists."""
        logger.debug(f"🗑️ Entering delete() with context: {ctx}")

        try:
            # Handle ctx.state as dictionary if needed
            if isinstance(ctx.state, dict):
                filename = ctx.state.get('filename', '')
            else:
                filename = ctx.state.filename if ctx.state else ''

            if not filename:
                logger.debug("🗑️⚠️ No filename found in state, nothing to delete")
                return

            path = Path(filename)
            if path.exists():
                logger.debug(f"🗑️🔄 Attempting to delete: {path}")
                path.unlink()
                logger.debug(f"🗑️✅ File {path} deleted successfully")
            else:
                logger.debug(f"🗑️⚠️ File {path} does not exist, no deletion needed")

        except OSError as e:
            logger.error(f"🗑️❌ Error deleting file: {e}")
            raise ResourceError(f"Failed to delete file: {e}")

    r""" #######################################################################
                _ _     _       _
               | (_)   | |     | |
    __   ____ _| |_  __| | __ _| |_ ___
    \ \ / / _` | | |/ _` |/ _` | __/ _ \
     \ V / (_| | | | (_| | (_| | ||  __/
      \_/ \__,_|_|_|\__,_|\__,_|\__\___|
    """ #validate###############################################################

    #@trace_span("file_content_validate")
    async def validate(self, config: FileContentConfig) -> None:
        """Validate resource configuration."""
        if Path(config.filename).is_dir():
            logger.error(f"Cannot write content to a directory: {ctx.config.filename}: {e}")
            raise ResourceError("Cannot write content to a directory")

    #@trace_span("file_content__validate_filename")
    def _validate_filename(self, filename: CtyString) -> None:
        """Validate filename for invalid characters."""
        invalid_chars = '\0\n\r\t\f\v:*?"<>|'
        if any(char in filename for char in invalid_chars):
            logger.error("Invalid characters in filename")
            raise ResourceError("Invalid characters in filename")

    #@trace_span("file_content_validate_config")
    async def validate_config(self, config: FileContentConfig) -> None:
        """Validate resource configuration."""
        logger.debug(f"Validating config: {config}")

        if not config.filename:
            logger.error("The 'filename' field cannot be empty")
            raise ResourceError("The 'filename' field cannot be empty")

        self._validate_filename(config.filename)
        path = Path(config.filename)

        # Check for absolute path
        if not path.is_absolute():
            logger.error("File path must be an absolute path")
            raise ResourceError("File path must be an absolute path")

        # Check if path exists and is a directory
        if path.exists() and path.is_dir():
            logger.error(f"Path exists and is a directory: {path}")
            raise ResourceError("Path exists and is a directory")

        # Validate parent directory access
        if not path.parent.exists():
            logger.error(f"Parent directory does not exist: {path.parent}")
            raise ResourceError(f"Parent directory does not exist: {path.parent}")

        # Check directory writeability
        if not os.access(path.parent, os.W_OK):
            logger.error("Parent directory is not writable")
            raise ResourceError("Parent directory is not writable")

        try:
            await super().validate_config(config)
        except Exception as e:
            logger.error(f"Configuration validation failed: {e}")
            raise ResourceError(f"Configuration validation failed: {e}") from e

        logger.debug("Validation passed for config: %s", config)

    @classmethod
    #@trace_span("file_content_from_dict")  # Apply the span decorator to the from_dict method
    def from_dict(cls, data: dict) -> "FileContentResource":
        """
        Create a resource instance from a dictionary.

        Args:
            data: Dictionary containing resource configuration

        Returns:
            FileContentResource: New resource instance
        """
        instance = cls()
        instance.validate_config(data)
        return instance

    #@trace_span("file_content_to_dict")  # Apply the span decorator to the to_dict method
    def to_dict(self) -> dict:
        """
        Convert resource to dictionary representation.

        Returns:
            dict: Dictionary representation of resource
        """
        return {
            "schema": self._schema,
            "type": "file_content",
        }

################################################################################