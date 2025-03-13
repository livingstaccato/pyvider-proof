#!/usr/bin/env python3
# components/data_sources/file_info.py

import attrs
import os
import datetime
import stat
from pathlib import Path
from typing import Optional, List

from pyvider.hub.decorators import register_data_source
from pyvider.telemetry import logger
from pyvider.exceptions import DataSourceError
from pyvider.resources.context import ResourceContext
from pyvider.schema.pvfactory import (
    a_str, a_num, a_bool, a_map, s_data_source
)

@attrs.define(frozen=True)
class FileInfoConfig:
    """Configuration for file info data source."""
    path: str = attrs.field()

    def to_dict(self) -> dict:
        return {"path": self.path}

@attrs.define(frozen=True)
class FileInfoState:
    """State representation of file info data source with extended metadata."""
    path: str = attrs.field()
    exists: bool = attrs.field(default=False)
    size: int = attrs.field(default=0)
    is_dir: bool = attrs.field(default=False)
    is_file: bool = attrs.field(default=False)
    is_symlink: bool = attrs.field(default=False)
    modified_time: str = attrs.field(default="")
    access_time: str = attrs.field(default="")
    creation_time: str = attrs.field(default="")
    permissions: str = attrs.field(default="")
    owner: str = attrs.field(default="")
    group: str = attrs.field(default="")
    mime_type: str = attrs.field(default="")

    def to_dict(self) -> dict:
        return {
            "path": self.path,
            "exists": self.exists,
            "size": self.size,
            "is_dir": self.is_dir,
            "is_file": self.is_file,
            "is_symlink": self.is_symlink,
            "modified_time": self.modified_time,
            "access_time": self.access_time,
            "creation_time": self.creation_time,
            "permissions": self.permissions,
            "owner": self.owner,
            "group": self.group,
            "mime_type": self.mime_type
        }

@register_data_source("pyvider_file_info")
class FileInfoDataSource:
    """Data source for retrieving comprehensive file information without managing it."""

    @staticmethod
    def get_schema():
        """Create the schema for the file info data source with extended metadata."""
        return s_data_source({
            # Input parameters
            "path": a_str(required=True, description="Path to the file or directory to inspect"),
            
            # Basic properties (computed)
            "exists": a_bool(computed=True, description="Whether the path exists"),
            "size": a_num(computed=True, description="Size of the file in bytes"),
            "is_dir": a_bool(computed=True, description="Whether the path is a directory"),
            "is_file": a_bool(computed=True, description="Whether the path is a regular file"),
            "is_symlink": a_bool(computed=True, description="Whether the path is a symbolic link"),
            
            # Time properties (computed)
            "modified_time": a_str(computed=True, description="Last modification time (ISO 8601)"),
            "access_time": a_str(computed=True, description="Last access time (ISO 8601)"),
            "creation_time": a_str(computed=True, description="Creation time (ISO 8601)"),
            
            # Permissions and ownership (computed)
            "permissions": a_str(computed=True, description="File permissions in octal notation"),
            "owner": a_str(computed=True, description="Owner username"),
            "group": a_str(computed=True, description="Group name"),
            
            # Content info (computed)
            "mime_type": a_str(computed=True, description="MIME type if detectable")
        })

    async def read(self, ctx: ResourceContext) -> FileInfoState:
        """Read comprehensive file information."""
        logger.debug(f"📡📖✅ Reading file info. Context: {ctx}")

        try:
            # Extract path from config
            if isinstance(ctx.config, dict):
                file_path = ctx.config.get('path', '')
            else:
                file_path = ctx.config.path

            logger.debug(f"📡📖🔍 Reading file info for path: {file_path}")

            if not file_path:
                logger.error("📡📖❌ Path is empty")
                raise DataSourceError("Path cannot be empty")

            path = Path(file_path)
            
            # Basic existence check - return early if not found
            if not path.exists():
                logger.debug(f"📡📖⚠️ Path does not exist: {path}")
                return FileInfoState(path=str(path), exists=False)

            # Initialize variables for file metadata
            is_dir = path.is_dir()
            is_file = path.is_file()
            is_symlink = path.is_symlink()
            size = path.stat().st_size if not is_dir else sum(f.stat().st_size for f in path.glob('**/*') if f.is_file())
            
            # Get stat info
            stat_info = path.stat()
            
            # Format times as ISO 8601
            modified_time = datetime.datetime.fromtimestamp(stat_info.st_mtime).isoformat()
            access_time = datetime.datetime.fromtimestamp(stat_info.st_atime).isoformat()
            creation_time = datetime.datetime.fromtimestamp(stat_info.st_ctime).isoformat()
            
            # Get permissions in octal format
            permissions = oct(stat_info.st_mode & 0o777)[2:]
            
            # Get owner and group names
            try:
                import pwd
                import grp
                owner = pwd.getpwuid(stat_info.st_uid).pw_name
                group = grp.getgrgid(stat_info.st_gid).gr_name
            except (ImportError, KeyError):
                # Fallback for systems without pwd/grp modules
                owner = str(stat_info.st_uid)
                group = str(stat_info.st_gid)
            
            # Try to determine mime type
            mime_type = ""
            if is_file:
                try:
                    import mimetypes
                    mime_type = mimetypes.guess_type(file_path)[0] or ""
                except ImportError:
                    mime_type = ""

            result = FileInfoState(
                path=str(path),
                exists=True,
                size=size,
                is_dir=is_dir,
                is_file=is_file,
                is_symlink=is_symlink,
                modified_time=modified_time,
                access_time=access_time,
                creation_time=creation_time,
                permissions=permissions,
                owner=owner,
                group=group,
                mime_type=mime_type
            )

            logger.debug(f"📡📖✅ File info read complete: {result}")
            return result

        except Exception as e:
            logger.error(f"📡📖❌ Error reading file info: {e}", exc_info=True)
            raise DataSourceError(f"Failed to read file info: {e}") from e

    async def validate(self, config) -> List[str]:
        """Validate data source configuration."""
        logger.debug(f"📡🔍✅ Validating config: {config}")

        diagnostics = []

        # Extract path from config
        if isinstance(config, dict):
            path = config.get('path', '')
        else:
            path = config.path

        # Check if path is provided
        if not path:
            diagnostics.append("Path cannot be empty")
            logger.error("📡🔍❌ Path cannot be empty")
            return diagnostics

        # Check if path is absolute
        if not Path(path).is_absolute():
            diagnostics.append("Path must be absolute")
            logger.error("📡🔍❌ Path must be absolute")

        # Check for invalid characters in path
        invalid_chars = '\0\n\r\t\f\v'
        if any(c in path for c in invalid_chars):
            diagnostics.append("Path contains invalid characters")
            logger.error("📡🔍❌ Path contains invalid characters")

        logger.debug(f"📡🔍✅ Validation complete: {len(diagnostics)} issues found")
        return diagnostics
