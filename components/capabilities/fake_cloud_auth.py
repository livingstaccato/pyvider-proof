import json
from typing import List

from attrs import define

from pyvider.telemetry import logger
from pyvider.core.base_types import ConfigType
from pyvider.core.capabilities.auth import AuthCapability
from pyvider.protocols.tfprotov6.protobuf import Schema, StringKind

logger.debug("Importing FakeCloudAuthCapability...")

fake_cloud_auth_schema = Schema(
    version=1,
    block=Schema.Block(
        attributes=[
            Schema.Attribute(
                name="cloud_token",
                type=json.dumps("string").encode('utf-8'),
                description="Token specific to FakeCloud for authentication.",
                required=True,
                optional=False,
                computed=False,
                sensitive=True,
                description_kind=StringKind.PLAIN,
            ),
        ],
        description="Handles FakeCloud-specific authentication and token management.",
        description_kind=StringKind.PLAIN,
    ),
)

@define
class FakeCloudAuthCapability(AuthCapability):
    """
    A capability that extends AuthCapability to handle FakeCloud-specific authentication.
    """

    #@trace_span("fake_cloud_auth_get_schema")
    def get_schema(self) -> Schema:
        base_schema = super().get_schema()
        base_schema.block.attributes.extend(fake_cloud_auth_schema.block.attributes)
        return base_schema

    #@trace_span("fake_cloud_auth_validate")
    async def validate(self, config: ConfigType) -> List[str]:
        diagnostics = await super().validate(config)
        if not config.get("cloud_token"):
            diagnostics.append("'cloud_token' is required for FakeCloud authentication.")
        return diagnostics

    def is_configured(self, config: ConfigType) -> bool:
        return super().is_configured(config) and "cloud_token" in config

    #@trace_span("fake_cloud_auth_initialize")
    async def initialize(self, config: ConfigType) -> None:
        logger.info("Initializing FakeCloudAuthCapability with config.")
        await super().initialize(config)

