import json
from typing import Any, Dict, List

from pyvider.hub import register_capability

from pyvider.telemetry import logger
from pyvider.core.base_types import ConfigType
from pyvider.core.services import Service

from pyvider.schema import Schema, Block, Attribute, StringKind, SchemaType


# Example schema for the fake cloud capability
fake_cloud_schema = Schema(
    version=1,
    block=Block(
        attributes=[
            Attribute(
                name="cloud_name",
                type=SchemaType.STRING,
                description="Name of the cloud provider",
                required=True,
                optional=False,
                computed=False,
                sensitive=False,
                description_kind=StringKind.PLAIN,
            ),
            Attribute(
                name="api_key",
                type=json.dumps("string").encode('utf-8'),
                description="API Key for accessing the fake cloud",
                required=True,
                optional=False,
                computed=False,
                sensitive=True,
                description_kind=StringKind.PLAIN,
            ),
            Attribute(
                name="region",
                type=json.dumps("string").encode('utf-8'),
                description="Region for deploying resources",
                required=False,
                optional=True,
                computed=False,
                sensitive=False,
                description_kind=StringKind.PLAIN,
            ),
        ],
        description="Fake Cloud capability for simulating cloud operations.",
        description_kind=StringKind.PLAIN,
    ),
)

@register_capability("fake_cloud")
class FakeCloudCapability(Service):
    """
    A mock cloud capability to simulate cloud interactions for testing.
    This can be used by resources or data sources that require cloud-like behavior.
    """

    #@trace_span("fake_cloud_get_schema")
    def get_schema(self) -> Schema:
        return fake_cloud_schema

    #@trace_span("fake_cloud_validate")
    async def validate(self, config: ConfigType) -> List[str]:
        diagnostics = []
        if not config.get("cloud_name"):
            diagnostics.append("'cloud_name' is required.")
        if not config.get("api_key"):
            diagnostics.append("'api_key' is required.")
        return diagnostics

    #@trace_span("fake_cloud_execute")
    async def execute(self, action: str, params: Dict[str, Any]) -> Dict[str, Any]:
        logger.debug(f"Executing {action} on fake cloud with params: {params}")
        return {"status": "success", "action": action, "params": params}
