"""HubSpot client over the official ``hubspot-api-client`` SDK.

The SDK is synchronous (urllib3). ``HubSpotDataSource`` runs its calls off the
event loop; use the SDK object from ``get_client()`` directly only from a thread.

Reference: https://github.com/HubSpot/hubspot-api-python
"""

import logging
from typing import Any

from hubspot import HubSpot
from pydantic import BaseModel
from urllib3.util.retry import Retry

from app.config.configuration_service import ConfigurationService
from app.sources.client.iclient import IClient

# 5xx on reads only. A 429 must reach the caller so it can back off across calls and
# stop on HubSpot's daily limit instead of parking a worker thread in urllib3; urllib3
# retries any 429 carrying Retry-After unless respect_retry_after_header is off. A
# retried POST could create an object twice. raise_on_status=False lets the last 5xx
# surface as the SDK's ApiException, which keeps the status and body.
_TRANSIENT_RETRY = Retry(
    total=3,
    backoff_factor=1.0,
    status_forcelist=(500, 502, 503, 504),
    allowed_methods=frozenset({"GET", "HEAD"}),
    respect_retry_after_header=False,
    raise_on_status=False,
)


class HubSpotResponse(BaseModel):
    """Result of one HubSpot call.

    On failure, ``error`` reads ``"<category>: <message> (HTTP <status>)"`` and the
    fields beside it let the caller branch: ``category`` is HubSpot's error category
    (``RATE_LIMITS``, ``MISSING_SCOPES``, ``OBJECT_NOT_FOUND``...), ``policy_name``
    names the rate limit that was hit (``DAILY`` will not clear by retrying).
    """

    success: bool
    data: Any = None
    error: str | None = None
    status_code: int | None = None
    category: str | None = None
    policy_name: str | None = None
    retry_after: float | None = None


class HubSpotClient(IClient):
    """Holds one authenticated HubSpot SDK instance.

    OAuth access tokens and service keys are both sent as a Bearer token, so
    one SDK instance serves both auth types.
    """

    def __init__(self, sdk: HubSpot, *, auth_type: str = "API_TOKEN", refresh_token: str | None = None) -> None:
        self._sdk = sdk
        self.auth_type = auth_type
        # Kept so a 401 can refresh with the token this client was built from (02-low-level-design.md).
        self.refresh_token = refresh_token

    def get_client(self) -> HubSpot:
        return self._sdk

    @classmethod
    def build_with_token(
        cls, access_token: str, *, auth_type: str = "API_TOKEN", refresh_token: str | None = None
    ) -> "HubSpotClient":
        if not access_token:
            raise ValueError("HubSpot access token is empty")
        sdk = HubSpot(access_token=access_token, retry=_TRANSIENT_RETRY)
        return cls(sdk, auth_type=auth_type, refresh_token=refresh_token)

    @classmethod
    async def build_from_services(
        cls,
        logger: logging.Logger,
        config_service: ConfigurationService,
        connector_instance_id: str,
    ) -> "HubSpotClient":
        """Build from the connector instance's stored config.

        Call it again before each batch of work: OAuth tokens are refreshed in the
        background and written back to config, so a client built once goes stale.
        """
        config = await config_service.get_config(
            f"/services/connectors/{connector_instance_id}/config", raise_on_error=True
        )
        if not config:
            raise ValueError(f"HubSpot connector {connector_instance_id} has no configuration")

        auth_config = config.get("auth") or {}
        auth_type = auth_config.get("authType", "OAUTH")
        refresh_token = None
        if auth_type == "OAUTH":
            credentials = config.get("credentials") or {}
            token, refresh_token = credentials.get("access_token"), credentials.get("refresh_token")
        elif auth_type == "API_TOKEN":
            token = auth_config.get("apiToken")
        else:
            raise ValueError(f"HubSpot connector {connector_instance_id} has unsupported auth type {auth_type}")

        if not token:
            logger.error("HubSpot connector %s has no %s token in its configuration", connector_instance_id, auth_type)
            raise ValueError(f"HubSpot connector {connector_instance_id} is not authenticated")
        return cls.build_with_token(token, auth_type=auth_type, refresh_token=refresh_token)
