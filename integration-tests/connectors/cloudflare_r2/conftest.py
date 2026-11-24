# pyright: ignore-file

"""Cloudflare R2 connector fixtures.

The suite syncs from a pre-provisioned bucket in a real R2 account. The token
must list buckets, and read, write and delete objects in the test bucket: the
connector itself only reads, but the suite uploads and removes its own data.
"""

import os
from collections.abc import AsyncGenerator
from typing import Any

import pytest
import pytest_asyncio
from connector_lifecycle import RESOURCE_NAME, constructor, destructor
from connectors.cloudflare_r2.r2_storage_helper import R2StorageHelper, r2_endpoint
from helper.graph_provider import GraphProviderProtocol
from helper.source_credentials import source_unavailable
from pipeshub_client import PipeshubClient  # type: ignore[import-not-found]

CONNECTOR_TYPE = "Cloudflare R2"
REQUIRED = ["CLOUDFLARE_R2_TEST_ACCOUNT_ID", "CLOUDFLARE_R2_TEST_ACCESS_KEY_ID", "CLOUDFLARE_R2_TEST_SECRET_ACCESS_KEY"]


def r2_bucket() -> str:
    return os.getenv("CLOUDFLARE_R2_TEST_BUCKET") or RESOURCE_NAME


def r2_connector_config() -> dict[str, Any]:
    """The connector has no bucket setting: the run folder filter limits the sync to the suite's own objects."""
    auth: dict[str, Any] = {
        "accountId": os.getenv("CLOUDFLARE_R2_TEST_ACCOUNT_ID"),
        "accessKey": os.getenv("CLOUDFLARE_R2_TEST_ACCESS_KEY_ID"),
        "secretKey": os.getenv("CLOUDFLARE_R2_TEST_SECRET_ACCESS_KEY"),
    }
    jurisdiction = os.getenv("CLOUDFLARE_R2_TEST_JURISDICTION")
    if jurisdiction:
        auth["jurisdiction"] = jurisdiction
    return {"auth": auth}


@pytest.fixture(scope="session")
def r2_storage() -> R2StorageHelper:
    if any(not os.getenv(name) for name in REQUIRED):
        source_unavailable("The Cloudflare R2 account this suite syncs from is not configured.", secrets=REQUIRED)
    return R2StorageHelper(
        access_key=os.environ["CLOUDFLARE_R2_TEST_ACCESS_KEY_ID"],
        secret_key=os.environ["CLOUDFLARE_R2_TEST_SECRET_ACCESS_KEY"],
        endpoint_url=r2_endpoint(
            os.environ["CLOUDFLARE_R2_TEST_ACCOUNT_ID"], os.getenv("CLOUDFLARE_R2_TEST_JURISDICTION")
        ),
    )


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def r2_connector(
    r2_storage: R2StorageHelper,
    pipeshub_client: PipeshubClient,
    graph_provider: GraphProviderProtocol,
    sample_data_root,
) -> AsyncGenerator[dict[str, Any], None]:
    state = await constructor(
        r2_storage,
        pipeshub_client,
        graph_provider,
        sample_data_root,
        storage_name="R2 bucket",
        connector_type=CONNECTOR_TYPE,
        connector_config=r2_connector_config(),
        resource_name_override=r2_bucket(),
    )
    state["bucket_name"] = state["resource_name"]
    yield state
    await destructor(r2_storage, pipeshub_client, graph_provider, state, connector_type=CONNECTOR_TYPE)
