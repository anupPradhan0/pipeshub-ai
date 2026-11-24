# pyright: ignore-file

"""
Cloudflare R2 Connector – Bucket Names filter integration test
==============================================================

  TC-ACCESS-REVOKE-001 — A bucket taken out of the Bucket Names filter leaves the index:
                         its records and record group go, the other bucket's stay.

Needs a second pre-provisioned bucket in ``CLOUDFLARE_R2_SECOND_TEST_BUCKET``; tests
never create buckets. Without it the module skips.
"""

import logging
import os
import uuid
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any

import pytest
import pytest_asyncio
from connector_lifecycle import (
    create_connector_and_await_sync,
    destructor,
    ensure_resource_exists,
)
from connectors.cloudflare_r2.conftest import (
    CONNECTOR_TYPE,
    r2_bucket,
    r2_connector_config,
)
from connectors.cloudflare_r2.r2_storage_helper import R2StorageHelper
from helper.graph_provider import GraphProviderProtocol
from helper.graph_provider_utils import (
    apply_filter_full_sync,
    wait_until_graph_condition,
)
from helper.run_folder import folder_filter, new_run_folder
from pipeshub_client import PipeshubClient  # type: ignore[import-not-found]

logger = logging.getLogger("cloudflare-r2-bucket-filter-test")

SECOND_BUCKET_ENV = "CLOUDFLARE_R2_SECOND_TEST_BUCKET"


def bucket_filters(folder: str, buckets: list[str]) -> dict:
    filters = folder_filter(folder)
    filters["sync"]["values"]["buckets"] = {"operator": "in", "type": "multiselect", "value": buckets}
    return filters


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def r2_two_bucket_connector(
    r2_storage: R2StorageHelper,
    pipeshub_client: PipeshubClient,
    graph_provider: GraphProviderProtocol,
) -> AsyncGenerator[dict[str, Any], None]:
    """A connector syncing one object in each of the two test buckets."""
    second = os.getenv(SECOND_BUCKET_ENV)
    if not second:
        pytest.skip(f"{SECOND_BUCKET_ENV} is not set: TC-ACCESS-REVOKE-001 needs a second pre-provisioned bucket")
    kept = r2_bucket()
    ensure_resource_exists(r2_storage, kept)
    ensure_resource_exists(r2_storage, second)

    folder = new_run_folder()
    token = uuid.uuid4().hex[:8]
    keys = {kept: f"{folder}revoke-kept-{token}.txt", second: f"{folder}revoke-dropped-{token}.txt"}
    state: dict[str, Any] = {"resource_name": kept, "folder": folder, "second_bucket": second, "keys": keys}
    try:
        for bucket, key in keys.items():
            r2_storage.upload_object(bucket, key, f"{bucket} {token}".encode(), content_type="text/plain")
        await create_connector_and_await_sync(
            pipeshub_client,
            graph_provider,
            state,
            connector_type=CONNECTOR_TYPE,
            connector_name=f"cloudflare-r2-bucket-filter-test-{token}",
            connector_config={**r2_connector_config(), "filters": bucket_filters(folder, [kept, second])},
            expected_records=len(keys),
        )
        yield state
    finally:
        if "connector_id" in state:
            await destructor(r2_storage, pipeshub_client, graph_provider, state, connector_type=CONNECTOR_TYPE)
        else:
            r2_storage.clear_objects(kept, folder)
        r2_storage.clear_objects(second, folder)


@pytest.mark.integration
@pytest.mark.cloudflare_r2
@pytest.mark.asyncio(loop_scope="session")
class TestCloudflareR2BucketFilter:
    async def test_tc_access_revoke_001_a_deselected_bucket_leaves_the_index(
        self,
        r2_two_bucket_connector: dict[str, Any],
        pipeshub_client: PipeshubClient,
        graph_provider: GraphProviderProtocol,
    ) -> None:
        state = r2_two_bucket_connector
        connector_id, kept, second = state["connector_id"], state["resource_name"], state["second_bucket"]
        kept_name = Path(state["keys"][kept]).name
        dropped_name = Path(state["keys"][second]).name

        async def _both_synced() -> bool:
            return await graph_provider.record_paths_or_names_contain(connector_id, [kept_name, dropped_name])

        await wait_until_graph_condition(
            connector_id, check=_both_synced, timeout=180, poll_interval=10, description="both buckets synced",
        )
        assert {kept, second} <= set(await graph_provider.fetch_record_group_names(connector_id))

        await apply_filter_full_sync(
            pipeshub_client, graph_provider, connector_id, bucket_filters(state["folder"], [kept])
        )

        async def _second_gone() -> bool:
            return (
                await graph_provider.get_record_by_name(connector_id, dropped_name) is None
                and second not in await graph_provider.fetch_record_group_names(connector_id)
            )

        await wait_until_graph_condition(
            connector_id, check=_second_gone, timeout=180, poll_interval=10, description="de-selected bucket removed",
        )
        await graph_provider.assert_record_not_exists(connector_id, dropped_name)
        assert await graph_provider.get_record_by_name(connector_id, kept_name) is not None
        assert kept in await graph_provider.fetch_record_group_names(connector_id)
        logger.info("TC-ACCESS-REVOKE-001 passed: %s removed, %s kept (connector %s)", second, kept, connector_id)
