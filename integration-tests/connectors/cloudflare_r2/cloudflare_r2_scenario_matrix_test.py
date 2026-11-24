# pyright: ignore-file

"""Cloudflare R2 in the shared scenario matrix (``connectors/scenario_matrix.py``).

Items are objects in this run's folder of the test bucket; see
``connectors/storage_scenario_adapter.py`` for the layout and the filter used.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator

import pytest
import pytest_asyncio
from connectors.cloudflare_r2.conftest import (
    CONNECTOR_TYPE,
    r2_bucket,
    r2_connector_config,
)
from connectors.cloudflare_r2.r2_storage_helper import R2StorageHelper
from connectors.scenario_matrix import Action, ConnectorScenarioMatrix
from connectors.storage_scenario_adapter import (
    APP_LEVEL_PERMISSIONS,
    StorageScenarioAdapter,
    storage_matrix,
)
from helper.graph_provider import GraphProviderProtocol
from pipeshub_client import PipeshubClient  # type: ignore[import-not-found]


class R2Adapter(StorageScenarioAdapter):
    """Objects written, copied and deleted through the R2 storage helper."""

    source = CONNECTOR_TYPE

    def _write(self, key: str, text: str) -> None:
        self.storage.upload_object(self.resource, key, text.encode(), "text/plain")

    def _delete(self, key: str) -> None:
        self.storage.delete_object(self.resource, key)

    def _rename(self, key: str, new_key: str) -> None:
        self.storage.rename_object(self.resource, key, new_key)


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def scenario_adapter(
    r2_storage: R2StorageHelper,
    pipeshub_client: PipeshubClient,
    graph_provider: GraphProviderProtocol,
) -> AsyncGenerator[R2Adapter, None]:
    async with storage_matrix(
        R2Adapter,
        storage=r2_storage,
        resource=r2_bucket(),
        client=pipeshub_client,
        graph=graph_provider,
        connector_type=CONNECTOR_TYPE,
        connector_config=r2_connector_config(),
    ) as adapter:
        yield adapter


@pytest.mark.integration
@pytest.mark.cloudflare_r2
class TestCloudflareR2ScenarioMatrix(ConnectorScenarioMatrix):
    SOURCE = CONNECTOR_TYPE
    TREE_MOVES = True
    UNSUPPORTED = {Action.CHANGE_PERMISSION.value: APP_LEVEL_PERMISSIONS}
