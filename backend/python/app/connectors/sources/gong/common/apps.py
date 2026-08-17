from app.config.constants.arangodb import AppGroups, Connectors
from app.connectors.core.interfaces.connector.apps import App


class GongApp(App):
    def __init__(self, connector_id: str) -> None:
        super().__init__(Connectors.GONG, AppGroups.GONG, connector_id)
