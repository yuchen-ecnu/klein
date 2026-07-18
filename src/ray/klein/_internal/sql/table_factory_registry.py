# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from collections.abc import Iterable
from importlib.metadata import entry_points

from ray.klein._internal.logging import get_logger
from ray.klein.api.sql_query_error import SQLQueryError
from ray.klein.api.table_factory import TableFactory

logger = get_logger(__name__)

TABLE_FACTORY_ENTRY_POINT_GROUP = "ray.klein.table_factories"


class TableFactoryRegistry:
    """Session-local table-factory registry with optional package discovery."""

    def __init__(self, factories: Iterable[TableFactory] = ()) -> None:
        self._factories: dict[str, TableFactory] = {}
        for factory in factories:
            self.register(factory)

    @classmethod
    def with_defaults(cls) -> TableFactoryRegistry:
        from ray.klein._internal.sql.filesystem_table_factory import FilesystemTableFactory
        from ray.klein._internal.sql.kafka_table_factory import KafkaTableFactory
        from ray.klein._internal.sql.print_table_factory import PrintTableFactory

        registry = cls((FilesystemTableFactory(), KafkaTableFactory(), PrintTableFactory()))
        registry.discover()
        return registry

    def discover(self) -> None:
        """Load third-party factories from the standard Python entry-point group."""

        discovered = entry_points()
        selected = discovered.select(group=TABLE_FACTORY_ENTRY_POINT_GROUP)
        for entry_point in selected:
            factory = _load_factory(entry_point)
            if factory is not None:
                self.register(factory)

    def register(self, factory: TableFactory, *, replace: bool = False) -> None:
        if not isinstance(factory, TableFactory):
            raise TypeError("table factory must inherit TableFactory")
        identifier = factory.identifier
        if not isinstance(identifier, str) or not identifier:
            raise TypeError("a table factory must define a non-empty string identifier")
        if identifier in self._factories and not replace:
            raise SQLQueryError(f"Table factory {identifier!r} is already registered")
        self._factories[identifier] = factory

    def get(self, identifier: str) -> TableFactory:
        try:
            return self._factories[identifier]
        except KeyError as exc:
            available = ", ".join(sorted(self._factories)) or "none"
            raise SQLQueryError(f"Unknown table factory {identifier!r}; available factories: {available}") from exc

    @property
    def identifiers(self) -> tuple[str, ...]:
        return tuple(sorted(self._factories))


def _load_factory(entry_point) -> TableFactory | None:
    try:
        value = entry_point.load()
        return value() if isinstance(value, type) else value
    except Exception:
        logger.warning("Failed to load table factory entry point %s", entry_point.name, exc_info=True)
        return None
