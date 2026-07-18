# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar

from ray.klein._internal.sql.connector_options import parse_option_value, reject_unknown_options, require_option
from ray.klein.api.sql_query_error import SQLQueryError
from ray.klein.api.table_factory import TableFactory

if TYPE_CHECKING:
    from ray.klein.api.catalog_table import CatalogTable
    from ray.klein.api.data_stream import DataStream
    from ray.klein.api.klein_context import KleinContext


class KafkaTableFactory(TableFactory):
    identifier = "kafka"
    _OPTIONS: ClassVar[frozenset[str]] = frozenset(
        {
            "connector",
            "topics",
            "topic",
            "bootstrap_servers",
            "trigger",
            "start_offset",
            "end_offset",
            "consumer_config",
            "num_cpus",
            "num_gpus",
            "memory",
            "ray_remote_args",
            "override_num_blocks",
            "timeout_ms",
            "partition_discovery_interval_ms",
            "max_batch_size",
            "key_field",
            "key_serializer",
            "value_serializer",
            "producer_config",
            "concurrency",
        }
    )

    def validate(self, table: CatalogTable) -> None:
        require_option(table.options, "bootstrap_servers", self.identifier)
        if not table.options.get("topics") and not table.options.get("topic"):
            raise SQLQueryError("Kafka table requires 'topics' for reads or 'topic' for writes")
        reject_unknown_options(table.options, connector=self.identifier, supported=self._OPTIONS)

    @staticmethod
    def _option(table: CatalogTable, name: str, default: Any = None) -> Any:
        value = table.options.get(name)
        return default if value is None else parse_option_value(value)

    def create_source(self, context: KleinContext, table: CatalogTable) -> DataStream:
        topics = self._option(table, "topics", self._option(table, "topic"))
        bootstrap_servers = table.options["bootstrap_servers"]
        options = {
            "trigger": self._option(table, "trigger", "once"),
            "start_offset": self._option(table, "start_offset", "earliest"),
            "end_offset": self._option(table, "end_offset", "latest"),
            "consumer_config": self._option(table, "consumer_config"),
            "num_cpus": self._option(table, "num_cpus"),
            "num_gpus": self._option(table, "num_gpus"),
            "memory": self._option(table, "memory"),
            "ray_remote_args": self._option(table, "ray_remote_args"),
            "override_num_blocks": self._option(table, "override_num_blocks"),
            "timeout_ms": self._option(table, "timeout_ms"),
            "concurrency": self._option(table, "concurrency"),
            "partition_discovery_interval_ms": self._option(table, "partition_discovery_interval_ms", 30_000),
            "max_batch_size": self._option(table, "max_batch_size", 1_000),
        }
        return context.read_kafka(topics, bootstrap_servers=bootstrap_servers, **options)

    def create_sink(self, stream: DataStream, table: CatalogTable) -> Any:
        topic = self._option(table, "topic")
        if topic is None:
            topics = self._option(table, "topics")
            if isinstance(topics, list) and len(topics) == 1:
                topics = topics[0]
            if not isinstance(topics, str) or "," in topics:
                raise SQLQueryError("Kafka sink requires exactly one 'topic'")
            topic = topics
        bootstrap_servers = self._option(table, "bootstrap_servers")
        if not isinstance(bootstrap_servers, str):
            raise SQLQueryError("Kafka sink 'bootstrap_servers' must be a string")
        options = {
            "key_field": self._option(table, "key_field"),
            "key_serializer": self._option(table, "key_serializer", "string"),
            "value_serializer": self._option(table, "value_serializer", "json"),
            "producer_config": self._option(table, "producer_config"),
            "ray_remote_args": self._option(table, "ray_remote_args"),
            "concurrency": self._option(table, "concurrency"),
        }
        return stream.write_kafka(topic, bootstrap_servers, **options)
