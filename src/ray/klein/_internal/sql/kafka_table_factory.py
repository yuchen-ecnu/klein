# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar

from ray.klein._internal.sql.connector_options import (
    parse_option_value,
    prefixed_options,
    reject_unknown_options,
    require_option,
)
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
            "format",
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
        reject_unknown_options(
            table.options,
            connector=self.identifier,
            supported=self._OPTIONS,
            prefixes=("canal-json.",),
        )
        value_format = self._option(table, "format", "raw")
        if value_format not in {"raw", "canal-json"}:
            raise SQLQueryError("Kafka source 'format' must be 'raw' or 'canal-json'")
        format_options = self._format_options(table)
        if value_format == "raw" and format_options:
            raise SQLQueryError("canal-json.* options require 'format'='canal-json'")
        if value_format == "canal-json":
            from ray.klein.formats.canal_json import _normalize_canal_json_options

            try:
                _normalize_canal_json_options(format_options)
            except (TypeError, ValueError) as error:
                raise SQLQueryError(str(error)) from error

    @staticmethod
    def _option(table: CatalogTable, name: str, default: Any = None) -> Any:
        value = table.options.get(name)
        return default if value is None else parse_option_value(value)

    @staticmethod
    def _format_options(table: CatalogTable) -> dict[str, Any]:
        return {name.replace("-", "_"): value for name, value in prefixed_options(table.options, "canal-json.").items()}

    def create_source(self, context: KleinContext, table: CatalogTable) -> DataStream:
        topics = self._option(table, "topics", self._option(table, "topic"))
        bootstrap_servers = table.options["bootstrap_servers"]
        value_format = self._option(table, "format", "raw")
        options = {
            "trigger": self._option(table, "trigger", "continuous" if value_format == "canal-json" else "once"),
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
            "value_format": value_format,
            "format_options": self._format_options(table),
        }
        return context.read_kafka(topics, bootstrap_servers=bootstrap_servers, **options)

    def create_sink(self, stream: DataStream, table: CatalogTable) -> Any:
        if self._option(table, "format", "raw") != "raw":
            raise SQLQueryError("Kafka sink formats are not supported; use 'value_serializer'")
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
