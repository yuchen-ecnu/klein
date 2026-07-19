# SPDX-License-Identifier: Apache-2.0
import inspect
import random
import string
import uuid
from collections.abc import Callable, Iterable, Mapping
from datetime import datetime
from threading import RLock
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Literal,
)

import ray.data
from ray.util.annotations import PublicAPI

from ray.klein._internal.logging import get_logger
from ray.klein.api.data_stream import DataStream
from ray.klein.api.functions.logical_function import LogicalFunction
from ray.klein.api.job_client import JobClient
from ray.klein.api.job_handle import JobHandle
from ray.klein.api.ray_data import (
    RayDataCall,
    RayDataContextAdapter,
)
from ray.klein.api.source_function import SourceFunction
from ray.klein.api.stream_sink import StreamSink
from ray.klein.api.stream_source import StreamSource
from ray.klein.config.configuration import ConfigInput, Configuration
from ray.klein.runtime.backend.batch_only_source import BatchOnlySource
from ray.klein.runtime.backend.collection_source import CollectionSource
from ray.klein.runtime.resources import Resources

if TYPE_CHECKING:
    from ray.klein.api.row_kind import RowKind
    from ray.klein.api.sql_session import SQLSession


logger = get_logger(__name__)


def _continuous_kafka_source_class() -> type[SourceFunction]:
    try:
        from ray.klein.integrations.kafka import KafkaSource
    except ModuleNotFoundError as error:
        if error.name != "confluent_kafka":
            raise
        raise ModuleNotFoundError("Continuous Kafka input requires `ray-klein[kafka]`.") from error
    return KafkaSource


def _normalize_kafka_value_format(
    value_format: str,
    format_options: Mapping[str, Any] | None,
    *,
    trigger: str,
) -> dict[str, Any] | None:
    if value_format not in {"raw", "canal-json"}:
        raise ValueError("value_format must be 'raw' or 'canal-json'")
    if value_format == "raw":
        if format_options:
            raise ValueError("format_options require a non-raw value_format")
        return None
    if trigger != "continuous":
        raise ValueError("value_format='canal-json' requires trigger='continuous'")
    from ray.klein.formats.canal_json import _normalize_canal_json_options

    return _normalize_canal_json_options(format_options)


def _rocketmq_source_class() -> type[SourceFunction]:
    try:
        import rocketmq.client  # noqa: F401

        from ray.klein.integrations.rocketmq import RocketMQSource
    except (ImportError, OSError) as error:
        raise ModuleNotFoundError(
            "RocketMQ input requires `ray-klein[rocketmq]` and a compatible `librocketmq` runtime."
        ) from error
    return RocketMQSource


class KleinContext:
    """Configuration and graph builder for one Klein pipeline.

    ``current()``, ``install()``, and ``reset()`` back the concise module-level
    API while direct construction provides isolated pipeline contexts.
    """

    _current: ClassVar["KleinContext | None"] = None
    _lock: ClassVar[RLock] = RLock()

    def __init__(self, configuration: ConfigInput = None) -> None:
        self._config = configuration if isinstance(configuration, Configuration) else Configuration(configuration)
        self._sinks: list[StreamSink] = []
        self._last_stream_id = 0
        self.interactive_mode_enabled = False
        self._sql_session = None

    def _allocate_stream_id(self) -> int:
        self._last_stream_id += 1
        return self._last_stream_id

    @classmethod
    def current(cls) -> "KleinContext":
        with cls._lock:
            if cls._current is None:
                cls._current = cls()
            return cls._current

    @classmethod
    def install(cls, context: "KleinContext") -> "KleinContext":
        if not isinstance(context, cls):
            raise TypeError(f"context must be {cls.__name__}, got {type(context).__name__}")
        with cls._lock:
            cls._current = context
        return context

    @classmethod
    def reset(cls, configuration: ConfigInput = None) -> "KleinContext":
        with cls._lock:
            cls._current = cls(configuration)
            return cls._current

    @property
    def config(self) -> Configuration:
        """Mutable typed configuration owned by this execution context."""

        return self._config

    @property
    def sinks(self) -> tuple[StreamSink, ...]:
        """Terminal streams registered with this context."""

        return tuple(self._sinks)

    def configure(self, options: ConfigInput = None) -> "KleinContext":
        """Overlay explicit code configuration and return this context."""

        self._config.update(options)
        return self

    @property
    def data(self) -> RayDataContextAdapter:
        """Public, version-adaptive bridge to the installed ``ray.data`` API."""

        return RayDataContextAdapter(self)

    @property
    def sql_session(self) -> "SQLSession":
        """Return this context's persistent SQL session and temporary-view catalog."""

        if self._sql_session is None:
            from ray.klein.api.sql_session import SQLSession

            self._sql_session = SQLSession(self)
        return self._sql_session

    def sql(
        self,
        query: str,
        /,
        *,
        tables: Mapping[str, "DataStream"] | None = None,
        num_cpus: float = 1.0,
    ) -> "DataStream":
        """Build lazy SQL over bounded DataStreams in this context.

        When ``tables`` is omitted, named DataStream variables in the caller's
        scope are discovered in the same style as ``daft.sql``.
        """

        if tables is None:
            from ray.klein._internal.sql.scope import discover_streams

            frame = inspect.currentframe()
            try:
                caller = frame.f_back if frame is not None else None
                tables = discover_streams(caller, context=self) if caller is not None else {}
            finally:
                del frame
        return self.sql_session.sql(
            query,
            tables=tables,
            num_cpus=num_cpus,
        )

    def execute_sql(self, statement: str, /, *, num_cpus: float = 1.0) -> Any:
        """Execute Flink-style table DDL/DML or build a lazy SQL query."""

        return self.sql_session.execute_sql(statement, num_cpus=num_cpus)

    def _from_ray_data(self, call: RayDataCall) -> "DataStream":
        """Attach one batch-only Ray Data source call to this context."""

        return self.source(
            BatchOnlySource,
            fn_constructor_args=[call.display_name],
            lowering=call,
            name=f"RayData.{call.display_name}",
            bounded=True,
        )

    def enable_interactive_mode(self, enable: bool = True) -> "KleinContext":
        """
        Enable interactive mode for the data stream context.

        Examples:

            .. testcode::

                from ray.klein.api.klein_context import KleinContext

                ctx = KleinContext()
                ctx.enable_interactive_mode()
                stream = ctx.data.read_tfrecords("s3://bucket/path")
                stream.data.schema()  # Evaluated immediately without ctx.execute().
                stream.data.take(1)

        Args:
            enable: True to turn interactive mode on (default), False to disable.
        """
        self.interactive_mode_enabled = enable
        return self

    @PublicAPI
    def read_kafka(
        self,
        topics: str | list[str],
        *,
        bootstrap_servers: str | list[str],
        trigger: Literal["once", "continuous"] = "once",
        start_offset: int | datetime | Literal["earliest", "latest"] | dict[Any, Any] = "earliest",
        end_offset: int | datetime | Literal["latest"] | dict[Any, Any] = "latest",
        consumer_config: dict[str, Any] | None = None,
        num_cpus: float | None = None,
        num_gpus: float | None = None,
        memory: float | None = None,
        ray_remote_args: dict[str, Any] | None = None,
        override_num_blocks: int | None = None,
        timeout_ms: int | None = None,
        concurrency: int | None = None,
        partition_discovery_interval_ms: int = 30_000,
        max_batch_size: int = 1_000,
        value_format: Literal["raw", "canal-json"] = "raw",
        format_options: dict[str, Any] | None = None,
    ) -> "DataStream":
        """Read a bounded Kafka snapshot or an unbounded Kafka stream.

        ``trigger="once"`` preserves :func:`ray.data.read_kafka` semantics.
        ``trigger="continuous"`` runs a checkpoint-aware Klein source and
        keeps polling until the job is drained. ``value_format="raw"`` emits
        Ray Data's byte-oriented Kafka schema. ``value_format="canal-json"``
        is continuous-only and expands Canal FlatMessage values into native
        INSERT/UPDATE/DELETE changelog rows.
        """

        if trigger not in {"once", "continuous"}:
            raise ValueError("trigger must be 'once' or 'continuous'")
        normalized_format_options = _normalize_kafka_value_format(
            value_format,
            format_options,
            trigger=trigger,
        )
        if trigger == "continuous":
            return self._read_continuous_kafka(
                topics,
                bootstrap_servers=bootstrap_servers,
                start_offset=start_offset,
                end_offset=end_offset,
                consumer_config=consumer_config,
                num_cpus=num_cpus,
                num_gpus=num_gpus,
                memory=memory,
                ray_remote_args=ray_remote_args,
                override_num_blocks=override_num_blocks,
                timeout_ms=timeout_ms,
                concurrency=concurrency,
                partition_discovery_interval_ms=partition_discovery_interval_ms,
                max_batch_size=max_batch_size,
                value_format=value_format,
                format_options=normalized_format_options,
            )

        if concurrency is not None:
            raise ValueError("concurrency is only supported when trigger='continuous'")
        if partition_discovery_interval_ms != 30_000 or max_batch_size != 1_000:
            raise ValueError("partition discovery and poll batch options require trigger='continuous'")

        return self.data.source(
            "read_kafka",
            topics,
            bootstrap_servers=bootstrap_servers,
            trigger=trigger,
            start_offset=start_offset,
            end_offset=end_offset,
            consumer_config=consumer_config,
            num_cpus=num_cpus,
            num_gpus=num_gpus,
            memory=memory,
            ray_remote_args=ray_remote_args,
            override_num_blocks=override_num_blocks,
            timeout_ms=timeout_ms,
        )

    def _read_continuous_kafka(
        self,
        topics: str | list[str],
        *,
        bootstrap_servers: str | list[str],
        start_offset: int | datetime | Literal["earliest", "latest"] | dict[Any, Any],
        end_offset: int | datetime | Literal["latest"] | dict[Any, Any],
        consumer_config: dict[str, Any] | None,
        num_cpus: float | None,
        num_gpus: float | None,
        memory: float | None,
        ray_remote_args: dict[str, Any] | None,
        override_num_blocks: int | None,
        timeout_ms: int | None,
        concurrency: int | None,
        partition_discovery_interval_ms: int,
        max_batch_size: int,
        value_format: Literal["raw", "canal-json"],
        format_options: dict[str, Any] | None,
    ) -> "DataStream":
        if end_offset != "latest":
            raise ValueError("end_offset is not supported when trigger='continuous'")
        if memory is not None:
            raise ValueError("memory is not supported by the continuous streaming backend")
        if ray_remote_args:
            raise ValueError("ray_remote_args is not supported by the continuous streaming backend")
        if concurrency is not None and override_num_blocks is not None and concurrency != override_num_blocks:
            raise ValueError("concurrency and override_num_blocks must match when both are provided")
        source_parallelism = concurrency if concurrency is not None else override_num_blocks
        source_options = {
            "bootstrap_servers": bootstrap_servers,
            "start_offset": start_offset,
            "consumer_config": consumer_config,
            "timeout_ms": timeout_ms,
            "partition_discovery_interval_ms": partition_discovery_interval_ms,
            "max_batch_size": max_batch_size,
        }
        if value_format != "raw":
            source_options["value_format"] = value_format
            source_options["format_options"] = format_options
        changelog_mode = None
        if value_format == "canal-json":
            from ray.klein.api.row_kind import RowKind

            changelog_mode = RowKind
        return self.source(
            _continuous_kafka_source_class(),
            fn_constructor_args=[topics],
            fn_constructor_kwargs=source_options,
            num_cpus=num_cpus,
            num_gpus=num_gpus,
            concurrency=source_parallelism,
            name="KafkaSource" if value_format == "raw" else f"KafkaSource[{value_format}]",
            bounded=False,
            changelog_mode=changelog_mode,
        )

    @PublicAPI
    def read_canal(
        self,
        topics: str | list[str],
        *,
        bootstrap_servers: str | list[str],
        start_offset: int | datetime | Literal["earliest", "latest"] | dict[Any, Any] = "earliest",
        consumer_config: dict[str, Any] | None = None,
        num_cpus: float | None = None,
        num_gpus: float | None = None,
        concurrency: int | None = None,
        timeout_ms: int | None = None,
        partition_discovery_interval_ms: int = 30_000,
        max_batch_size: int = 1_000,
        include_metadata: bool = True,
        ddl_handling: Literal["ignore", "emit", "fail"] = "ignore",
    ) -> "DataStream":
        """Read Canal FlatMessage JSON continuously from Kafka.

        The Canal server must use an MQ mode with ``canal.mq.flatMessage=true``.
        INSERT and DELETE events emit one changelog row per row image; UPDATE
        events emit ``UPDATE_BEFORE`` followed by ``UPDATE_AFTER``. Values keep
        Canal's string-or-null representation and are not coerced from MySQL
        type metadata.
        """

        return self.read_kafka(
            topics,
            bootstrap_servers=bootstrap_servers,
            trigger="continuous",
            start_offset=start_offset,
            consumer_config=consumer_config,
            num_cpus=num_cpus,
            num_gpus=num_gpus,
            concurrency=concurrency,
            timeout_ms=timeout_ms,
            partition_discovery_interval_ms=partition_discovery_interval_ms,
            max_batch_size=max_batch_size,
            value_format="canal-json",
            format_options={"include_metadata": include_metadata, "ddl_handling": ddl_handling},
        )

    @PublicAPI
    def read_rocketmq(
        self,
        topic: str,
        *,
        name_server_address: str,
        consumer_group: str,
        tag_expression: str = "*",
        message_model: Literal["clustering", "broadcasting"] = "clustering",
        orderly: bool = False,
        access_key: str | None = None,
        access_secret: str | None = None,
        channel: str = "KLEIN",
        ssl_enabled: bool = False,
        ssl_property_file: str | None = None,
        consumer_threads: int = 20,
        max_pending_messages: int = 1_000,
        poll_timeout_ms: int = 1_000,
        message_trace_enabled: bool = False,
        num_cpus: float | None = None,
        num_gpus: float | None = None,
        concurrency: int | None = None,
    ) -> "DataStream":
        """Read an unbounded stream from an Apache RocketMQ topic.

        The source uses RocketMQ's remoting-protocol PushConsumer. Consumer
        progress is owned by the RocketMQ consumer group; Klein waits until a
        record has entered the downstream collector before acknowledging the
        callback. The emitted mapping contains raw byte ``key``, ``value``, and
        ``tags`` fields together with RocketMQ message and queue metadata.
        """

        if message_model == "broadcasting" and concurrency not in {None, 1}:
            raise ValueError("broadcasting RocketMQ input requires concurrency=1 to avoid duplicate source copies")
        return self.source(
            _rocketmq_source_class(),
            fn_constructor_args=[topic],
            fn_constructor_kwargs={
                "name_server_address": name_server_address,
                "consumer_group": consumer_group,
                "tag_expression": tag_expression,
                "message_model": message_model,
                "orderly": orderly,
                "access_key": access_key,
                "access_secret": access_secret,
                "channel": channel,
                "ssl_enabled": ssl_enabled,
                "ssl_property_file": ssl_property_file,
                "consumer_threads": consumer_threads,
                "max_pending_messages": max_pending_messages,
                "poll_timeout_ms": poll_timeout_ms,
                "message_trace_enabled": message_trace_enabled,
            },
            num_cpus=num_cpus,
            num_gpus=num_gpus,
            concurrency=concurrency,
            name="RocketMQSource",
            bounded=False,
        )

    @PublicAPI
    def from_items(
        self,
        items: list[Any],
        *,
        name: str = "FromItemsSource",
    ) -> "DataStream":
        """Create a :class:`~klein.api.DataStream` from a list of local Python objects as Single Column.

        Use this method to create small datasets from data that fits in memory.

        Examples:

            .. testcode::

                ctx = KleinContext()
                stream = ctx.from_items([{"name": "Jack", "age": 23}, {"name": "Lucy", "age": 18}])

        Args:
            items: List of local Python objects.
            name: operator name.

        Returns:
            A :class:`StreamSource` constructed from the provided items.
        """
        stream = self.source(
            CollectionSource,
            fn_constructor_args=[items],
            lowering=RayDataCall.source_callable(
                ray.data.from_items,
                (items,),
                {},
            ),
            name=name,
            bounded=True,
        )
        from ray.klein.api.changelog_row import row_kind_of

        row_kinds = {row_kind_of(item) for item in items if isinstance(item, Mapping)}
        return stream._set_changelog_mode(frozenset(row_kinds)) if row_kinds else stream

    @PublicAPI
    def from_values(self, *values: Any, name: str = "ValueSource") -> "DataStream":
        """Creates a data stream from values with multiple column.

        Examples:

            .. testcode::

                ctx = KleinContext()
                stream = ctx.from_values({"name": "Jack", "age": 23}, {"name": "Lucy", "age": 18})

        Args:
            *values: The elements to create the data stream from.
            name: operator name.

        Returns:
            The data stream representing the given values
        """
        if not values:
            raise ValueError("from_values() requires at least one value")
        for index, value in enumerate(values):
            if not isinstance(value, Mapping):
                raise TypeError(
                    f"from_values() values must be mappings; value at index {index} is {type(value).__name__}"
                )
        stream = self.source(
            CollectionSource,
            fn_constructor_args=[values],
            name=name,
        )
        from ray.klein.api.changelog_row import row_kind_of

        return stream._set_changelog_mode(frozenset(row_kind_of(value) for value in values))

    def source(
        self,
        fn: type[SourceFunction],
        *,
        fn_constructor_args: Iterable[Any] | None = None,
        fn_constructor_kwargs: dict[str, Any] | None = None,
        lowering: Callable | None = None,
        num_cpus: float | None = None,
        num_gpus: float | None = None,
        concurrency: int | tuple[int, int] | None = None,
        name: str | None = None,
        bounded: bool = False,
        changelog_mode: Iterable["RowKind"] | None = None,
    ) -> "DataStream":
        """Create an input data stream with a SourceFunction

        Args:
            fn: the SourceFunction used to create the data stream
            fn_constructor_args: Positional arguments to pass to ``fn``'s constructor.
                You can only provide this if ``fn`` is a callable class. These arguments
                are top-level arguments in the underlying Ray actor construction task.
            fn_constructor_kwargs: Keyword arguments to pass to ``fn``'s constructor.
                This can only be provided if ``fn`` is a callable class. These arguments
                are top-level arguments in the underlying Ray actor construction task.
            lowering: declarative recipe for lowering this source to a ray.data
                read. ``None`` for streaming-only sources (no batch backend).
            num_cpus: The number of CPU cores.
            num_gpus: The number of GPU.
            concurrency: The number of parallelism, defaults to 1.
            name: operator name
            bounded: Indicate whether source is bounded, unbounded by default.
            changelog_mode: Flink-style row changes emitted by this source.

        Returns:
            The data stream constructed from the source_func
        """
        if not isinstance(fn, type) or not issubclass(fn, SourceFunction):
            raise TypeError("fn must be a SourceFunction class")
        resources = Resources(num_cpus, num_gpus, concurrency)
        stream = StreamSource(
            self,
            LogicalFunction(
                fn,
                fn_constructor_args=fn_constructor_args,
                fn_constructor_kwargs=fn_constructor_kwargs,
                lowering=lowering,
                resources=resources,
            ),
            resources=resources,
            name=name,
            bounded=bounded,
        )
        if changelog_mode is None:
            return stream
        from ray.klein.api.row_kind import RowKind

        declared = frozenset(changelog_mode)
        if not declared or any(not isinstance(row_kind, RowKind) for row_kind in declared):
            raise ValueError("changelog_mode must contain one or more RowKind values")
        return stream._set_changelog_mode(declared)

    def execute(self, job_name: str | None = None) -> "JobHandle":
        """
        Execute the job.

        Args:
            job_name: name of the job

        Returns:
            A :class:`JobHandle` for the submitted (or already-finished) job.
        """
        job_name = job_name or ("klein-" + "".join(random.choices(string.ascii_letters + string.digits, k=8)))
        client = JobClient(self._config)
        return client.execute(job_name, self._sinks)

    def explain(self, job_name: str | None = None) -> str:
        """Get the execution plan of the data stream"""
        client = JobClient(self._config)
        job_name = job_name or f"job_{uuid.uuid4()}"
        return client.explain(job_name, self._sinks)

    def add_sink(self, sink: StreamSink) -> None:
        self._sinks.append(sink)


def current_context() -> KleinContext:
    return KleinContext.current()


def install_context(context: KleinContext) -> KleinContext:
    return KleinContext.install(context)


def reset_context(configuration: ConfigInput = None) -> KleinContext:
    return KleinContext.reset(configuration)


def configure(options: ConfigInput = None) -> KleinContext:
    """Overlay the current process-global context configuration."""

    return KleinContext.current().configure(options)


def execute(job_name: str | None = None) -> JobHandle:
    return KleinContext.current().execute(job_name)


def explain(job_name: str | None = None) -> str:
    return KleinContext.current().explain(job_name)


def execute_sql(statement: str, /, *, num_cpus: float = 1.0) -> Any:
    return KleinContext.current().execute_sql(statement, num_cpus=num_cpus)
