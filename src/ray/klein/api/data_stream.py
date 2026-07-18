# SPDX-License-Identifier: Apache-2.0
import copy
from collections.abc import Callable, Iterable, Mapping
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from ray.data.block import UserDefinedFunction
from ray.util.annotations import PublicAPI

from ray.klein._internal.messages import ChineseMessages
from ray.klein.api.collect_function import CollectFunction
from ray.klein.api.functions.logical_function import LogicalFunction
from ray.klein.api.functions.ray_data_lowering import (
    lower_filter,
    lower_flat_map,
    lower_map,
    lower_map_batches,
    lower_union,
)
from ray.klein.api.missing_data_strategy import MissingDataStrategy
from ray.klein.api.node_type import NodeType
from ray.klein.api.ray_data import (
    RayDataCall,
    RayDataStreamAdapter,
)
from ray.klein.api.sink_function import SinkFunction
from ray.klein.api.stream import Stream
from ray.klein.integrations.console.console_sink import ConsoleSinkFunction
from ray.klein.integrations.filesystem.streaming_file_sink import StreamingFileSink
from ray.klein.runtime.backend.batch_only_sink import BatchOnlySink
from ray.klein.runtime.backend.batch_only_transform import BatchOnlyTransform
from ray.klein.runtime.operator.batch_process_operator import BatchProcessOperator
from ray.klein.runtime.operator.filter_operator import FilterOperator
from ray.klein.runtime.operator.flat_map_operator import FlatMapOperator
from ray.klein.runtime.operator.flat_map_with_rank_operator import FlatMapWithRankOperator
from ray.klein.runtime.operator.map_operator import MapOperator
from ray.klein.runtime.operator.operator import StreamOperator
from ray.klein.runtime.operator.reduce_operator import ReduceOperator
from ray.klein.runtime.operator.union_operator import UnionOperator
from ray.klein.runtime.partitioning.adaptive_partitioner import AdaptivePartitioner
from ray.klein.runtime.partitioning.broadcast_partitioner import BroadcastPartitioner
from ray.klein.runtime.partitioning.key_partitioner import KeyPartitioner
from ray.klein.runtime.partitioning.partitioner import Partitioner
from ray.klein.runtime.partitioning.rescale_partitioner import RescalePartitioner
from ray.klein.runtime.partitioning.round_robin_partitioner import RoundRobinPartitioner
from ray.klein.runtime.partitioning.simple_partitioner import SimplePartitioner
from ray.klein.runtime.resources import Resources

if TYPE_CHECKING:
    from ray.klein.api.keyed_stream import KeyedStream
    from ray.klein.api.klein_context import KleinContext
    from ray.klein.api.row_kind import RowKind
    from ray.klein.api.watermark_strategy import WatermarkStrategy
    from ray.klein.integrations.redis.redis_connection_config import RedisConnectionConfig
    from ray.klein.integrations.redis.redis_sink_config import RedisSinkConfig


def _identity(value: Any) -> Any:
    return value


class DataStream(Stream):
    """
    Represents a stream of data which applies a transformation executed by
    python.
    """

    def __init__(
        self,
        input_stream: "Stream | list[Stream]",
        stream_operator: StreamOperator,
        name: str,
        node_type: NodeType,
        *,
        resources: Resources | None = None,
        context: "KleinContext | None" = None,
        ray_serve_enabled: bool = False,
    ) -> None:
        super().__init__(
            input_stream if isinstance(input_stream, list) else [input_stream],
            stream_operator,
            name,
            node_type,
            resources=resources,
            context=context,
            ray_serve_enabled=ray_serve_enabled,
        )
        from ray.klein.api.row_kind import RowKind

        inherited_modes = [stream.changelog_mode for stream in self.input_streams if isinstance(stream, DataStream)]
        self._changelog_mode = frozenset().union(*inherited_modes) if inherited_modes else frozenset({RowKind.INSERT})

    @property
    def changelog_mode(self) -> frozenset["RowKind"]:
        """Changes this stream can emit when executed as a dynamic table."""

        return self._changelog_mode

    def _set_changelog_mode(self, mode: frozenset["RowKind"]) -> "DataStream":
        self._changelog_mode = frozenset(mode)
        return self

    def _transform(
        self,
        operator: StreamOperator,
        name: str,
        resources: Resources,
        ray_serve_enabled: bool = False,
    ) -> "DataStream":
        """Build a downstream transform from this stream."""
        return DataStream(
            self,
            operator,
            name,
            NodeType.TRANSFORM,
            resources=resources,
            ray_serve_enabled=ray_serve_enabled,
        )

    @property
    def data(self) -> RayDataStreamAdapter:
        """Public, version-adaptive bridge to ``ray.data.Dataset`` methods."""

        return RayDataStreamAdapter(self)

    def sql(
        self,
        query: str,
        /,
        *,
        table_name: str = "self",
        tables: Mapping[str, "DataStream"] | None = None,
        num_cpus: float = 1.0,
    ) -> "DataStream":
        """Build lazy SQL with this bounded stream registered as ``self`` by default."""

        extra_bindings = dict(tables or {})
        if table_name in extra_bindings and extra_bindings[table_name] is not self:
            raise ValueError(f"SQL table {table_name!r} is already bound to another DataStream")
        bindings = {table_name: self, **extra_bindings}
        return self.context.sql_session.sql(
            query,
            tables=bindings,
            num_cpus=num_cpus,
        )

    def _apply_ray_data(self, call: RayDataCall, dependencies: tuple["Stream", ...]) -> "DataStream":
        """Attach a batch-only Dataset-producing call to the graph."""

        resources = Resources()
        return DataStream(
            list(dependencies),
            MapOperator(
                LogicalFunction(
                    BatchOnlyTransform,
                    lowering=call,
                    resources=resources,
                )
            ),
            f"RayData.{call.display_name}",
            NodeType.TRANSFORM,
            resources=resources,
        )

    def _consume_ray_data(
        self,
        call: RayDataCall,
        dependencies: tuple["Stream", ...],
    ) -> Any:
        """Attach a batch-only terminal Ray Data call to the graph."""

        input_streams = list(dependencies)
        if self.context.interactive_mode_enabled:
            input_streams = copy.deepcopy(input_streams)

        resources = Resources()
        sink = StreamSink(
            input_streams,
            LogicalFunction(
                BatchOnlySink,
                lowering=call,
                resources=resources,
            ),
            resources=resources,
            name=f"RayData.{call.display_name}",
        )
        if self.context.interactive_mode_enabled:
            return input_streams[0].context.execute(sink.name).get()
        return sink

    @PublicAPI

    # ------------------------------------------------------------------
    #  Element-wise transforms: map, map_batches, flat_map, map_reduce, filter
    # ------------------------------------------------------------------

    def map(
        self,
        fn: UserDefinedFunction[dict[str, Any], dict[str, Any]],
        *,
        fn_constructor_args: Iterable[Any] | None = None,
        fn_constructor_kwargs: dict[str, Any] | None = None,
        num_cpus: float | None = None,
        num_gpus: float | None = None,
        concurrency: int | tuple[int, int] | None = None,
        batch_size: int | None = None,
        batch_timeout: timedelta = timedelta(seconds=3),
        name: str | None = None,
        ray_serve_enabled: bool = False,
        async_buffer_size: int | None = None,
    ) -> "DataStream":
        """
        Applies a Map transformation on a :class:`DataStream`.

        Examples:

            Call :meth:`~DataStream.map` to transform your data.

            .. testcode::

                from ray.klein.api.klein_context import KleinContext
                def add_dog_years(batch: dict) -> dict:
                    return {"age_in_dog_years": batch["age"] * 7}

                ctx = KleinContext()
                ctx.from_items([
                    {"name": "Luna", "age": 4},
                    {"name": "Rory", "age": 14},
                    {"name": "Scout", "age": 9},
                ]).map_batches(add_dog_years).show()
                ctx.execute("demo").wait()

        Args:
            fn: The function or generator to apply to a record/batch, or a class type
                that can be instantiated to create such a callable. Note ``fn`` must be
                pickle-able.
            fn_constructor_args: Positional arguments to pass to ``fn``'s constructor.
                You can only provide this if ``fn`` is a callable class. These arguments
                are top-level arguments in the underlying Ray actor construction task.
            fn_constructor_kwargs: Keyword arguments to pass to ``fn``'s constructor.
                This can only be provided if ``fn`` is a callable class. These arguments
                are top-level arguments in the underlying Ray actor construction task.
            num_cpus: The number of CPU cores, defaults to 1
            num_gpus: The number of GPU, defaults to 0.
            concurrency: The number of parallelism, defaults to 1
            batch_size: The max number of records for each batch, defaults to None.
            batch_timeout: The maximum waiting time in seconds, defaults to 3s.
                Note that the batch triggers when either batch_size or
                timeout_in_seconds is reached.
            name: operator name
            ray_serve_enabled: Whether to enable Ray Serve for this operator.
            async_buffer_size: The size of the async buffer, defaults to None.

        Returns:
            A new :class:`DataStream` transformed by the given MapFunction.

        .. seealso::

            :meth:`~DataStream.flat_map`
                Call this method to create new rows from existing ones. Unlike
                :meth:`~DataStream.map`, a function passed to
                :meth:`~DataStream.flat_map` can return multiple rows.

            :meth:`~DataStream.map_batches`
                Call this method to transform batches of data.
        """
        resources = Resources(num_cpus, num_gpus, concurrency)
        return self._transform(
            MapOperator(
                LogicalFunction(
                    fn,
                    fn_constructor_args=fn_constructor_args,
                    fn_constructor_kwargs=fn_constructor_kwargs,
                    lowering=lower_map,
                    resources=resources,
                    batch_size=batch_size,
                    batch_timeout=int(batch_timeout.total_seconds()),
                    async_buffer_size=async_buffer_size,
                ),
            ),
            name or "Map",
            resources,
            ray_serve_enabled=ray_serve_enabled,
        )

    @PublicAPI
    def map_batches(
        self,
        fn: UserDefinedFunction,
        *,
        fn_constructor_args: Iterable[Any] | None = None,
        fn_constructor_kwargs: dict[str, Any] | None = None,
        num_cpus: float | None = None,
        num_gpus: float | None = None,
        concurrency: int | tuple[int, int] | None = None,
        batch_size: int = 1,
        batch_timeout: timedelta = timedelta(seconds=3),
        batch_format: str | None = "default",
        name: str | None = None,
        ray_serve_enabled: bool = False,
        async_buffer_size: int | None = None,
    ) -> "DataStream":
        """
        Applies a MapBatches transformation on a :class:`DataStream`.

        Examples:

            Call :meth:`~DataStream.map_batches` to transform your data.

            .. testcode::

                from ray.klein.api.klein_context import KleinContext
                import numpy as np

                def add_dog_years(batch: dict[str, np.ndarray]) -> dict[str, np.ndarray]:
                    batch["age_in_dog_years"] = 7 * batch["age"]
                    return batch

                ctx = KleinContext()
                ctx.from_items([
                    {"name": "Luna", "age": 4},
                    {"name": "Rory", "age": 14},
                    {"name": "Scout", "age": 9},
                ]).map_batches(add_dog_years).show()
                ctx.execute("demo").wait()

        Args:
            fn: The function or generator to apply to a record/batch, or a class type
                that can be instantiated to create such a callable. Note ``fn`` must be
                pickle-able.
            fn_constructor_args: Positional arguments to pass to ``fn``'s constructor.
                You can only provide this if ``fn`` is a callable class. These arguments
                are top-level arguments in the underlying Ray actor construction task.
            fn_constructor_kwargs: Keyword arguments to pass to ``fn``'s constructor.
                This can only be provided if ``fn`` is a callable class. These arguments
                are top-level arguments in the underlying Ray actor construction task.
            num_cpus: The number of CPU cores, defaults to 1
            num_gpus: The number of GPU, defaults to 0.
            concurrency: The number of parallelism, defaults to 1
            batch_size: The max number of records for each batch, defaults to 1.
            batch_timeout: The maximum waiting time in seconds, defaults to 3s.
                Note that the batch triggers when either batch_size or
                timeout_in_seconds is reached.
            batch_format: If ``"default"`` or ``"numpy"``, batches are
                ``dict[str, numpy.ndarray]``. If ``"pandas"``, batches are
                ``pandas.DataFrame``.
            name: operator name
            ray_serve_enabled: Whether to enable Ray Serve for this operator.
            async_buffer_size: The size of the async buffer, defaults to None.

        Returns:
            A new :class:`DataStream` transformed by the given MapBatchesFunction.

        .. note::

            The size of the batches provided to ``fn`` might be smaller than the
            specified ``batch_size`` if ``batch_size`` doesn't evenly divide the
            block(s) sent to a given map task.

        .. seealso::

            :meth:`~DataStream.flat_map`
                Call this method to create new records from existing ones. Unlike
                :meth:`~DataStream.map`, a function passed to :meth:`~DataStream.flat_map`
                can return multiple records.

            :meth:`~DataStream.map`
                Call this method to transform one record at time.
        """
        resources = Resources(num_cpus, num_gpus, concurrency)
        return self._transform(
            MapOperator(
                LogicalFunction(
                    fn,
                    fn_constructor_args=fn_constructor_args,
                    fn_constructor_kwargs=fn_constructor_kwargs,
                    lowering=lower_map_batches,
                    resources=resources,
                    batch_size=batch_size,
                    batch_format=batch_format,
                    batch_timeout=int(batch_timeout.total_seconds()),
                    async_buffer_size=async_buffer_size,
                ),
            ),
            name or "MapBatches",
            resources,
            ray_serve_enabled=ray_serve_enabled,
        )

    @PublicAPI
    def flat_map(
        self,
        fn: UserDefinedFunction,
        *,
        fn_constructor_args: Iterable[Any] | None = None,
        fn_constructor_kwargs: dict[str, Any] | None = None,
        num_cpus: float | None = None,
        num_gpus: float | None = None,
        concurrency: int | tuple[int, int] | None = None,
        batch_size: int | None = None,
        batch_timeout: timedelta = timedelta(seconds=3),
        name: str | None = None,
        ray_serve_enabled: bool = False,
        async_buffer_size: int | None = None,
    ) -> "DataStream":
        """
        Applies a FlatMap transformation on a :class:`DataStream`.

        You can emit any number of elements including none.

        .. tip::
            :meth:`~DataStream.map_batches` can also modify the number of rows. If your
            transformation is vectorized like most NumPy and pandas operations,
            it might be faster.

        Examples:

            Return a list contains any number of elements.

            .. testcode::

                from ray.klein.api.klein_context import KleinContext

                def duplicate_row(row: dict) -> list[dict]:
                    return [row] * 2

                ctx = KleinContext()
                ctx.from_items([
                    {"col": 4}, {"col": 14}, { "col": 9}
                ]).flat_map(duplicate_row).show()
                ctx.execute("demo").wait()

            Return elements by yield.

            .. testcode::

                def duplicate_row(row: dict):
                    for item in [row] * 2:
                        yield item

                ctx = KleinContext()
                ctx.from_items([
                    {"col": 4}, {"col": 14}, { "col": 9}
                ]).flat_map(duplicate_row).show()
                ctx.execute("demo").wait()

        Args:
            fn: The function or generator to apply to a record/batch, or a class type
                that can be instantiated to create such a callable. Note ``fn`` must be
                pickle-able.
            fn_constructor_args: Positional arguments to pass to ``fn``'s constructor.
                You can only provide this if ``fn`` is a callable class. These arguments
                are top-level arguments in the underlying Ray actor construction task.
            fn_constructor_kwargs: Keyword arguments to pass to ``fn``'s constructor.
                This can only be provided if ``fn`` is a callable class. These arguments
                are top-level arguments in the underlying Ray actor construction task.
            num_cpus: The number of CPU cores, defaults to 1
            num_gpus: The number of GPU, defaults to 0.
            concurrency: The number of parallelism, defaults to 1
            batch_size: The max number of records for each batch, defaults to None.
            batch_timeout: The maximum waiting time in seconds, defaults to 3s.
                Note that the batch triggers when either batch_size or
                timeout_in_seconds is reached.
            name: operator name
            ray_serve_enabled: Whether to enable Ray Serve for this operator.
            async_buffer_size: The size of the async buffer, defaults to None.

        Returns:
            The transformed :class:`DataStream`.

        .. seealso::

            :meth:`~DataStream.map_batches`
                Call this method to transform batches of data.

            :meth:`~DataStream.map`
                Call this method to transform one row at time.
        """
        resources = Resources(num_cpus, num_gpus, concurrency)
        return self._transform(
            FlatMapOperator(
                LogicalFunction(
                    fn,
                    fn_constructor_args=fn_constructor_args,
                    fn_constructor_kwargs=fn_constructor_kwargs,
                    lowering=lower_flat_map,
                    resources=resources,
                    batch_size=batch_size,
                    batch_timeout=int(batch_timeout.total_seconds()),
                    async_buffer_size=async_buffer_size,
                ),
            ),
            name or "FlatMap",
            resources,
            ray_serve_enabled=ray_serve_enabled,
        )

    @PublicAPI
    def map_reduce(
        self,
        key_selector: Callable[[Any], Any],
        preprocess_fn: UserDefinedFunction,
        batch_process_fn: UserDefinedFunction,
        postprocess_fn: UserDefinedFunction = _identity,
        *,
        preprocess_fn_constructor_args: Iterable[Any] | None = None,
        preprocess_fn_constructor_kwargs: dict[str, Any] | None = None,
        batch_process_fn_constructor_args: Iterable[Any] | None = None,
        batch_process_fn_constructor_kwargs: dict[str, Any] | None = None,
        postprocess_fn_constructor_args: Iterable[Any] | None = None,
        postprocess_fn_constructor_kwargs: dict[str, Any] | None = None,
        num_cpus: tuple[float, float, float] = (1.0, 1.0, 1.0),
        num_gpus: tuple[float, float, float] = (0.0, 0.0, 0.0),
        concurrency: tuple[int, int, int] = (1, 1, 1),
        preprocess_missing_data_strategy: MissingDataStrategy = MissingDataStrategy.ERROR,
        batch_process_size: int = 1,
        batch_process_timeout: timedelta = timedelta(seconds=3),
        batch_process_format: str | None = "default",
        name: str | None = None,
    ) -> "DataStream":
        """
        Applies three transformations on a :class:`DataStream`:
        FlatMap-like preprocessing --Adaptive--> batch processing --KeyBy--> Reduce-like postprocessing

        .. tip::

            通过 ``preprocess_fn`` 将数据拆分后，数据将自动交由 ``batch_process_fn`` 处理，并在 ``postprocess_fn`` 实现数据的
            后处理和聚合。本方法主要用于 **用户处理数据的粒度与推理的粒度不同** 情况时的数据，以简化用户编程。

        Examples:

            Call :meth:`~DataStream.map_reduce` to transform your data.
            The following is an example using the map-reduce API:

            1. 定义 ``map_reduce`` 所需的处理函数

            .. testcode::

                def key_selector(data: Any) -> Any:
                    return data.get("note_id")

                def pre_processor(data):
                    for x in data["comment_list"]:
                        data = {
                            "note_id": data["note_id"],
                            "comment_input_id": [random.randint(1, 1000) for _ in range(len(x))],
                        }
                        yield data

                def batch_process(data):
                    return {
                        "note_id": data["note_id"],
                        "comment_embeddings": [
                            [id / 1000 for id in ids] for ids in data["comment_input_id"]
                        ],
                    }

                def postprocess_fn(data):
                    data['comment_embeddings'] = [
                        [-value for value in sublist] for sublist in data['comment_embeddings']
                    ]
                    return data

            2. 初始化 ``DataStream`` 并使用假数据作为输入

            .. testcode::

                ctx = KleinContext()
                stream = ctx.from_values(
                    {
                        "note_id": "111",
                        "comment_list": ["可以的", "小猫好乖啊", "好可爱的小猫", "给姨姨吸吸"],
                    },
                    {"note_id": "222", "comment_list": ["好漂亮的花瓶", "哪里买的呀"]})
                stream.show()

            .. testoutput::

                {'note_id': '111', 'comment_list': ['可以的', '小猫好乖啊', '好可爱的小猫', '给姨姨吸吸']}
                {'note_id': '222', 'comment_list': ['好漂亮的花瓶', '哪里买的呀']}

            3. 调用 ``map_reduce`` 并展示处理结果

            .. testcode::

                stream = stream.map_reduce(
                    key_selector=key_selector,
                    preprocess_fn=pre_processor,
                    batch_process_fn=batch_process,
                    postprocess_fn=postprocess_fn,
                    concurrency=(2, 2, 2),
                    process_batch_size=3,
                )
                stream.write(ConsoleSinkFunction, num_cpus=0.1)
                ctx.execute("test_map_reduce").wait()

            .. testoutput::

                {'note_id': ['111', '111', '111', '111'],
                    'comment_embeddings': [[0.502, 0.5, 0.496], [0.18, 0.372, 0.862, 0.158, 0.525],
                        [0.876, 0.257, 0.753, 0.724, 0.135, 0.308], [0.765, 0.826, 0.635, 0.478, 0.568]]}
                {'note_id': ['222', '222'],
                    'comment_embeddings': [[0.921, 0.289, 0.198, 0.919, 0.995, 0.494],
                        [0.486, 0.012, 0.285, 0.037, 0.689]]}

        Args:
            key_selector: A function that takes keys and returns the value. The key here is used to send data with
                the same key to the same operator and aggregate them on the reduce side.
            preprocess_fn: A function that expand the data and does pre-processing to the expanded data.
                Expansion here means splitting the data of a certain dimension, generating new data and returning it.
                For example, convert record from
                ``{'note_id': '222', 'comment_list': ['好漂亮的花瓶', '哪里买的呀']}`` to
                ``{'note_id': '222', 'comment_input_id': [612, 542, 159, 242, 13, 44]}`` and
                ``{'note_id': '222', 'comment_input_id': [761, 782, 663, 383, 395]}``.
                For the specific implementation, you can refer to the ``pre_processor`` function in the example above.
            batch_process_fn: A function for batch data processing. You can use the batch processing method here, and
                the system will automatically batch the data and pass it to ``batch_process_fn``. Taking the *val*
                mentioned in ``preprocess_fn`` as an example, you can directly process the data of the *val* column in
                the ``batch_process_fn`` in the batch data structure, where the structure is specified by the parameter
                ``process_batch_format``. In addition, you can set the batch processing parameters through
                ``process_batch_size`` and ``process_batch_timeout``.
            postprocess_fn: A function for post-processing aggregated data, as shown in the sample code above, you can
                use postprocess_fn to negate the result calculated by `batch_process_fn`.
            preprocess_fn_constructor_args:
                args of pre-processing function, used to initialize the user-defined ``preprocess_fn``
            preprocess_fn_constructor_kwargs:
                kwargs of pre-processing function, used to initialize the user-defined ``preprocess_fn``
            batch_process_fn_constructor_args:
                args of processing function, used to initialize the user-defined ``batch_process_fn``
            batch_process_fn_constructor_kwargs:
                kwargs of processing function, used to initialize the user-defined ``batch_process_fn``
            postprocess_fn_constructor_args:
                args of post-processing function, used to initialize the user-defined ``postprocess_fn``
            postprocess_fn_constructor_kwargs:
                kwargs of post-processing function, used to initialize the user-defined ``postprocess_fn``
            num_cpus: The number of CPU cores of each operator, all defaults to 1
            num_gpus: The number of GPU of each operator, all defaults to 0.
            concurrency: The number of parallelism of each operator, all defaults to 1.
            preprocess_missing_data_strategy: The strategy for missing data during pre-processing.
                `MissingDataStrategy.IGNORE` will omit the missing data, `MissingDataStrategy.WARNING`
                will print a warning message, and `MissingDataStrategy.ERROR` will directly report an error.
            batch_process_size: The max number of records for each batch, defaults to None.
            batch_process_timeout: The maximum waiting time in seconds, defaults to 3s.
            batch_process_format: If ``"default"`` or ``"numpy"``, batches are
                ``dict[str, numpy.ndarray]``. If ``"pandas"``, batches are
                ``pandas.DataFrame``.
            name: Operator name.

        Returns:
            The transformed DataStream

        .. seealso::

            :meth:`~DataStream.flat_map`
                Call this function to convert one piece of data into multiple pieces.

            :meth:`~DataStream.map_batches`
                Call this function to process data in batches.

            :meth:`~DataStream.map`
                Call this method to transform one record at time.
        """
        preprocess_resources = Resources(num_cpus[0], num_gpus[0], concurrency[0])
        preprocess = DataStream(
            self,
            FlatMapWithRankOperator(
                LogicalFunction(
                    preprocess_fn,
                    fn_constructor_args=preprocess_fn_constructor_args,
                    fn_constructor_kwargs=preprocess_fn_constructor_kwargs,
                    lowering=lower_flat_map,
                    resources=preprocess_resources,
                ),
                missing_data_strategy=preprocess_missing_data_strategy,
            ),
            (name + "-FlatMap") if name else "Inner-FlatMap",
            NodeType.TRANSFORM,
            resources=preprocess_resources,
        )
        preprocess.adaptive_shuffle()
        process_resources = Resources(num_cpus[1], num_gpus[1], concurrency[1])
        process = DataStream(
            preprocess,
            BatchProcessOperator(
                LogicalFunction(
                    batch_process_fn,
                    fn_constructor_args=batch_process_fn_constructor_args,
                    fn_constructor_kwargs=batch_process_fn_constructor_kwargs,
                    lowering=lower_map,
                    resources=process_resources,
                    batch_size=batch_process_size,
                    batch_timeout=int(batch_process_timeout.total_seconds()),
                    batch_format=batch_process_format,
                ),
            ),
            (name + "-MapBatches") if name else "Inner-MapBatches",
            NodeType.TRANSFORM,
            resources=process_resources,
        )
        process.partition_by(KeyPartitioner(key_selector=key_selector))
        return DataStream(
            process,
            ReduceOperator(
                LogicalFunction(
                    postprocess_fn,
                    fn_constructor_args=postprocess_fn_constructor_args,
                    fn_constructor_kwargs=postprocess_fn_constructor_kwargs,
                ),
                key_selector,
            ),
            (name + "-Reduce") if name else "Inner-Reduce",
            NodeType.TRANSFORM,
            resources=Resources(num_cpus[2], num_gpus[2], concurrency[2]),
        )

    @PublicAPI
    def filter(
        self,
        fn: UserDefinedFunction[dict[str, Any], bool | list[bool]],
        *,
        fn_constructor_args: Iterable[Any] | None = None,
        fn_constructor_kwargs: dict[str, Any] | None = None,
        num_cpus: float | None = None,
        num_gpus: float | None = None,
        concurrency: int | tuple[int, int] | None = None,
        batch_size: int | None = None,
        batch_timeout: timedelta = timedelta(seconds=3),
        name: str | None = None,
        ray_serve_enabled: bool = False,
        async_buffer_size: int | None = None,
    ) -> "DataStream":
        """
        Applies a Filter transformation on a :class:`DataStream`.
        DataStream and retains only those element for which the function
        returns True.

        Args:
            fn: The function or generator to apply to a record/batch, or a class type
                that can be instantiated to create such a callable. Note ``fn`` must be
                pickle-able.
            fn_constructor_args: Positional arguments to pass to ``fn``'s constructor.
                You can only provide this if ``fn`` is a callable class. These arguments
                are top-level arguments in the underlying Ray actor construction task.
            fn_constructor_kwargs: Keyword arguments to pass to ``fn``'s constructor.
                This can only be provided if ``fn`` is a callable class. These arguments
                are top-level arguments in the underlying Ray actor construction task.
            num_cpus: The number of CPU cores, defaults to 1
            num_gpus: The number of GPU, defaults to 0.
            concurrency: The number of parallelism, defaults to 1
            batch_size: The max number of records for each batch, defaults to None.
            batch_timeout: The maximum waiting time in seconds, defaults to 3s.
                Note that the batch triggers when either batch_size or
                timeout_in_seconds is reached.
            name: operator name
            ray_serve_enabled: Whether to enable Ray Serve for this operator.
            async_buffer_size: The size of the async buffer, defaults to None.

        Returns:
            The filtered DataStream
        """
        resources = Resources(num_cpus, num_gpus, concurrency)
        return self._transform(
            FilterOperator(
                LogicalFunction(
                    fn,
                    fn_constructor_args=fn_constructor_args,
                    fn_constructor_kwargs=fn_constructor_kwargs,
                    lowering=lower_filter,
                    resources=resources,
                    batch_size=batch_size,
                    batch_timeout=int(batch_timeout.total_seconds()),
                    async_buffer_size=async_buffer_size,
                ),
            ),
            name or "Filter",
            resources,
            ray_serve_enabled=ray_serve_enabled,
        )

    # ------------------------------------------------------------------
    #  Graph merges and repartitioning: union, group_by, broadcast, rescale, round_robin, adaptive_shuffle, partition_by
    # ------------------------------------------------------------------

    def union(self, *streams: "DataStream") -> "DataStream":
        """Apply union transformations to this stream by merging data stream
         outputs of the same type with each other.

        Args:
            *streams: The DataStreams to union output with.

        Returns:
            A new UnionStream.
        """

        def union_fn(_record: dict[str, Any]) -> None:
            raise RuntimeError("map-reduce pipeline construction reached an invalid state")

        input_streams = [self, *streams]
        return UnionStream(
            input_streams,
            UnionOperator(
                LogicalFunction(
                    union_fn,
                    lowering=lower_union,
                )
            ),
        )

    @PublicAPI
    def assign_timestamps_and_watermarks(self, strategy: "WatermarkStrategy") -> "DataStream":
        """Assign event timestamps and emit ordered Watermark/Idle/Active controls."""

        from ray.klein.api.watermark_strategy import WatermarkStrategy
        from ray.klein.runtime.operator.watermark_operator import WatermarkOperator

        if not isinstance(strategy, WatermarkStrategy):
            raise TypeError("strategy must be a WatermarkStrategy")
        return self._transform(
            WatermarkOperator(strategy=strategy),
            "Watermarks",
            self.resources,
        )

    @PublicAPI
    def key_by(self, fn: Callable[[dict[str, Any]], Any]) -> "KeyedStream":
        """Hash-partition this branch and expose managed keyed operations."""

        from ray.klein.api.keyed_stream import KeyedStream
        from ray.klein.runtime.operator.key_by_operator import KeyByOperator

        branch = self._transform(
            KeyByOperator(),
            "KeyBy",
            self.resources,
        )
        branch.partition_by(KeyPartitioner(key_selector=fn))
        return KeyedStream(branch, fn)

    @PublicAPI
    def group_by(self, fn: Callable[[dict[str, Any]], Any]) -> "KeyedStream":
        """
        Creates a new :class:`KeyedStream` that uses the provided key to
        partition data stream by key.

        Args:
            fn: The KeyFunction that is used for extracting the key for
                partitioning. If `fn` is a python function instead of a subclass
                of KeyFunction, it will be wrapped as SimpleKeyFunction.

        Returns:
             A KeyedStream
        """
        return self.key_by(fn)

    @PublicAPI
    def join(
        self,
        other: "DataStream",
        *,
        left_key: Callable[[dict[str, Any]], Any],
        right_key: Callable[[dict[str, Any]], Any],
        left_timestamp: Callable[[dict[str, Any]], int],
        right_timestamp: Callable[[dict[str, Any]], int],
        lower_bound: timedelta,
        upper_bound: timedelta,
        join_function: Callable[[dict[str, Any], dict[str, Any]], Any],
        allowed_lateness: timedelta = timedelta(0),
        state_ttl: timedelta | None = None,
        num_cpus: float | None = None,
        num_gpus: float | None = None,
        concurrency: int | None = None,
        name: str = "IntervalJoin",
    ) -> "DataStream":
        """Create a keyed event-time interval join over two streaming branches."""

        from ray.klein.runtime.operator.input_tag_operator import InputTagOperator
        from ray.klein.runtime.operator.interval_join_operator import (
            IntervalJoinOperator,
        )

        left_branch = self._transform(
            InputTagOperator(input_tag=0),
            "JoinLeft",
            self.resources,
        )
        right_branch = other._transform(
            InputTagOperator(input_tag=1),
            "JoinRight",
            other.resources,
        )
        left_branch.partition_by(KeyPartitioner(key_selector=left_key))
        right_branch.partition_by(KeyPartitioner(key_selector=right_key))
        resources = Resources(num_cpus, num_gpus, concurrency)
        return DataStream(
            [left_branch, right_branch],
            IntervalJoinOperator(
                left_key=left_key,
                right_key=right_key,
                left_timestamp=left_timestamp,
                right_timestamp=right_timestamp,
                lower_bound=lower_bound,
                upper_bound=upper_bound,
                join_function=join_function,
                allowed_lateness=allowed_lateness,
                state_ttl=state_ttl,
            ),
            name,
            NodeType.TRANSFORM,
            resources=resources,
        )

    interval_join = join

    def broadcast(self) -> "DataStream":
        """
        Sets the partitioning of the :class:`DataStream` so that the output
        elements are broadcast to every parallel instance of the next
        operation.

        Returns:
            The DataStream with broadcast partitioning set.
        """
        self.partitioner = BroadcastPartitioner()
        return self

    def rescale(self) -> "DataStream":
        """
        Sets the partitioning of the :class:`DataStream` so that the output
        elements are rescaled to parallel instance of the next
        operation.

        Returns:
            The DataStream with rescale partitioning set.
        """
        self.partitioner = RescalePartitioner()
        return self

    def round_robin(self) -> "DataStream":
        """
        Sets the partitioning of the :class:`DataStream` so that the output
        elements are round-robin to parallel instance of the next
        operation.

        Returns:
            The DataStream with round-robin partitioning set.
        """
        self.partitioner = RoundRobinPartitioner()
        return self

    def adaptive_shuffle(self) -> "DataStream":
        """
        Sets the partitioning of the :class:`DataStream` so that the output
        elements are adaptive shuffle to parallel instance of the next
        operation.

        Returns:
            The DataStream with adaptive partitioning set.
        """
        self.partitioner = AdaptivePartitioner()
        return self

    def partition_by(self, partition_func: Partitioner | Callable) -> "DataStream":
        """
        Sets the partitioning of the :class:`DataStream` so that the elements
        of stream are partitioned by specified partition function.

        Args:
            partition_func: partition function. If `func` is a python function instead of a subclass of Partition,
                it will be wrapped as SimplePartition.

        Returns:
            The DataStream with specified partitioning set.
        """
        if isinstance(partition_func, Partitioner):
            self.partitioner = partition_func
        elif callable(partition_func):
            self.partitioner = SimplePartitioner(partition_func)
        else:
            raise TypeError("partition_func must be a Partitioner or callable")
        return self

    @PublicAPI

    # ------------------------------------------------------------------
    #  Diagnostics: show, take_all, take, schema
    # ------------------------------------------------------------------

    def show(
        self,
        limit: int = 20,
        num_cpus: float | None = None,
        concurrency: int | None = None,
        batch_size: int | None = None,
        batch_timeout: timedelta = timedelta(seconds=3),
        name: str | None = None,
    ) -> "StreamSink | None":
        """Print up to the given number of rows from the :class:`DataStream`.

        Args:
            limit: The maximum number of rows to show. Note that limit has no effect in streaming mode.
            num_cpus: The number of CPU cores, defaults to 1.
            concurrency: The number of parallelism, defaults to 1
            batch_size: The max number of records for each batch, defaults to None.
            batch_timeout: The maximum waiting time in seconds, defaults to 3s.
            name: The name of show operator, defaults to "Show".

        Returns:
            List of records read into the driver.
        """
        return self.write(
            ConsoleSinkFunction,
            fn_constructor_kwargs={"limit": limit},
            lowering=RayDataCall.dataset_method(
                "show",
                (),
                {"limit": limit},
                expects_dataset=False,
            ),
            num_cpus=num_cpus,
            concurrency=concurrency,
            batch_size=batch_size,
            batch_timeout=batch_timeout,
            name=name if name is not None else "Show",
        )

    @PublicAPI
    def take_all(self, limit: int | None = None) -> list[dict[str, Any]]:
        """Return all the rows in this :class:`DataStream`.

        Args:
            limit: Raise an error if the size exceeds the specified limit.

        Returns:
            List of records read into the driver.
        """
        if not self.context.interactive_mode_enabled:
            raise RuntimeError(ChineseMessages.SET_INTERACTIVE_MODE_FOR_TAKE)
        return self.write(
            CollectFunction,
            fn_constructor_kwargs={"limit": limit},
            lowering=RayDataCall.dataset_method(
                "take_all",
                (),
                {"limit": limit},
                expects_dataset=False,
            ),
            concurrency=1,
            node_type=NodeType.TAKE,
            name="TakeAll",
        )

    @PublicAPI
    def take(self, limit: int = 20) -> list[dict[str, Any]]:
        """Return up to ``limit`` rows from the :class:`DataStream`.

        Args:
            limit: The maximum number of rows to return.

        Returns:
            List of records read into the driver.
        """
        if not self.context.interactive_mode_enabled:
            raise RuntimeError(ChineseMessages.SET_INTERACTIVE_MODE_FOR_TAKE)
        return self.write(
            CollectFunction,
            fn_constructor_kwargs={"limit": limit},
            lowering=RayDataCall.dataset_method(
                "take",
                (),
                {"limit": limit},
                expects_dataset=False,
            ),
            concurrency=1,
            node_type=NodeType.TAKE,
            name="Take",
        )

    @PublicAPI
    def schema(self, fetch_if_missing: bool = True) -> Any:
        """Return the schema of the datastream.

        Args:
            fetch_if_missing: If True, synchronously fetch the schema if it's
                not known. If False, None is returned if the schema is not known.
                Default is True.

        Returns:
            :class:`StreamSink`.
        """
        return self.write(
            ConsoleSinkFunction,
            lowering=RayDataCall.dataset_method(
                "schema",
                (),
                {"fetch_if_missing": fetch_if_missing},
                expects_dataset=False,
            ),
            name="Schema",
        )

    @PublicAPI
    # ------------------------------------------------------------------
    #  Klein-native sinks: files, Kafka, Redis, and custom SinkFunction
    # ------------------------------------------------------------------

    @PublicAPI
    def write_files(
        self,
        path: str,
        data_format: str,
        *,
        columns: Iterable[str] | None = None,
        storage_options: dict[str, Any] | None = None,
        filename_prefix: str = "part",
        max_rows_per_file: int | None = None,
        max_bytes_per_file: int | None = None,
        rollover_interval: timedelta | None = None,
        inactivity_interval: timedelta | None = None,
        ray_remote_args: dict[str, Any] | None = None,
        concurrency: int | None = None,
        ray_data_options: Mapping[str, Any] | None = None,
    ) -> "StreamSink":
        """Write a stream to checkpoint-transactional part files.

        Streaming execution keeps files private until their checkpoint metadata
        is durable, then publishes every part idempotently. JSON, CSV, Parquet,
        and single-column text are supported. Batch execution delegates JSON,
        CSV, and Parquet writes to the matching public Ray Data API.
        """

        normalized_format = data_format.strip().lower()
        supported_formats = frozenset({"csv", "json", "parquet", "text"})
        if normalized_format not in supported_formats:
            raise ValueError(f"data_format must be one of {sorted(supported_formats)}, got {data_format!r}")
        batch_lowering = None
        if normalized_format != "text":
            batch_options = dict(ray_data_options or {})
            if ray_remote_args is not None:
                batch_options.setdefault("ray_remote_args", ray_remote_args)
            if concurrency is not None:
                batch_options.setdefault("concurrency", concurrency)
            batch_lowering = RayDataCall.dataset_method(
                f"write_{normalized_format}",
                (path,),
                batch_options,
                expects_dataset=False,
            )
        remote_args = ray_remote_args or {}
        return self.write(
            StreamingFileSink,
            fn_constructor_args=[path, normalized_format],
            fn_constructor_kwargs={
                "columns": tuple(columns) if columns is not None else None,
                "storage_options": storage_options,
                "filename_prefix": filename_prefix,
                "max_rows_per_file": max_rows_per_file,
                "max_bytes_per_file": max_bytes_per_file,
                "rollover_interval_seconds": (
                    rollover_interval.total_seconds() if rollover_interval is not None else None
                ),
                "inactivity_interval_seconds": (
                    inactivity_interval.total_seconds() if inactivity_interval is not None else None
                ),
            },
            lowering=batch_lowering,
            num_cpus=remote_args.get("num_cpus"),
            num_gpus=remote_args.get("num_gpus"),
            concurrency=concurrency,
            name=f"{normalized_format.title()}FileSink",
        )

    @PublicAPI
    def write_json(self, path: str, **options: Any) -> "StreamSink":
        """Write newline-delimited JSON files in batch or streaming mode."""

        return self.write_files(path, "json", **options)

    @PublicAPI
    def write_csv(self, path: str, **options: Any) -> "StreamSink":
        """Write CSV files in batch or streaming mode."""

        return self.write_files(path, "csv", **options)

    @PublicAPI
    def write_parquet(self, path: str, **options: Any) -> "StreamSink":
        """Write Parquet files in batch or streaming mode."""

        return self.write_files(path, "parquet", **options)

    @PublicAPI
    def write_text(self, path: str, **options: Any) -> "StreamSink":
        """Write one-column UTF-8 text files in streaming mode."""

        return self.write_files(path, "text", **options)

    def write_kafka(
        self,
        topic: str,
        bootstrap_servers: str,
        key_field: str | None = None,
        key_serializer: str = "string",
        value_serializer: str = "json",
        producer_config: dict[str, Any] | None = None,
        *,
        ray_remote_args: dict[str, Any] | None = None,
        concurrency: int | None = None,
    ) -> "StreamSink":
        """Write records to Kafka in batch or streaming execution.

        Batch jobs lower to :meth:`ray.data.Dataset.write_kafka`. Streaming
        jobs use a checkpoint-aligned producer and provide at-least-once
        delivery. ``num_cpus`` and ``num_gpus`` in ``ray_remote_args`` are also
        applied to the streaming sink task; other remote arguments are used by
        the Ray Data batch backend only.
        """

        try:
            from ray.klein.integrations.kafka.kafka_sink import KafkaSink
        except ModuleNotFoundError as error:
            if error.name != "confluent_kafka":
                raise
            raise ModuleNotFoundError("Kafka output requires `ray-klein[kafka]`.") from error

        batch_lowering = RayDataCall.dataset_method(
            "write_kafka",
            (topic, bootstrap_servers),
            {
                "key_field": key_field,
                "key_serializer": key_serializer,
                "value_serializer": value_serializer,
                "producer_config": producer_config,
                "ray_remote_args": ray_remote_args,
                "concurrency": concurrency,
            },
            expects_dataset=False,
        )
        remote_args = ray_remote_args or {}
        return self.write(
            KafkaSink,
            fn_constructor_args=[topic, bootstrap_servers],
            fn_constructor_kwargs={
                "key_field": key_field,
                "key_serializer": key_serializer,
                "value_serializer": value_serializer,
                "producer_config": producer_config,
            },
            lowering=batch_lowering,
            num_cpus=remote_args.get("num_cpus"),
            num_gpus=remote_args.get("num_gpus"),
            concurrency=concurrency,
            name="KafkaSink",
        )

    @PublicAPI
    def write_redis(
        self,
        connection: "RedisConnectionConfig",
        *,
        key: Callable[[dict[str, Any]], Any],
        value: Callable[[dict[str, Any]], Any],
        config: "RedisSinkConfig | None" = None,
        num_cpus: float | None = None,
        num_gpus: float | None = None,
        concurrency: int | None = None,
        batch_size: int | None = None,
        batch_timeout: timedelta = timedelta(seconds=3),
        name: str | None = None,
    ) -> "StreamSink":
        """Write records to Redis using extracted keys and values.

        Args:
            connection: Redis endpoint, pool, timeout, and retry settings.
            key: Function extracting the Redis key from one record.
            value: Function extracting the Redis value from one record.
            config: Redis data shape, key namespace, TTL, and buffering policy.
            num_cpus: The number of CPU cores, defaults to 1.
            num_gpus: The number of GPU, defaults to 0.
            concurrency: The number of parallel sink tasks, defaults to 1.
            batch_size: The max number of records for each batch, defaults to None.
            batch_timeout: The maximum waiting time in seconds, defaults to 3s.
            name: Operator name.

        Returns:
            :class:`StreamSink`.
        """
        try:
            from ray.klein.integrations.redis.sink import RedisSink
        except ModuleNotFoundError as error:
            if error.name != "redis":
                raise
            raise ModuleNotFoundError("Redis output requires `ray-klein[redis]`.") from error

        return self.write(
            RedisSink,
            fn_constructor_args=[connection, key, value],
            fn_constructor_kwargs={"config": config},
            num_cpus=num_cpus,
            num_gpus=num_gpus,
            concurrency=concurrency,
            batch_size=batch_size,
            batch_timeout=batch_timeout,
            name=name if name is not None else "RedisSink",
        )

    def write(
        self,
        fn: type[SinkFunction],
        *,
        fn_constructor_args: Iterable[Any] | None = None,
        fn_constructor_kwargs: dict[str, Any] | None = None,
        lowering: Callable | None = None,
        num_cpus: float | None = None,
        num_gpus: float | None = None,
        concurrency: int | tuple[int, int] | None = None,
        batch_size: int | None = None,
        batch_timeout: timedelta = timedelta(seconds=3),
        node_type: NodeType | None = None,
        name: str | None = None,
    ) -> Any:
        """
        Create a StreamSink with the given sink function.

        Args:
            fn: The user defined sink function.
            fn_constructor_args: Positional arguments to pass to ``fn``'s constructor.
                You can only provide this if ``fn`` is a callable class. These arguments
                are top-level arguments in the underlying Ray actor construction task.
            fn_constructor_kwargs: Keyword arguments to pass to ``fn``'s constructor.
                This can only be provided if ``fn`` is a callable class. These arguments
                are top-level arguments in the underlying Ray actor construction task.
            lowering: declarative recipe for lowering this sink to a ray.data
                write. ``None`` for streaming-only sinks (no batch backend).
            num_cpus: The number of CPU cores, defaults to 1.
            num_gpus: The number of GPU, defaults to 0.
            concurrency: The number of parallelism, defaults to 1
            batch_size: The max number of records for each batch, defaults to None.
            batch_timeout: The maximum waiting time in seconds, defaults to 3s.
                Note that the batch triggers when either batch_size or
                timeout_in_seconds is reached.
            node_type: Optional :class:`NodeType` to associate with the sink node.
            name: operator name

        Returns:
            :class:`StreamSink`.
        """
        if not isinstance(fn, type) or not issubclass(fn, SinkFunction):
            raise TypeError("fn must be a SinkFunction class")
        data_stream = self
        if self.context.interactive_mode_enabled:
            # In interactive mode, datastream should not be modified, so we copied stream here for execution.
            data_stream = copy.deepcopy(self)

        resources = Resources(num_cpus, num_gpus, concurrency)
        stream_sink = StreamSink(
            data_stream,
            LogicalFunction(
                fn,
                fn_constructor_args=fn_constructor_args,
                fn_constructor_kwargs=fn_constructor_kwargs,
                lowering=lowering,
                resources=resources,
                batch_size=batch_size,
                batch_timeout=int(batch_timeout.total_seconds()),
            ),
            resources=resources,
            node_type=node_type,
            name=name,
        )
        if self.context.interactive_mode_enabled:
            return data_stream.context.execute(name).get()
        return stream_sink


# Imported after DataStream is defined to keep the small stream class modules
# acyclic while preserving runtime ``isinstance`` behaviour.
from ray.klein.api.stream_sink import StreamSink  # noqa: E402
from ray.klein.api.union_stream import UnionStream  # noqa: E402
