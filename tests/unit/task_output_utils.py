from types import SimpleNamespace

from ray.klein.api.runtime_info import RuntimeInfo
from ray.klein.config.configuration import Configuration
from ray.klein.runtime.collector.edge_output import DeliveryMode, EdgeOutput
from ray.klein.runtime.collector.task_output import TaskOutput


def open_task_output(
    targets,
    partitioner,
    control_targets,
    names,
    *,
    max_rows: int = 100,
    put_timeout: float = 1.0,
    namespace: str = "test",
    task_index: int = 0,
    parallelism: int = 1,
    delivery_mode: DeliveryMode = DeliveryMode.INLINE,
    config_values: dict | None = None,
) -> TaskOutput:
    edge = EdgeOutput(
        list(targets),
        partitioner,
        control_targets=tuple(control_targets),
        output_buffer_max_rows=max_rows,
        target_task_names=list(names),
        put_timeout=put_timeout,
        namespace=namespace,
        delivery_mode=delivery_mode,
    )
    output = TaskOutput([edge])
    output.open(
        SimpleNamespace(
            task_name="test-output",
            task_index=task_index,
            parallelism=parallelism,
            config=Configuration(
                {"pipeline.internal.batch-size": 0, **(config_values or {})},
                include_environment=False,
            ),
            metric_group=None,
            runtime_info=RuntimeInfo(),
        )
    )
    return output
