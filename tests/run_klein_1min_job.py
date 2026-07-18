# SPDX-License-Identifier: Apache-2.0
"""
一个持续运行 1 分钟的 Klein for Ray 任务示例（复杂拓扑）。

任务管道（fan-in + fan-out，用于演示 JobClient.wait() 的拓扑树/反压视图）::

    LoopSource A ─ Map A ─┐
                          ├─ Union ─ DoubleMap ─┬─ FilterEven ─ ConsoleSink(even)
    LoopSource B ─ Map B ─┘                     └─────────────  ConsoleSink(all)

- 两个 source 不同速率（A 快、B 慢），便于观察 union 的合流与各分支吞吐差异。
- DoubleMap 后扇出到两个 sink：一条经过滤算子，一条直连，演示多 sink。
- 各算子用不同 concurrency，便于观察并行度列与逐实例状态。

任务在主线程运行指定时长后主动调用 ``handle.cancel()`` 结束流式作业。
"""

import os
import time
from time import sleep
from typing import Any

from ray.klein._internal.logging import get_logger
from ray.klein.api.job_handle import JobHandle
from ray.klein.api.klein_context import KleinContext
from ray.klein.api.runtime_context import RuntimeContext
from ray.klein.api.source_context import SourceContext
from ray.klein.api.source_function import SourceFunction
from ray.klein.integrations.console.console_sink import ConsoleSinkFunction

logger = get_logger(__name__)

JOB_DURATION_SECONDS = int(os.getenv("RAY_KLEIN_SOAK_SECONDS", "60"))


class LoopSourceFunction(SourceFunction):
    """持续产生递增 id 数据的 Source。

    ``tag`` 标记数据来自哪个 source，便于在下游/sink 区分两条输入流。
    """

    def __init__(self, sleep_interval: float = 0.1, tag: str = "A"):
        self.idx: int = 0
        self._interrupted: bool = False
        self._sleep_interval = sleep_interval
        self._tag = tag

    def run(self, context: SourceContext) -> None:
        while not self._interrupted:
            self.idx += 1
            context.collect({"idx": self.idx, "tag": self._tag, "ts": time.time()})
            sleep(self._sleep_interval)

    def snapshot_state(self, checkpoint_id: int) -> int:
        return self.idx

    def restore_state(self, state: int) -> None:
        self.idx = state

    def cancel(self) -> None:
        self._interrupted = True


class DoubleMapFunction:
    """把 idx 翻倍的 Map 函数，并故意放慢处理速度。

    每条记录 sleep ``process_delay`` 秒，使本算子的处理速率远低于上游 source
    的产出速率，于是它的 inbox 会被填满 —— 这正是反压（backpressure）：上游的
    ``put`` 因 inbox 满而挂起。用于在 wait() 视图中观察中间算子的反压状态。
    """

    def __init__(self, runtime_context: RuntimeContext = None, process_delay: float = 0.15):
        self.runtime_context = runtime_context
        self._process_delay = process_delay

    def __call__(self, data: dict[str, Any]) -> dict[str, Any]:
        # 慢处理：制造下游瓶颈，使 inbox 堆积、上游被反压。
        sleep(self._process_delay)
        data["idx"] = data["idx"] * 2
        logger.info(
            "DoubleMap[%s/%s] tag=%s -> %s",
            self.runtime_context.task_index,
            self.runtime_context.parallelism,
            data.get("tag"),
            data["idx"],
        )
        return data


class TagMapFunction:
    """给数据打上经过哪个上游 map 的标记（便于追踪 union 前的两条分支）。"""

    def __init__(self, runtime_context: RuntimeContext = None, branch: str = "?"):
        self.runtime_context = runtime_context
        self._branch = branch

    def __call__(self, data: dict[str, Any]) -> dict[str, Any]:
        data["branch"] = self._branch
        return data


class EvenFilterFunction:
    """只保留 idx 为偶数的记录。"""

    def __init__(self, runtime_context: RuntimeContext = None):
        self.runtime_context = runtime_context

    def __call__(self, data: dict[str, Any]) -> bool:
        return data["idx"] % 2 == 0


def main() -> None:
    ctx = KleinContext()

    # 两个不同速率的 source，各接一个打标 map
    branch_a = ctx.source(
        LoopSourceFunction,
        fn_constructor_kwargs={"sleep_interval": 0.05, "tag": "A"},
        num_cpus=0.1,
        concurrency=2,
        bounded=False,
        name="LoopSourceA",
    ).map(
        TagMapFunction,
        fn_constructor_kwargs={"branch": "A"},
        num_cpus=0.1,
        concurrency=2,
        name="TagMapA",
    )

    branch_b = ctx.source(
        LoopSourceFunction,
        fn_constructor_kwargs={"sleep_interval": 0.2, "tag": "B"},
        num_cpus=0.1,
        concurrency=1,
        bounded=False,
        name="LoopSourceB",
    ).map(
        TagMapFunction,
        fn_constructor_kwargs={"branch": "B"},
        num_cpus=0.1,
        concurrency=1,
        name="TagMapB",
    )

    # fan-in：union 两条分支后再翻倍
    doubled = branch_a.union(branch_b).map(
        DoubleMapFunction,
        num_cpus=0.1,
        concurrency=3,
        name="DoubleMap",
    )

    # fan-out：一条经偶数过滤后落 sink，另一条全量落 sink
    doubled.filter(
        EvenFilterFunction,
        num_cpus=0.1,
        concurrency=2,
        name="FilterEven",
    ).write(
        ConsoleSinkFunction,
        num_cpus=0.1,
        concurrency=1,
        name="ConsoleSinkEven",
    )

    doubled.write(
        ConsoleSinkFunction,
        num_cpus=0.1,
        concurrency=2,
        name="ConsoleSinkAll",
    )

    logger.info("Submitting Klein job, will run for %d seconds...", JOB_DURATION_SECONDS)
    client: JobHandle = ctx.execute("KleinOneMinuteJob")

    start = time.time()
    try:
        while time.time() - start < JOB_DURATION_SECONDS:
            elapsed = int(time.time() - start)
            logger.info("Job running... elapsed=%ds / %ds", elapsed, JOB_DURATION_SECONDS)
            sleep(10)
    finally:
        logger.info("Reached %d seconds, cancelling the job...", JOB_DURATION_SECONDS)
        if not client.cancel(timeout=30):
            raise RuntimeError("Klein soak job did not acknowledge cancellation")
        logger.info("Job cancelled with status %s.", client.status.name)


if __name__ == "__main__":
    main()
