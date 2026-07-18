# SPDX-License-Identifier: Apache-2.0
"""Hierarchical metric groups backed by Ray's native metrics API."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass

from ray.klein.observability.metrics.metric_spec import MetricKind, MetricSpec
from ray.klein.observability.metrics.metrics import Counter, Gauge, Histogram, create_metric

Metric = Counter | Gauge | Histogram


@dataclass(frozen=True, slots=True)
class _MetricRegistration:
    kind: MetricKind
    description: str
    boundaries: tuple[float, ...]
    labels: tuple[tuple[str, str], ...]


class MetricGroup(ABC):
    """A scope that adds stable labels and a prefix to Ray metrics."""

    @abstractmethod
    def metric(self, spec: MetricSpec, tags: dict[str, str] | None = None) -> Metric:
        """Register a built-in metric from the canonical catalogue."""

    def builtin_counter(self, spec: MetricSpec, tags: dict[str, str] | None = None) -> Counter:
        """Register a catalogue counter and enforce its declared kind."""

        if spec.kind is not MetricKind.COUNTER:
            raise ValueError(f"Built-in metric {spec.name!r} is not a counter")
        metric = self.metric(spec, tags)
        if not isinstance(metric, Counter):
            raise TypeError(f"Metric backend returned the wrong type for counter {spec.name!r}")
        return metric

    def builtin_gauge(self, spec: MetricSpec, tags: dict[str, str] | None = None) -> Gauge:
        """Register a catalogue gauge and enforce its declared kind."""

        if spec.kind is not MetricKind.GAUGE:
            raise ValueError(f"Built-in metric {spec.name!r} is not a gauge")
        metric = self.metric(spec, tags)
        if not isinstance(metric, Gauge):
            raise TypeError(f"Metric backend returned the wrong type for gauge {spec.name!r}")
        return metric

    def builtin_histogram(self, spec: MetricSpec, tags: dict[str, str] | None = None) -> Histogram:
        """Register a catalogue histogram and enforce its declared kind."""

        if spec.kind is not MetricKind.HISTOGRAM:
            raise ValueError(f"Built-in metric {spec.name!r} is not a histogram")
        metric = self.metric(spec, tags)
        if not isinstance(metric, Histogram):
            raise TypeError(f"Metric backend returned the wrong type for histogram {spec.name!r}")
        return metric

    @abstractmethod
    def counter(
        self,
        name: str,
        tags: dict[str, str] | None = None,
        description: str = "User-defined Klein counter.",
    ) -> Counter:
        """Register a user-defined counter."""

    @abstractmethod
    def gauge(
        self,
        name: str,
        tags: dict[str, str] | None = None,
        description: str = "User-defined Klein gauge.",
    ) -> Gauge:
        """Register a user-defined gauge."""

    @abstractmethod
    def histogram(
        self,
        name: str,
        boundaries: list[float] | tuple[float, ...],
        tags: dict[str, str] | None = None,
        description: str = "User-defined Klein histogram.",
    ) -> Histogram:
        """Register a user-defined histogram."""

    @abstractmethod
    def add_group(self, group_name: str, labels: dict[str, str] | None = None) -> MetricGroup:
        """Create a child metric group."""

    @property
    @abstractmethod
    def all_labels(self) -> dict[str, str]:
        """Return all inherited labels."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Return the local group name."""

    @abstractmethod
    def metric_identifier(self, metric_name: str) -> str:
        """Return the fully qualified Ray metric name."""


class GenericMetricGroup(MetricGroup):
    def __init__(
        self,
        group_name: str,
        parent: GenericMetricGroup | None = None,
        labels: dict[str, str] | None = None,
        metrics: dict[str, Metric] | None = None,
        sub_groups: dict[str, GenericMetricGroup] | None = None,
        closed: bool = False,
    ) -> None:
        self.group_name = group_name
        self.labels = self._init_labels(parent, labels)
        self.metrics = {} if metrics is None else metrics
        self.parent = parent
        self.sub_groups = {} if sub_groups is None else sub_groups
        self.closed = closed
        self._registrations: dict[str, _MetricRegistration] = {}

    @staticmethod
    def _init_labels(
        parent: GenericMetricGroup | None,
        labels: dict[str, str] | None,
    ) -> dict[str, str]:
        result = {} if parent is None else dict(parent.labels)
        if labels:
            result.update({key: str(value) for key, value in labels.items()})
        return result

    @staticmethod
    def _concat_group_name(group: GenericMetricGroup) -> str:
        if group.parent is None:
            return group.group_name
        return f"{GenericMetricGroup._concat_group_name(group.parent)}_{group.group_name}"

    def _add_metric(
        self,
        name: str,
        kind: MetricKind,
        description: str,
        tags: dict[str, str] | None,
        boundaries: tuple[float, ...] = (),
    ) -> Metric:
        if not isinstance(name, str) or not name.strip():
            raise ValueError("metric name cannot be blank")
        if kind is MetricKind.HISTOGRAM and (not boundaries or tuple(sorted(set(boundaries))) != boundaries):
            raise ValueError("histogram boundaries must be unique and increasing")
        if self.closed:
            raise RuntimeError(f"Group {self.group_name} has already been closed.")
        if name in self.sub_groups:
            raise ValueError(f"Name collision: group {self.group_name!r} already contains subgroup {name!r}.")
        labels = dict(self.all_labels)
        if tags:
            labels.update({key: str(value) for key, value in tags.items()})
        registration = _MetricRegistration(
            kind=kind,
            description=description,
            boundaries=boundaries,
            labels=tuple(sorted(labels.items())),
        )
        existing = self.metrics.get(name)
        if existing is not None:
            if self._registrations.get(name) != registration:
                raise ValueError(
                    f"Metric {self.metric_identifier(name)!r} was registered with a different "
                    "type, description, boundaries, or label set."
                )
            return existing

        metric = create_metric(
            kind,
            self.metric_identifier(name),
            labels,
            boundaries,
            description,
        )
        self.metrics[name] = metric
        self._registrations[name] = registration
        return metric

    def metric(self, spec: MetricSpec, tags: dict[str, str] | None = None) -> Metric:
        return self._add_metric(
            spec.name,
            spec.kind,
            spec.description,
            tags,
            spec.boundaries,
        )

    def counter(
        self,
        name: str,
        tags: dict[str, str] | None = None,
        description: str = "User-defined Klein counter.",
    ) -> Counter:
        metric = self._add_metric(name, MetricKind.COUNTER, description, tags)
        if not isinstance(metric, Counter):
            raise AssertionError("counter registration returned a non-counter metric")
        return metric

    def gauge(
        self,
        name: str,
        tags: dict[str, str] | None = None,
        description: str = "User-defined Klein gauge.",
    ) -> Gauge:
        metric = self._add_metric(name, MetricKind.GAUGE, description, tags)
        if not isinstance(metric, Gauge):
            raise AssertionError("gauge registration returned a non-gauge metric")
        return metric

    def histogram(
        self,
        name: str,
        boundaries: list[float] | tuple[float, ...],
        tags: dict[str, str] | None = None,
        description: str = "User-defined Klein histogram.",
    ) -> Histogram:
        metric = self._add_metric(
            name,
            MetricKind.HISTOGRAM,
            description,
            tags,
            tuple(boundaries),
        )
        if not isinstance(metric, Histogram):
            raise AssertionError("histogram registration returned a non-histogram metric")
        return metric

    def add_group(self, group_name: str, labels: dict[str, str] | None = None) -> MetricGroup:
        if not isinstance(group_name, str) or not group_name.strip():
            raise ValueError("metric group name cannot be blank")
        if self.closed:
            raise RuntimeError(f"Group {self.name!r} has been closed.")
        existing = self.sub_groups.get(group_name)
        if existing is not None:
            expected = self._init_labels(self, labels)
            if existing.labels != expected:
                raise ValueError(f"Metric subgroup {group_name!r} was registered with different labels.")
            return existing
        if group_name in self.metrics:
            raise ValueError(f"Name collision: group {self.name!r} already contains metric {group_name!r}.")
        group = GenericMetricGroup(group_name=group_name, parent=self, labels=labels)
        self.sub_groups[group_name] = group
        return group

    @property
    def all_labels(self) -> dict[str, str]:
        return dict(self.labels)

    @property
    def name(self) -> str:
        return self.group_name

    def metric_identifier(self, metric_name: str) -> str:
        return f"{self._concat_group_name(self)}_{metric_name}"

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        for group in self.sub_groups.values():
            group.close()


class JobManagerMetricGroup(GenericMetricGroup):
    """Metric group scoped to a job manager."""


class JobMetricGroup(GenericMetricGroup):
    def __init__(self, job_name: str, job_id: str | None = None) -> None:
        super().__init__(
            group_name="ray_klein_job",
            labels={"job_name": job_name, "job_id": job_id or job_name},
        )
        self.job_name = job_name
        self.job_id = job_id or job_name
        self.task_groups: dict[tuple[str, int], TaskMetricGroup] = {}

    def add_task_group(self, task_id: str, task_name: str, subtask_index: int) -> TaskMetricGroup:
        key = (task_id, subtask_index)
        existing = self.task_groups.get(key)
        if existing is not None:
            if existing.task_name != task_name:
                raise ValueError(f"Task metric group {key!r} was registered with a different name.")
            return existing
        group = TaskMetricGroup(self, task_id, task_name, subtask_index)
        self.task_groups[key] = group
        return group

    def task_group(self, task_id: str, subtask_index: int | None = None) -> TaskMetricGroup:
        if subtask_index is not None:
            return self.task_groups[task_id, subtask_index]
        matches = [group for (candidate, _), group in self.task_groups.items() if candidate == task_id]
        if len(matches) != 1:
            raise ValueError(f"Task id {task_id!r} maps to {len(matches)} metric groups; specify subtask_index.")
        return matches[0]


class TaskMetricGroup(GenericMetricGroup):
    def __init__(
        self,
        parent: JobMetricGroup | None,
        task_id: str,
        task_name: str,
        subtask_index: int,
    ) -> None:
        super().__init__(
            group_name="ray_klein_task",
            parent=parent,
            labels={"task_id": task_id, "task_name": task_name, "subtask_index": subtask_index},
        )
        self.task_id = task_id
        self.task_name = task_name
        self.subtask_index = subtask_index
        self.operator_groups: dict[str, OperatorMetricGroup] = {}

    def metric_identifier(self, metric_name: str) -> str:
        return f"ray_klein_task_{metric_name}"

    def add_operator_group(self, operator_id: str, operator_name: str) -> OperatorMetricGroup:
        existing = self.operator_groups.get(operator_id)
        if existing is not None:
            if existing.operator_name != operator_name:
                raise ValueError(f"Operator metric group {operator_id!r} has a different name.")
            return existing
        group = OperatorMetricGroup(self, operator_id, operator_name, self.subtask_index)
        self.operator_groups[operator_id] = group
        return group

    def operator_group(self, operator_id: str) -> OperatorMetricGroup:
        return self.operator_groups[operator_id]


class OperatorMetricGroup(GenericMetricGroup):
    def __init__(
        self,
        parent: TaskMetricGroup | None,
        operator_id: str,
        operator_name: str,
        subtask_index: int,
    ) -> None:
        super().__init__(
            group_name="ray_klein_operator",
            parent=parent,
            labels={
                "operator_id": operator_id,
                "operator_name": operator_name,
                "subtask_index": subtask_index,
            },
        )
        self.operator_id = operator_id
        self.operator_name = operator_name
        self.subtask_index = subtask_index

    def metric_identifier(self, metric_name: str) -> str:
        return f"ray_klein_operator_{metric_name}"
