# SPDX-License-Identifier: Apache-2.0
from dataclasses import FrozenInstanceError
from datetime import timedelta
from unittest import TestCase

import pytest

from ray.klein.config.config_option import ConfigOption
from ray.klein.config.configuration import Configuration
from ray.klein.config.event_time_options import EventTimeOptions
from ray.klein.config.execution_options import ExecutionOptions
from ray.klein.config.pipeline_options import PipelineOptions
from ray.klein.config.runtime_execution_mode import RuntimeExecutionMode
from ray.klein.config.state_options import StateOptions
from ray.klein.config.table_options import TableOptions


class ConfigurationTest(TestCase):
    STATIC_USERNAME_CONFIG_OPTION = ConfigOption("username", None, str, "username for test")
    STATIC_PASSWORD_CONFIG_OPTION = ConfigOption("password", None, str, "password for test")
    STATIC_AGE_CONFIG_OPTION = ConfigOption("age", 10, int, "age for test")
    STATIC_CHK_INTERVAL_CONFIG_OPTION = ConfigOption(
        "chk_interval", timedelta(seconds=10), timedelta, "test checkpoint interval"
    )

    def test_set(self) -> None:
        config = Configuration()
        self.assertIs(config.get(ConfigurationTest.STATIC_USERNAME_CONFIG_OPTION), None)
        config.set(ConfigurationTest.STATIC_USERNAME_CONFIG_OPTION, "user1")
        self.assertEqual(config.get(ConfigurationTest.STATIC_USERNAME_CONFIG_OPTION), "user1")
        self.assertRaises(
            TypeError,
            config.set,
            ConfigurationTest.STATIC_PASSWORD_CONFIG_OPTION,
            10,
        )

    def test_update_from_configuration(self) -> None:
        config = Configuration()
        config.set(ConfigurationTest.STATIC_USERNAME_CONFIG_OPTION, "user1")
        self.assertIs(config.get(ConfigurationTest.STATIC_USERNAME_CONFIG_OPTION), "user1")
        config2 = Configuration()
        config2.set(ConfigurationTest.STATIC_USERNAME_CONFIG_OPTION, "user2")
        self.assertIs(config2.get(ConfigurationTest.STATIC_USERNAME_CONFIG_OPTION), "user2")
        updated_config = config.update(config2)
        self.assertIs(
            updated_config.get(ConfigurationTest.STATIC_USERNAME_CONFIG_OPTION),
            "user2",
        )

    def test_get_default(self) -> None:
        config = Configuration()
        self.assertEqual(config.get(ConfigurationTest.STATIC_AGE_CONFIG_OPTION), 10)
        config.set(ConfigurationTest.STATIC_AGE_CONFIG_OPTION, 15)
        self.assertEqual(config.get(ConfigurationTest.STATIC_AGE_CONFIG_OPTION), 15)

        self.assertEqual(
            config.get(ConfigurationTest.STATIC_CHK_INTERVAL_CONFIG_OPTION),
            timedelta(seconds=10),
        )
        config.set(ConfigurationTest.STATIC_CHK_INTERVAL_CONFIG_OPTION, timedelta(seconds=15))
        self.assertEqual(
            config.get(ConfigurationTest.STATIC_CHK_INTERVAL_CONFIG_OPTION),
            timedelta(seconds=15),
        )

    def test_get_optional(self) -> None:
        config = Configuration()
        self.assertIs(config.get_optional(ConfigurationTest.STATIC_USERNAME_CONFIG_OPTION), None)

    def test_from_dict(self) -> None:
        config = Configuration(
            {
                "username": "zhangsan",
                "password": "abc123",
                "age": "18",
                "chk_interval": "13s",
            }
        )
        self.assertEqual(config.get(ConfigurationTest.STATIC_USERNAME_CONFIG_OPTION), "zhangsan")
        self.assertEqual(config.get(ConfigurationTest.STATIC_PASSWORD_CONFIG_OPTION), "abc123")
        self.assertEqual(config.get(ConfigurationTest.STATIC_AGE_CONFIG_OPTION), 18)
        self.assertEqual(
            config.get(ConfigurationTest.STATIC_CHK_INTERVAL_CONFIG_OPTION),
            timedelta(seconds=13),
        )

    def test_enum(self):
        config = Configuration(
            {
                "execution.runtime.mode": "BATCH",
            }
        )
        self.assertEqual(RuntimeExecutionMode.BATCH, config.get(ExecutionOptions.MODE))

    def test_invalid_enum(self):
        config = Configuration(
            {
                "execution.runtime.mode": "NOT_EXIST_MODE",
            }
        )
        self.assertRaises(ValueError, config.get, ExecutionOptions.MODE)


def test_configuration_accepts_pair_strings_and_json_objects() -> None:
    pairs = Configuration("execution.runtime.mode=streaming; pipeline.operator_chaining.enabled=false")
    payload = Configuration('{"state.backend.type": "memory", "state.ttl.cleanup.batch_size": 32}')

    assert pairs.get(ExecutionOptions.MODE) is RuntimeExecutionMode.STREAMING
    assert pairs.get(PipelineOptions.OPERATOR_CHAINING) is False
    assert payload.to_dict() == {
        "state.backend.type": "memory",
        "state.ttl.cleanup.batch-size": 32,
    }


def test_strict_configuration_rejects_unknown_keys_with_a_suggestion() -> None:
    with pytest.raises(ValueError, match=r"Did you mean 'execution\.runtime\.mode'"):
        Configuration({"execution.runtime.mod": "streaming"}, strict=True)


def test_permissive_configuration_retains_application_metadata() -> None:
    config = Configuration({"application.owner": "analytics"}, strict=False)

    assert config.to_dict() == {"application.owner": "analytics"}
    assert config.strict is False


def test_strict_configuration_exposes_framework_owned_keys() -> None:
    config = Configuration({"execution.runtime.mode": "streaming"}, strict=True)

    assert config.strict is True
    assert "execution.runtime.mode" in config.known_keys()


def test_environment_is_typed_snapshotted_and_lower_priority(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("RAY_KLEIN_EXECUTION_RUNTIME_MODE", "streaming")
    monkeypatch.setenv("RAY_KLEIN_PIPELINE_OPERATOR_CHAINING_ENABLED", "false")
    from_environment = Configuration()
    explicit = Configuration({"execution.runtime.mode": "batch"})

    monkeypatch.setenv("RAY_KLEIN_EXECUTION_RUNTIME_MODE", "batch")

    assert from_environment.get(ExecutionOptions.MODE) is RuntimeExecutionMode.STREAMING
    assert from_environment.get(PipelineOptions.OPERATOR_CHAINING) is False
    assert explicit.get(ExecutionOptions.MODE) is RuntimeExecutionMode.BATCH

    merged = Configuration(include_environment=False).update(from_environment)
    assert merged.get(ExecutionOptions.MODE) is RuntimeExecutionMode.STREAMING


def test_key_group_and_idle_input_options_use_standard_environment_style(monkeypatch) -> None:
    assert Configuration(include_environment=False).get(StateOptions.MAX_PARALLELISM) == 32768

    monkeypatch.setenv("RAY_KLEIN_STATE_KEYED_MAX_PARALLELISM", "256")
    monkeypatch.setenv("RAY_KLEIN_EVENT_TIME_IDLE_INPUT_CHECK_INTERVAL", "250ms")

    config = Configuration()

    assert config.get(StateOptions.MAX_PARALLELISM) == 256
    assert config.get(EventTimeOptions.IDLE_INPUT_CHECK_INTERVAL) == timedelta(milliseconds=250)


def test_flink_table_state_ttl_uses_the_unified_configuration_sources(monkeypatch) -> None:
    monkeypatch.setenv("RAY_KLEIN_TABLE_EXEC_STATE_TTL", "2h")

    from_environment = Configuration()
    from_string = Configuration("table.exec.state.ttl=30min")

    assert from_environment.get(TableOptions.STATE_TTL) == timedelta(hours=2)
    assert from_string.get(TableOptions.STATE_TTL) == timedelta(minutes=30)


def test_config_option_is_immutable_and_validates_its_default() -> None:
    option = ConfigOption("service.timeout", 30, int)

    assert option.key == "service.timeout"
    with pytest.raises(FrozenInstanceError):
        option.key = "changed"  # type: ignore[misc]
    with pytest.raises(TypeError, match="must be an instance of int"):
        ConfigOption("service.timeout", "30", int)


def test_configuration_rejects_none_values() -> None:
    config = Configuration()

    with pytest.raises(ValueError, match="use unset"):
        config.set(ConfigurationTest.STATIC_USERNAME_CONFIG_OPTION, None)
    with pytest.raises(ValueError, match="cannot be None"):
        config.update({"username": None})


@pytest.mark.parametrize("value", [True, False])
@pytest.mark.parametrize("value_type", [int, float, timedelta])
def test_numeric_configuration_values_reject_booleans(value_type: type, value: bool) -> None:
    option = ConfigOption("numeric.value", None, value_type)

    with pytest.raises(ValueError, match="unable to convert"):
        Configuration({"numeric.value": value}).get(option)
    with pytest.raises(TypeError, match="wrong type"):
        Configuration().set(option, value)
    with pytest.raises(TypeError, match="wrong type"):
        Configuration().get(option, value)
    with pytest.raises(TypeError, match="must be an instance"):
        ConfigOption("numeric.default", value, value_type)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (True, True),
        (False, False),
        (1, True),
        (0, False),
        ("1", True),
        ("0", False),
    ],
)
def test_boolean_configuration_accepts_only_boolean_numbers(value: object, expected: bool) -> None:
    option = ConfigOption("feature.enabled", None, bool)

    assert Configuration({"feature.enabled": value}).get(option) is expected


@pytest.mark.parametrize("value", [2, -1, 3])
def test_boolean_configuration_rejects_other_integers(value: int) -> None:
    option = ConfigOption("feature.enabled", None, bool)

    with pytest.raises(ValueError, match="unable to convert"):
        Configuration({"feature.enabled": value}).get(option)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (3, 3),
        ("3", 3),
        (" +3 ", 3),
        ("-2", -2),
        ("003", 3),
    ],
)
def test_integer_configuration_accepts_only_integers_and_integer_strings(value: object, expected: int) -> None:
    option = ConfigOption("worker.count", None, int)

    assert Configuration({"worker.count": value}).get(option) == expected


@pytest.mark.parametrize("value", [3.0, 3.7, "3.0", "1e3"])
def test_integer_configuration_does_not_truncate_or_parse_non_integer_values(value: object) -> None:
    option = ConfigOption("worker.count", None, int)

    with pytest.raises(ValueError, match="unable to convert"):
        Configuration({"worker.count": value}).get(option)


def test_float_configuration_accepts_real_numbers_across_all_typed_paths() -> None:
    option = ConfigOption("ratio", 1, float)
    config = Configuration({"ratio": 2})

    assert option.default == 1.0
    assert config.get(option) == 2.0
    assert isinstance(config.get(option), float)

    config.set(option, 3)
    assert config.get(option) == 3.0
    assert config.to_dict()["ratio"] == 3.0

    missing = Configuration()
    assert missing.get(option, 4) == 4.0


@pytest.mark.parametrize("value", [float("inf"), float("-inf"), float("nan")])
def test_float_configuration_rejects_non_finite_values_across_all_typed_paths(value: float) -> None:
    option = ConfigOption("ratio", None, float)

    with pytest.raises(ValueError, match="unable to convert"):
        Configuration({"ratio": value}).get(option)
    with pytest.raises(ValueError, match="unable to convert"):
        Configuration({"ratio": str(value)}).get(option)
    with pytest.raises(TypeError, match="finite float"):
        Configuration().set(option, value)
    with pytest.raises(TypeError, match="finite float"):
        Configuration().get(option, value)
    with pytest.raises(TypeError, match="must be an instance"):
        ConfigOption("ratio.default", value, float)


def test_numeric_configuration_normalizes_overflow_from_extreme_integers() -> None:
    value = 10**400
    float_option = ConfigOption("ratio", None, float)
    duration_option = ConfigOption("request.timeout", None, timedelta)

    with pytest.raises(ValueError, match="unable to convert"):
        Configuration({"ratio": value}).get(float_option)
    with pytest.raises(TypeError, match="finite float"):
        Configuration().set(float_option, value)
    with pytest.raises(TypeError, match="must be an instance"):
        ConfigOption("ratio.default", value, float)
    with pytest.raises(ValueError, match="unable to convert"):
        Configuration({"request.timeout": value}).get(duration_option)


def test_collection_configuration_requires_matching_json_shapes() -> None:
    sequence = ConfigOption("download.hosts", None, tuple)
    mapping = ConfigOption("storage.options", None, dict)

    assert Configuration({"download.hosts": ["a.example", "b.example"]}).get(sequence) == (
        "a.example",
        "b.example",
    )
    assert Configuration({"download.hosts": '["a.example"]'}).get(sequence) == ("a.example",)
    assert Configuration({"storage.options": '{"region":"us-east-1"}'}).get(mapping) == {"region": "us-east-1"}

    for invalid in ({"host": "a.example"}, '{"host":"a.example"}', '"a.example"', 3):
        with pytest.raises(ValueError, match="unable to convert"):
            Configuration({"download.hosts": invalid}).get(sequence)
    for invalid in ([["region", "us-east-1"]], '["region", "us-east-1"]'):
        with pytest.raises(ValueError, match="unable to convert"):
            Configuration({"storage.options": invalid}).get(mapping)


@pytest.mark.parametrize("value", [float("inf"), float("-inf"), float("nan"), 1e300])
def test_duration_configuration_normalizes_non_finite_and_overflow_errors(value: float) -> None:
    option = ConfigOption("request.timeout", None, timedelta)

    with pytest.raises(ValueError, match="unable to convert"):
        Configuration({"request.timeout": value}).get(option)
