# SPDX-License-Identifier: Apache-2.0
"""Property tests for configuration and connector string boundaries."""

from __future__ import annotations

import json

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from ray.klein._internal.sql.connector_options import parse_option_value
from ray.klein.config.config_option import normalize_config_key
from ray.klein.config.configuration import Configuration

JSON_SCALARS = st.one_of(st.none(), st.booleans(), st.integers(), st.text(max_size=256))
CONFIG_SCALARS = st.one_of(st.booleans(), st.integers(), st.text(max_size=256))
CONFIG_KEYS = st.sampled_from(
    [
        "execution.runtime.mode",
        "pipeline.operator_chaining.enabled",
        "state.keyed.max_parallelism",
        "table.exec.state.ttl",
    ]
)


@given(st.dictionaries(CONFIG_KEYS, CONFIG_SCALARS, max_size=4))
@settings(suppress_health_check=[HealthCheck.too_slow])
def test_json_configuration_round_trips_arbitrary_scalars(values: dict[str, object]) -> None:
    configuration = Configuration(json.dumps(values))

    assert configuration.to_dict() == {normalize_config_key(key): value for key, value in values.items()}


@given(JSON_SCALARS)
def test_flink_connector_json_scalars_round_trip(value: object) -> None:
    assert parse_option_value(json.dumps(value)) == value
