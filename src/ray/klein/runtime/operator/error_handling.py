# SPDX-License-Identifier: Apache-2.0
"""User-function error policy shared by stream operators."""

from ray.klein._internal.logging import get_logger
from ray.klein.observability.metrics.metrics import Counter

logger = get_logger(__name__)


def handle_udf_exception(
    ignored_exception_count: Counter,
    ignore_exceptions: bool,
    e: Exception,
    name: str,
) -> bool:
    """
    Handle UDF exceptions, and decide whether to ignore the exceptions and
    record the metrics based on the configuration.

    Args:
        ignored_exception_count: Counter tracking the number of ignored exceptions.
        ignore_exceptions: Whether UDF exceptions should be ignored.
        e: The exception raised by the UDF.
        name: Name of the UDF for logging context.

    Returns:
        True if ray.klein is configured to ignore UDF exceptions, otherwise False.
    """
    if ignore_exceptions:
        ignored_exception_count.inc()
        logger.warning("Ignoring UDF caused exception %s in %s", e, name)
        return True
    return False
