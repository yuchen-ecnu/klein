# SPDX-License-Identifier: Apache-2.0
from ray.klein.config.config_option import ConfigOption


class UDFOptions:
    IGNORE_EXCEPTIONS = ConfigOption(
        "udf.ignore-exception", False, bool, description="Continue processing after a user-function exception."
    )
