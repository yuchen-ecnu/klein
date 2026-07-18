# SPDX-License-Identifier: Apache-2.0


class SQLQueryError(ValueError):
    """Raised when a Klein SQL query or its table bindings are invalid."""
