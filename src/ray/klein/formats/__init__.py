# SPDX-License-Identifier: Apache-2.0
"""Message formats shared by external integrations."""

from ray.klein.formats.canal_json import DdlHandling, decode_canal_json

__all__ = ["DdlHandling", "decode_canal_json"]
