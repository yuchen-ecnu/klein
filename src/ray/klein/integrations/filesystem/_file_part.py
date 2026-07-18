# SPDX-License-Identifier: Apache-2.0
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class FilePart:
    """Private pending path and public final path for one immutable part."""

    pending_path: str
    final_path: str
