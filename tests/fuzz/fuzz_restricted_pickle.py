# SPDX-License-Identifier: Apache-2.0
"""Coverage-guided fuzz target for the framework snapshot trust boundary."""

from __future__ import annotations

import pickle
import sys
from contextlib import suppress

import atheris

with atheris.instrument_imports():
    from ray.klein.state.restricted_pickle import restricted_pickle_loads


@atheris.instrument_func
def test_one_input(payload: bytes) -> None:
    with suppress(pickle.UnpicklingError):
        restricted_pickle_loads(payload)


def main() -> None:
    atheris.Setup(sys.argv, test_one_input)
    atheris.Fuzz()


if __name__ == "__main__":
    main()
