# SPDX-License-Identifier: Apache-2.0
from ray.data import Dataset


class FakeDataset(Dataset):
    """Dataset identity used by lowering tests without starting Ray."""

    def __init__(self) -> None:
        pass

    def __del__(self) -> None:
        pass


def logical_function_of(stream):
    return stream.stream_operator.logical_function
