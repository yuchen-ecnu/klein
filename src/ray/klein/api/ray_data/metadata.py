# SPDX-License-Identifier: Apache-2.0
import functools
import inspect
from collections.abc import Callable


def copy_callable_metadata(wrapper: Callable, target: Callable, *, drop_first: bool) -> Callable:
    functools.update_wrapper(wrapper, target)
    try:
        signature = inspect.signature(target)
        if drop_first:
            signature = signature.replace(parameters=tuple(signature.parameters.values())[1:])
        wrapper.__signature__ = signature
    except (TypeError, ValueError):
        pass
    return wrapper
