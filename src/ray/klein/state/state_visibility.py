# SPDX-License-Identifier: Apache-2.0
from enum import Enum


class StateVisibility(str, Enum):
    NEVER_RETURN_EXPIRED = "never_return_expired"
    RETURN_EXPIRED_IF_NOT_CLEANED_UP = "return_expired_if_not_cleaned_up"
