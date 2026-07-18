# SPDX-License-Identifier: Apache-2.0
import enum


class OperatorType(enum.Enum):
    SOURCE = 0  # Sources are where your program reads its input from
    ONE_INPUT = 1  # This operator has one data stream as it's input stream.
    TWO_INPUT = 2  # This operator has two data stream as it's input stream.
    SINK = 3  # Sink operator.
    COLLECT = 4  # Collect operator.
    REDUCE = 5  # Reduce operator.
