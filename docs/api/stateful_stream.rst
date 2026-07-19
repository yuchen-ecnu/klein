.. SPDX-License-Identifier: Apache-2.0

.. _stateful-stream-api:

Stateful stream API
===================

.. currentmodule:: ray.klein

.. autosummary::
   :nosignatures:

   KeyedProcessFunction
   KeyedStream
   WindowAssigner
   TumblingWindow
   SlidingWindow
   SessionWindow
   TimeWindow
   WindowedStream

.. currentmodule:: ray.klein.api.keyed_process_function

.. autoclass:: KeyedProcessFunction

.. autosummary::
   :nosignatures:

   KeyedProcessFunction.process
   KeyedProcessFunction.on_timer

.. currentmodule:: ray.klein.api.keyed_stream

.. autosummary::
   :nosignatures:

   KeyedStream.process
   KeyedStream.window

.. currentmodule:: ray.klein.api.windowed_stream

.. autosummary::
   :nosignatures:

   WindowedStream.reduce
