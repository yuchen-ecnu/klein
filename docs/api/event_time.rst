.. SPDX-License-Identifier: Apache-2.0

.. _event-time-api:

Event time API
==============

.. currentmodule:: ray.klein

.. autosummary::
   :nosignatures:

   WatermarkStrategy

   WatermarkStrategy.for_monotonous_timestamps
   WatermarkStrategy.for_bounded_out_of_orderness
   WatermarkStrategy.with_idleness

.. currentmodule:: ray.klein.api.source_context

.. autosummary::
   :nosignatures:

   SourceContext.collect
   SourceContext.on_idle
   SourceContext.emit_watermark
   SourceContext.mark_idle
   SourceContext.mark_active
