.. SPDX-License-Identifier: Apache-2.0

Custom source API
=================

See :doc:`../connectors/custom` for a complete implementation and
:doc:`../event-time` for the idle-input and watermark protocol.

.. currentmodule:: ray.klein

.. autoclass:: SourceFunction
   :members:

.. currentmodule:: ray.klein.api.source_context

.. autoclass:: SourceContext
   :members:

Lifecycle order
---------------

For each physical source subtask Klein constructs one ``SourceFunction``,
calls ``open()``, restores source state when configured, and then calls
``run()``. Cancellation calls ``cancel()`` so the run loop can return;
``close()`` releases resources once. Completed checkpoints invoke
``notify_checkpoint_complete()`` at least once, so external offset commits
must be idempotent by checkpoint ID.

