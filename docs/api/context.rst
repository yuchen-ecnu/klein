.. SPDX-License-Identifier: Apache-2.0

.. _klein-context-api:

Pipeline configuration and compatibility API
============================================

New application code can use :class:`Pipeline` as an explicit, strictly
configured graph owner. A single terminal submits directly; a
:class:`StatementSet` groups multiple side-effect sinks. Module-level
``from_*``, ``read_*``, and ``execute`` remain supported for one implicit,
deferred pipeline. ``KleinContext`` remains the permissive advanced-isolation
contract.

.. currentmodule:: ray.klein

.. autoclass:: KleinContext

.. autoclass:: Pipeline

.. autoclass:: StatementSet
   :members: pipeline, sinks, add, add_sink, add_insert_sql, execute, explain

.. autosummary::
   :nosignatures:

   KleinContext.config
   KleinContext.sinks
   KleinContext.configure
   KleinContext.data
   KleinContext.ray_data
   KleinContext.sql_session
   KleinContext.sql
   KleinContext.execute_sql
   KleinContext.from_items
   KleinContext.from_values
   KleinContext.source
   KleinContext.read_kafka
   KleinContext.read_canal
   KleinContext.read_rocketmq
   KleinContext.execute
   KleinContext.run
   KleinContext.explain
   Pipeline.name
   Pipeline.create_statement_set

.. autosummary::
   :nosignatures:

   configure
   get_config
   execute
   run
   explain
   execute_sql
   register_scalar_function
   register_ai_function
   register_table_factory
   from_items
   from_values
   from_ray_dataset
   from_ray_data
   source
   dataset_factory
   read_kafka
   read_canal
   read_rocketmq

Legacy ambient-context helpers
------------------------------

``reset_context`` and ``enable_interactive_mode`` are deprecated. New code
should not depend on terminal operations changing their return type.

.. autosummary::
   :nosignatures:

   current_context
   install_context
   reset_context
   KleinContext.current
   KleinContext.install
   KleinContext.reset
   KleinContext.enable_interactive_mode
