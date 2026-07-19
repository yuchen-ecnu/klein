.. SPDX-License-Identifier: Apache-2.0

.. _klein-context-api:

Klein context API
=================

.. currentmodule:: ray.klein

.. autoclass:: KleinContext

.. autosummary::
   :nosignatures:

   KleinContext.current
   KleinContext.install
   KleinContext.reset
   KleinContext.config
   KleinContext.sinks
   KleinContext.configure
   KleinContext.data
   KleinContext.sql_session
   KleinContext.sql
   KleinContext.execute_sql
   KleinContext.enable_interactive_mode
   KleinContext.from_items
   KleinContext.from_values
   KleinContext.source
   KleinContext.read_kafka
   KleinContext.read_canal
   KleinContext.read_rocketmq
   KleinContext.execute
   KleinContext.explain

.. autosummary::
   :nosignatures:

   current_context
   install_context
   reset_context
   configure
   execute
   explain
   execute_sql
   from_items
   from_values
   from_ray_dataset
   source
   dataset_factory
   read_kafka
   read_canal
   read_rocketmq
