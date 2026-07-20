.. SPDX-License-Identifier: Apache-2.0

.. _klein-context-api:

Pipeline configuration and compatibility API
============================================

Application code should build streams with module-level ``from_*`` and
``read_*`` functions, attach terminal sinks, and call ``execute("job-name")``
after the complete graph has been registered. ``KleinContext`` remains for
advanced isolation; selective sink roots are documented in the advanced
section of :doc:`../job-lifecycle`.

.. currentmodule:: ray.klein

.. autoclass:: KleinContext

.. autosummary::
   :nosignatures:

   KleinContext.config
   KleinContext.sinks
   KleinContext.configure
   KleinContext.data
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
   KleinContext.explain

.. autosummary::
   :nosignatures:

   configure
   get_config
   execute
   explain
   execute_sql
   register_table_factory
   from_items
   from_values
   from_ray_dataset
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
