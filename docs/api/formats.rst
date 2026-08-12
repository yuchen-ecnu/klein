.. SPDX-License-Identifier: Apache-2.0

Message format API
==================

Canal JSON
----------

.. currentmodule:: ray.klein.formats

.. autofunction:: decode_canal_json

.. autodata:: DdlHandling

The decoder preserves Canal's string-or-null column representation and returns
zero or more :class:`~ray.klein.ChangelogRow` values. See
:doc:`../connectors/canal` for the input envelope, metadata columns, DDL policy,
ordering, and checkpoint behavior.

Kafka JSON
----------

.. autofunction:: decode_kafka_json

The decoder accepts the raw Kafka record envelope and returns its JSON object
value, with optional prefixed Kafka metadata. ``read_kafka(...,
value_format="json")`` applies it lazily in batch and streaming modes.
