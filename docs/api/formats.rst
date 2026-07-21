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
