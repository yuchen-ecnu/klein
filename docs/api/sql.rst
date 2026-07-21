.. SPDX-License-Identifier: Apache-2.0

SQL and table API
=================

.. currentmodule:: ray.klein

.. autosummary::
   :nosignatures:

   sql
   register_scalar_function
   SQLQueryError
   SQLSession
   ChangelogRow
   RowKind
   CatalogTable
   TableColumn
   TableFactory

.. autoclass:: ray.klein.api.sql_session.SQLSession
   :members:

.. autoclass:: ray.klein.api.catalog_table.CatalogTable
   :members:

.. autoclass:: ray.klein.api.table_column.TableColumn
   :members:

.. autoclass:: ray.klein.api.table_factory.TableFactory
   :members:

.. autoclass:: ray.klein.api.changelog_row.ChangelogRow
   :members:

.. autoclass:: ray.klein.api.row_kind.RowKind
   :members:
