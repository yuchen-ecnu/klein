.. SPDX-License-Identifier: Apache-2.0

.. _configurations-api:

Configuration API
=================

.. currentmodule:: ray.klein.config.configuration

.. autoclass:: Configuration

.. autosummary::
   :nosignatures:

   Configuration.get
   Configuration.get_optional
   Configuration.set
   Configuration.update
   Configuration.unset
   Configuration.to_dict
   Configuration.convert_value

Typed option descriptor
-----------------------

.. currentmodule:: ray.klein.config.config_option

.. autoclass:: ConfigOption
   :members:

See :doc:`../configuration-reference` for the complete option catalog and
:doc:`../configuration` for configuration precedence and conversion rules.
