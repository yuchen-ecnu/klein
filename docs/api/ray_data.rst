.. SPDX-License-Identifier: Apache-2.0

.. _ray-data-adapter-api:

Ray Data adapter API
====================

.. currentmodule:: ray.klein.api.ray_data

.. autoclass:: RayDataContextAdapter
   :members: source, from_dataset, available

.. autoclass:: RayDataStreamAdapter
   :members: transform, consume, available, kind

.. autoclass:: RayDataMethodKind
   :members:

.. autoclass:: RayDataAPIError

Advanced discovery contracts
----------------------------

The following functions support adapters that need to inspect the public API
of the installed compatible Ray version. Ordinary graph code should prefer
``name in ctx.data.available`` and ``name in stream.data.available``.

.. autosummary::
   :nosignatures:

   public_dataset_factories
   public_dataset_methods
   has_public_dataset_factory
   has_public_dataset_method
   classify_dataset_method
   RayDataCall
