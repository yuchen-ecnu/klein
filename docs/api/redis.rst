.. SPDX-License-Identifier: Apache-2.0

Redis integration
=================

See :doc:`../connectors/redis` for connection and sink defaults, lookup
behavior, examples, execution modes, and delivery guarantees.

.. currentmodule:: ray.klein.integrations.redis

.. autosummary::
   :nosignatures:

   RedisConnectionConfig
   RedisSinkConfig
   RedisDataType
   RedisSink
   RedisValueLookup
   RedisMissingKeyFilter

Writing records
---------------

``DataStream.write_redis`` accepts one connection object, key and value
extractors, and an optional immutable sink configuration::

   from ray.klein.integrations.redis import (
       RedisConnectionConfig,
       RedisDataType,
       RedisSinkConfig,
   )

   stream.write_redis(
       RedisConnectionConfig("localhost"),
       key=lambda row: row["id"],
       value=lambda row: row["attributes"],
       config=RedisSinkConfig(data_type=RedisDataType.HASH),
   )

Strings are replaced with ``SET``. Hashes, lists, and sets are replaced in a
transaction using ``DEL`` followed by ``HSET``, ``RPUSH``, or ``SADD``. This
makes retrying a batch idempotent and preserves list order.
