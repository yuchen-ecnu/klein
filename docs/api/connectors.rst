.. SPDX-License-Identifier: Apache-2.0

Connector extension classes
===========================

Most applications should use module-level ``ray.klein.read_*`` functions and
:class:`~ray.klein.DataStream` writer methods. The classes below are public
extension contracts for direct construction, subclassing, or lifecycle tests.
Install the corresponding optional dependency on the driver and workers before
importing a connector module.

Kafka
-----

.. currentmodule:: ray.klein.integrations.kafka

.. autoclass:: KafkaSource
   :members:

.. autoclass:: KafkaSink
   :members:

Apache RocketMQ
---------------

.. currentmodule:: ray.klein.integrations.rocketmq

.. autoclass:: RocketMQSource
   :members:

Filesystem
----------

.. currentmodule:: ray.klein.integrations.filesystem

.. autoclass:: StreamingFileSink
   :members:

Apache Iceberg
--------------

.. currentmodule:: ray.klein.integrations.iceberg

.. autoclass:: StreamingIcebergSink
   :members:

Database API
------------

.. currentmodule:: ray.klein.integrations.sql

.. autoclass:: StreamingSQLSink
   :members:

Console
-------

.. currentmodule:: ray.klein.integrations.console

.. autoclass:: ConsoleSinkFunction
   :members:

See :doc:`../connectors/index` for installation, schemas, complete option
tables, execution-mode support, delivery guarantees, and operational guidance.
