.. SPDX-License-Identifier: Apache-2.0

.. _data-stream-api:

DataStream API
==============

.. currentmodule:: ray.klein.api.data_stream

.. autoclass:: DataStream

.. autosummary::
   :nosignatures:

   DataStream.changelog_mode
   DataStream.map
   DataStream.map_batches
   DataStream.flat_map
   DataStream.map_reduce
   DataStream.filter
   DataStream.union
   DataStream.assign_timestamps_and_watermarks
   DataStream.key_by
   DataStream.group_by
   DataStream.join
   DataStream.interval_join
   DataStream.broadcast
   DataStream.rescale
   DataStream.round_robin
   DataStream.adaptive_shuffle
   DataStream.partition_by
   DataStream.data
   DataStream.sql
   DataStream.show
   DataStream.take_all
   DataStream.take
   DataStream.schema
   DataStream.write_files
   DataStream.write_json
   DataStream.write_csv
   DataStream.write_parquet
   DataStream.write_text
   DataStream.write_sql
   DataStream.write_kafka
   DataStream.write_redis
   DataStream.write
