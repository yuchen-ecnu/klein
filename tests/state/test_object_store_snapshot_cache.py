# SPDX-License-Identifier: Apache-2.0
import pytest

from ray.klein.state.object_store_snapshot_cache import ObjectStoreSnapshotCache


def test_small_snapshot_stays_inline_and_large_snapshot_uses_object_store():
    objects = {}

    def put(value):
        reference = f"ref-{len(objects)}"
        objects[reference] = value
        return reference

    cache = ObjectStoreSnapshotCache(put, objects.__getitem__, min_size_bytes=4)
    small = cache.cache(b"abc")
    large = cache.cache(b"abcdefgh")

    assert small.inline_payload == b"abc"
    assert small.object_ref is None
    assert large.inline_payload is None
    assert large.object_ref == "ref-0"
    assert cache.materialize(large) == b"abcdefgh"


def test_snapshot_integrity_is_checked():
    objects = {"ref": b"tampered"}
    cache = ObjectStoreSnapshotCache(lambda _: "ref", objects.__getitem__, min_size_bytes=0)
    reference = cache.cache(b"original")

    with pytest.raises(ValueError, match=r"snapshot (?:size|checksum) mismatch"):
        cache.materialize(reference)
