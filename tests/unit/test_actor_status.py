# SPDX-License-Identifier: Apache-2.0
"""Unit tests for get_actor_status, the failover health-loop liveness probe.

The recovery supervisor polls this on every tick, so it must classify an actor
as ALIVE / DEAD / NOT_EXIST using only cheap control-plane calls (get_actor +
a short ping), never the rate-limited observability API. Tested by mocking Ray
so the three-way classification and namespace forwarding are pinned without a
real cluster.
"""

import unittest
from unittest import mock

from ray.klein._internal import ray as w_mod
from ray.klein.api.stream_task_status import StreamTaskStatus


class GetActorStatusTest(unittest.TestCase):
    def setUp(self):
        # Force real (non-debug) path; debug mode short-circuits to ALIVE.
        self._debug_patch = mock.patch.object(w_mod, "is_debug_mode", return_value=False)
        self._debug_patch.start()
        self.addCleanup(self._debug_patch.stop)

    def test_not_exist_when_get_actor_raises(self):
        with mock.patch.object(w_mod.ray, "get_actor", side_effect=ValueError("no such actor")):
            self.assertEqual(w_mod.get_actor_status("gone"), StreamTaskStatus.NOT_EXIST)

    def test_alive_when_ping_returns(self):
        actor = mock.MagicMock()
        with (
            mock.patch.object(w_mod.ray, "get_actor", return_value=actor),
            mock.patch.object(w_mod.ray, "get", return_value="pong"),
        ):
            self.assertEqual(w_mod.get_actor_status("up"), StreamTaskStatus.ALIVE)

    def test_dead_when_ping_times_out(self):
        # Exists by name but ping fails -> being rebuilt -> DEAD (recoverable).
        actor = mock.MagicMock()
        with (
            mock.patch.object(w_mod.ray, "get_actor", return_value=actor),
            mock.patch.object(w_mod.ray, "get", side_effect=TimeoutError("ping timed out")),
        ):
            self.assertEqual(w_mod.get_actor_status("rebuilding"), StreamTaskStatus.DEAD)

    def test_debug_mode_is_always_alive(self):
        # The base setUp patch forces non-debug; re-patch to True here.
        with mock.patch.object(w_mod, "is_debug_mode", return_value=True):
            self.assertEqual(w_mod.get_actor_status("anything"), StreamTaskStatus.ALIVE)

    def test_namespace_forwarded_to_get_actor(self):
        actor = mock.MagicMock()
        with (
            mock.patch.object(w_mod.ray, "get_actor", return_value=actor) as p,
            mock.patch.object(w_mod.ray, "get", return_value="pong"),
        ):
            w_mod.get_actor_status("x", namespace="job-ns")
        p.assert_called_once_with("x", namespace="job-ns")

    def test_namespace_defaults_to_none(self):
        actor = mock.MagicMock()
        with (
            mock.patch.object(w_mod.ray, "get_actor", return_value=actor) as p,
            mock.patch.object(w_mod.ray, "get", return_value="pong"),
        ):
            w_mod.get_actor_status("x")
        p.assert_called_once_with("x", namespace=None)
