# SPDX-License-Identifier: Apache-2.0
"""Unit tests for Klein's per-job Ray-namespace isolation.

These tests don't spin up a real Ray cluster — they verify the *plumbing* that
makes namespace isolation work: namespace generation, propagation through the
factory methods (so two JobClients in the same process pick different
namespaces) and the propagation into ``ray_remote_args`` / descriptor /
collector so the eventual ``ray.get_actor`` / ``ray.remote(...)`` calls land
in the right namespace. The actual cluster-level isolation (that two jobs
with the same vertex names can coexist) is an integration concern verified
by the existing end-to-end Klein test jobs once they are run in a real
Ray cluster.
"""

import unittest
from unittest import mock

from ray.klein._internal.constants import (
    ComponentName,
    _sanitize_job_name_for_namespace,
    build_job_namespace,
)
from ray.klein.config.configuration import Configuration
from ray.klein.config.job_manager_options import JobManagerOptions


class SanitizeJobNameTest(unittest.TestCase):
    def test_empty_collapses_to_job(self):
        # Empty / all-illegal inputs must still produce a non-empty component so
        # the final namespace ("klein--<uuid>") never has a double dash.
        self.assertEqual(_sanitize_job_name_for_namespace(""), "job")
        self.assertEqual(_sanitize_job_name_for_namespace("!!!"), "job")

    def test_lowercase_and_dashes(self):
        # Whitespace + punctuation collapse to single dashes; leading/trailing
        # dashes are trimmed so the join with the uuid suffix stays clean.
        self.assertEqual(_sanitize_job_name_for_namespace("  My @Job#1!  "), "my-job-1")

    def test_length_capped(self):
        # Cap keeps long auto-generated job names from blowing up the
        # namespace string and making logs / dashboards unreadable.
        result = _sanitize_job_name_for_namespace("a" * 200)
        self.assertEqual(len(result), 40)
        self.assertTrue(set(result) == {"a"})


class BuildJobNamespaceTest(unittest.TestCase):
    def test_explicit_namespace_passthrough(self):
        # An explicit namespace from JobManagerOptions.NAMESPACE wins over
        # auto-generation (use-case: ops tooling that attaches to an
        # already-running job by namespace).
        self.assertEqual(
            build_job_namespace(job_name="anything", explicit_namespace="ops-ns"),
            "ops-ns",
        )

    def test_auto_generated_namespaces_are_unique(self):
        # Cross-job uniqueness is the whole point — two calls with the same
        # job_name must still produce different namespaces.
        ns1 = build_job_namespace(job_name="my-job")
        ns2 = build_job_namespace(job_name="my-job")
        self.assertNotEqual(ns1, ns2)
        self.assertTrue(ns1.startswith("klein-my-job-"))
        self.assertTrue(ns2.startswith("klein-my-job-"))

    def test_auto_generated_namespace_handles_empty_job_name(self):
        # JobClient picks a namespace in __init__ before job_name is known,
        # so the empty-job_name path must still produce a valid namespace.
        ns = build_job_namespace(job_name=None)
        self.assertTrue(ns.startswith("klein-job-"))


class JobManagerCreateNamespaceTest(unittest.TestCase):
    """``JobManager.create`` must pass the per-job namespace into both the
    lookup and the eventual ``ray.remote(...).remote()`` registration so each
    Klein job lands on its own JobManager instead of sharing one cluster-
    global ``"JobManager"`` named actor with siblings."""

    def test_create_passes_namespace_to_lookup_and_remote_args(self):
        from ray.klein.runtime.job_manager import job_manager as jm_mod

        captured_lookup = {}
        captured_remote_args = {}

        def fake_get_actor_by_name(name, namespace=None):
            captured_lookup["name"] = name
            captured_lookup["namespace"] = namespace
            return  # force the create branch

        def fake_create_remote_actor(actor_clz, construct_args=None, ray_remote_args=None, **_):
            captured_remote_args["construct_args"] = construct_args
            captured_remote_args["ray_remote_args"] = ray_remote_args
            return mock.sentinel.handle

        with (
            mock.patch.object(jm_mod.klein, "get_actor_by_name", side_effect=fake_get_actor_by_name),
            mock.patch.object(jm_mod, "create_remote_actor", side_effect=fake_create_remote_actor),
        ):
            handle = jm_mod.JobManager.create(Configuration(), namespace="my-ns")

        self.assertIs(handle, mock.sentinel.handle)
        # Lookup must be scoped — otherwise the second JobClient finds the
        # first job's JobManager and silently reuses it.
        self.assertEqual(captured_lookup["name"], ComponentName.KLEIN_JOB_MANAGER)
        self.assertEqual(captured_lookup["namespace"], "my-ns")
        # Registration must be scoped — otherwise both jobs would try to
        # register "JobManager" in the same default namespace and the second
        # would either collide or alias onto the first.
        self.assertEqual(captured_remote_args["ray_remote_args"]["namespace"], "my-ns")
        self.assertEqual(
            captured_remote_args["ray_remote_args"]["name"],
            ComponentName.KLEIN_JOB_MANAGER,
        )
        self.assertEqual(captured_remote_args["ray_remote_args"]["lifetime"], "detached")
        self.assertEqual(captured_remote_args["ray_remote_args"]["max_restarts"], -1)
        self.assertEqual(captured_remote_args["ray_remote_args"]["max_task_retries"], -1)
        # The constructed JobManager actor needs the namespace too so its
        # supervisor loop can propagate it to JobMaster / StreamTask
        # without re-deriving from config.
        self.assertEqual(captured_remote_args["construct_args"]["namespace"], "my-ns")


class CheckpointCoordinatorNamespaceTest(unittest.TestCase):
    def test_get_or_create_passes_namespace(self):
        from ray.klein.runtime.coordinator import checkpoint_coordinator as pc_mod

        captured_lookup = {}
        captured_remote = {}

        def fake_get_actor_by_name(name, namespace=None):
            captured_lookup["name"] = name
            captured_lookup["namespace"] = namespace
            return

        def fake_create_remote_actor(actor_clz, construct_args=None, ray_remote_args=None):
            captured_remote["ray_remote_args"] = ray_remote_args
            captured_remote["construct_args"] = construct_args
            return mock.sentinel.handle

        with (
            mock.patch.object(pc_mod.klein, "get_actor_by_name", side_effect=fake_get_actor_by_name),
            mock.patch.object(pc_mod, "create_remote_actor", side_effect=fake_create_remote_actor),
        ):
            pc_mod.CheckpointCoordinator.open_or_create(Configuration(), namespace="job-ns")

        self.assertEqual(captured_lookup["name"], ComponentName.KLEIN_CHECKPOINT_COORDINATOR)
        self.assertEqual(captured_lookup["namespace"], "job-ns")
        self.assertEqual(captured_remote["ray_remote_args"]["namespace"], "job-ns")
        self.assertEqual(captured_remote["construct_args"]["job_id"], "job-ns")


class GetActorByNameNamespaceTest(unittest.TestCase):
    """``klein.get_actor_by_name(name, namespace=...)`` must propagate the
    namespace into Ray's ``ray.get_actor`` so the lookup is scoped — that's
    the actual mechanism that prevents two coexisting Klein jobs from
    resolving each other's named actors."""

    def test_namespace_forwarded_to_ray_get_actor(self):
        from ray.klein._internal import ray as w_mod

        # debug mode short-circuits the ray.get_actor path — force it off so
        # we actually exercise the forwarding logic.
        with (
            mock.patch.object(w_mod, "is_debug_mode", return_value=False),
            mock.patch.object(w_mod.ray, "get_actor", return_value=mock.sentinel.actor) as p,
        ):
            handle = w_mod.get_actor_by_name("X", namespace="ns-1")
        p.assert_called_once_with("X", namespace="ns-1")
        self.assertIsNotNone(handle)

    def test_namespace_none_still_calls_ray_get_actor_with_none(self):
        # The low-level helper intentionally exposes Ray's current-namespace
        # lookup; Klein runtime components use explicit per-job namespaces.
        from ray.klein._internal import ray as w_mod

        with (
            mock.patch.object(w_mod, "is_debug_mode", return_value=False),
            mock.patch.object(w_mod.ray, "get_actor", return_value=mock.sentinel.actor) as p,
        ):
            w_mod.get_actor_by_name("X")
        p.assert_called_once_with("X", namespace=None)

    def test_collector_reresolve_keeps_job_namespace(self):
        from ray.klein.runtime.collector import downstream_sender as sender_module
        from ray.klein.runtime.collector.delivery_journal import DeliveryJournal
        from ray.klein.runtime.collector.downstream_sender import DownstreamSender

        sender = DownstreamSender(
            [mock.sentinel.stale],
            ["downstream-0"],
            (0,),
            DeliveryJournal(1),
            1.0,
            "job-ns",
        )
        with mock.patch.object(
            sender_module.klein,
            "get_actor_by_name",
            return_value=mock.sentinel.live,
        ) as lookup:
            sender.refresh_target(0)

        lookup.assert_called_once_with("downstream-0", namespace="job-ns")


class ConfigurationNamespaceOptionTest(unittest.TestCase):
    def test_explicit_namespace_from_config_used_by_build(self):
        # End-to-end check that JobClient's "config.get(NAMESPACE) -> build_job_namespace"
        # path honours the explicit value (rather than auto-generating).
        cfg = Configuration()
        cfg.set(JobManagerOptions.NAMESPACE, "shared-ns")
        explicit = cfg.get(JobManagerOptions.NAMESPACE)
        self.assertEqual(
            build_job_namespace(job_name="x", explicit_namespace=explicit),
            "shared-ns",
        )

    def test_default_namespace_empty_string(self):
        # The default is the empty string (so "not set" is distinguishable
        # from "set to nothing"); JobClient maps that to None before calling
        # build_job_namespace so the auto-generation path kicks in.
        self.assertEqual(Configuration().get(JobManagerOptions.NAMESPACE), "")
