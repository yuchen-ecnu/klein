# SPDX-License-Identifier: Apache-2.0
import asyncio
import os
import threading
from contextlib import suppress
from unittest.mock import patch

import pytest


class _Request:
    def __init__(self, body) -> None:
        self._body = body
        self.headers = {}

    async def json(self):
        return self._body


class _Closable:
    def __init__(self, result: str) -> None:
        self.result = result
        self.close_count = 0

    def __call__(self, data):
        return {"result": self.result}

    def close(self) -> None:
        self.close_count += 1


class _BlockingClosable(_Closable):
    def __init__(self, result: str) -> None:
        super().__init__(result)
        self.entered = threading.Event()
        self.release = threading.Event()
        self.active = False
        self.closed_while_active = False
        self.lock = threading.Lock()

    def __call__(self, data):
        with self.lock:
            self.active = True
        self.entered.set()
        try:
            if not self.release.wait(timeout=2):
                raise TimeoutError("test did not release blocking operator")
            return data
        finally:
            with self.lock:
                self.active = False

    def close(self) -> None:
        with self.lock:
            self.closed_while_active = self.active
        super().close()


def _deployment():
    from ray.klein.runtime.serve import KleinServeDeployment

    return KleinServeDeployment.func_or_class()


def test_content_change_reloads_and_closes_previous_chain(tmp_path) -> None:
    workflow = tmp_path / "workflow.py"
    workflow.write_text("old", encoding="utf-8")
    original_stat = workflow.stat()
    old = _Closable("old")
    new = _Closable("new")
    deployment = _deployment()

    with patch("ray.klein.runtime.serve_extract.run_extraction", side_effect=[[old, old], [new]]) as extract:
        deployment.reconfigure({"workflow": str(workflow)})
        workflow.write_text("new", encoding="utf-8")  # same size
        os.utime(workflow, ns=(original_stat.st_atime_ns, original_stat.st_mtime_ns))
        deployment.reconfigure({"workflow": str(workflow)})

    assert extract.call_count == 2
    assert deployment.operators == [new]
    assert old.close_count == 1
    assert new.close_count == 0
    deployment.__del__()
    assert new.close_count == 1
    assert not deployment.ready
    deployment.__del__()
    assert new.close_count == 1


def test_failed_reload_keeps_previous_chain_ready(tmp_path) -> None:
    workflow = tmp_path / "workflow.py"
    workflow.write_text("old", encoding="utf-8")
    old = _Closable("old")
    deployment = _deployment()

    with patch("ray.klein.runtime.serve_extract.run_extraction", return_value=[old]):
        deployment.reconfigure({"workflow": str(workflow)})
    workflow.write_text("new", encoding="utf-8")
    with (
        patch("ray.klein.runtime.serve_extract.run_extraction", side_effect=RuntimeError("broken")),
        pytest.raises(RuntimeError, match="Failed to extract"),
    ):
        deployment.reconfigure({"workflow": str(workflow)})

    assert deployment.ready
    assert deployment.operators == [old]
    assert old.close_count == 0
    deployment.__del__()


def test_reload_closes_new_chain_if_workflow_disappears_after_extraction(tmp_path) -> None:
    workflow = tmp_path / "workflow.py"
    workflow.write_text("old", encoding="utf-8")
    old = _Closable("old")
    new = _Closable("new")
    deployment = _deployment()

    with patch("ray.klein.runtime.serve_extract.run_extraction", return_value=[old]):
        deployment.reconfigure({"workflow": str(workflow)})
    workflow.write_text("new", encoding="utf-8")

    def extract_and_remove(_entrypoint):
        workflow.unlink()
        return [new]

    with (
        patch("ray.klein.runtime.serve_extract.run_extraction", side_effect=extract_and_remove),
        pytest.raises(FileNotFoundError),
    ):
        deployment.reconfigure({"workflow": str(workflow)})

    assert new.close_count == 1
    assert old.close_count == 0
    assert deployment.ready
    assert deployment.operators == [old]
    deployment.__del__()


def test_cancelled_request_finishes_before_reload_closes_old_chain(tmp_path) -> None:
    workflow = tmp_path / "workflow.py"
    workflow.write_text("old", encoding="utf-8")
    old = _BlockingClosable("old")
    new = _Closable("new")
    deployment = _deployment()

    with patch("ray.klein.runtime.serve_extract.run_extraction", return_value=[old]):
        deployment.reconfigure({"workflow": str(workflow)})
    workflow.write_text("new", encoding="utf-8")
    extracted = threading.Event()

    def extract_new(_entrypoint):
        extracted.set()
        return [new]

    async def cancel_and_reload():
        request = asyncio.create_task(deployment(_Request({"value": [1]})))
        assert await asyncio.to_thread(old.entered.wait, 1)
        request.cancel()
        with suppress(asyncio.CancelledError):
            await request
        reload_done = threading.Event()
        reload_errors = []

        def reload_workflow():
            try:
                deployment.reconfigure({"workflow": str(workflow)})
            except BaseException as error:
                reload_errors.append(error)
            finally:
                reload_done.set()

        with patch("ray.klein.runtime.serve_extract.run_extraction", side_effect=extract_new):
            reload_thread = threading.Thread(target=reload_workflow)
            reload_thread.start()
            try:
                for _ in range(100):
                    if extracted.is_set():
                        break
                    await asyncio.sleep(0.01)
                assert extracted.is_set()
                assert old.close_count == 0
                assert not old.closed_while_active
            finally:
                old.release.set()
            for _ in range(100):
                if reload_done.is_set():
                    break
                await asyncio.sleep(0.01)
            reload_thread.join(timeout=0.1)
            assert reload_done.is_set()
            assert reload_errors == []

    asyncio.run(cancel_and_reload())
    assert old.close_count == 1
    assert not old.closed_while_active
    deployment.__del__()


def test_replica_serializes_calls_to_shared_operator_instance() -> None:
    class Probe:
        def __init__(self) -> None:
            self.active = 0
            self.max_active = 0
            self.lock = threading.Lock()
            self.entered = threading.Event()
            self.release = threading.Event()

        def __call__(self, data):
            with self.lock:
                self.active += 1
                self.max_active = max(self.max_active, self.active)
                self.entered.set()
            try:
                if not self.release.wait(timeout=1):
                    raise TimeoutError("test did not release probe")
                return data
            finally:
                with self.lock:
                    self.active -= 1

    deployment = _deployment()
    probe = Probe()
    deployment.operators = [probe]
    deployment.ready = True

    async def invoke_concurrently():
        first = asyncio.create_task(deployment(_Request({"value": [1]})))
        assert await asyncio.to_thread(probe.entered.wait, 1)
        second = asyncio.create_task(deployment(_Request({"value": [2]})))
        await asyncio.sleep(0)
        assert not second.done()
        probe.release.set()
        return await asyncio.gather(first, second)

    responses = asyncio.run(invoke_concurrently())
    assert len(responses) == 2
    assert probe.max_active == 1
    deployment.__del__()
