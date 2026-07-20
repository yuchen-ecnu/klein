# SPDX-License-Identifier: Apache-2.0
import asyncio
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import numpy as np

from ray.klein.api.klein_context import KleinContext
from ray.klein.api.resource_plan import ResourcePlan
from ray.klein.config.configuration import Configuration
from ray.klein.config.environment_variables import EnvironmentVariables
from ray.klein.config.serve_options import ServeOptions
from ray.klein.runtime.graph.logical_graph import LogicalGraph
from ray.klein.runtime.graph.serve_rewriter import ServeRewriter
from ray.klein.runtime.serve import (
    decode_batch,
    instantiate_logical_functions,
)


def simple_map_function(batch: dict[str, np.ndarray]) -> dict[str, np.ndarray]:
    """简单的映射函数，将输入的数字乘以2"""
    result = {}
    for key, value in batch.items():
        result[key] = value * 2
    return result


def embedding_function(batch: dict[str, np.ndarray]) -> dict[str, np.ndarray]:
    """模拟嵌入计算的函数"""
    result = {}
    for key, value in batch.items():
        # 模拟生成嵌入向量
        if key == "input_ids":
            # 为每个输入ID生成一个随机的嵌入向量
            embedding_dim = 128
            batch_size = len(value)
            result["embeddings"] = np.random.random((batch_size, embedding_dim))
        else:
            result[key] = value
    return result


class TestRayServe(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        # 配置Ray Serve相关参数
        self.config = Configuration()
        self.config.set(ServeOptions.CLIENT_NUM_CPUS, 1.0)
        self.config.set(ServeOptions.CLIENT_CONCURRENCY, 2)
        self.config.set(ServeOptions.CLIENT_BATCH_SIZE, 2)
        self.config.set(ServeOptions.CLIENT_BATCH_TIMEOUT, 3)
        self.config.set(ServeOptions.CLIENT_ASYNC_BUFFER_SIZE, 32)
        self.config.set(ServeOptions.DEPLOYMENT_NAME, "embedding-service")
        self.config.set(ServeOptions.ROUTE_PREFIX, "/embed")

    def test_convert_to_ray_serve(self):
        """测试将数据管道转换为Ray Serve应用"""
        # 创建KleinContext
        ctx = KleinContext(self.config)

        # 构建包含Ray Serve功能的数据管道
        input_data = [
            {"input_ids": np.array([1, 2, 3, 4])},
            {"input_ids": np.array([5, 6, 7, 8])},
        ]

        # 创建一个简单的数据管道，启用Ray Serve功能
        stream = ctx.from_items(input_data)
        # 应用简单的预处理，不启用Ray Serve
        preprocessed = stream.map_batches(simple_map_function)
        # 应用嵌入计算，启用Ray Serve
        embedding = preprocessed.map_batches(
            embedding_function,
            ray_serve_enabled=True,  # 开启Ray Serve
            num_cpus=2.0,
            num_gpus=1.0,
            concurrency=2,
            batch_size=16,
        )
        # 添加Sink以构建完整管道
        embedding.show()

        # 创建LogicalGraph
        graph = LogicalGraph.from_sinks(ctx.sinks, "test_ray_serve_job", self.config)

        rewriter = ServeRewriter(graph)
        ray_serve_operators = rewriter.extract_serve_functions()
        rewritten = rewriter.rewrite()

        # 验证结果
        # 1. 检查Ray Serve区域是否被正确识别并转换
        self.assertIsNotNone(ray_serve_operators)
        # 2. 验证生成的operator是否包含embedding_function
        self.assertEqual(len(ray_serve_operators), 1)

        # 3. 检查图中的节点变化情况
        # 验证ray_serve_enabled的节点已被移除，并替换为EmbeddedProxyClient
        has_proxy_client = False
        for node in rewritten.vertices.values():
            if node.name.startswith("EmbeddedProxyClient"):
                has_proxy_client = True
                break

        self.assertTrue(has_proxy_client, "EmbeddedProxyClient节点未创建")

        # 4. 检查是否有任何节点仍包含ray_serve_enabled=True
        for node in rewritten.vertices.values():
            self.assertFalse(node.ray_serve_enabled, f"节点 {node.name} 仍然标记为ray_serve_enabled=True")
        self.assertTrue(any(node.ray_serve_enabled for node in graph.vertices.values()))

    def test_complex_ray_serve_pipeline(self):
        """测试包含多个Ray Serve节点的复杂管道"""
        ctx = KleinContext(self.config)

        # 构建包含多个Ray Serve节点的复杂管道
        input_data = [
            {"input_ids": np.array([1, 2, 3, 4])},
            {"input_ids": np.array([5, 6, 7, 8])},
        ]

        # 创建管道
        stream = ctx.from_items(input_data)

        # 第一个节点启用Ray Serve
        node1 = stream.map_batches(
            simple_map_function,
            ray_serve_enabled=True,
            batch_size=16,
        )

        # 第二个节点，也启用Ray Serve，应该会形成一个连接区域
        node2 = node1.map_batches(embedding_function, ray_serve_enabled=True, batch_size=16)

        # 添加Sink
        node2.show()

        # 创建LogicalGraph
        graph = LogicalGraph.from_sinks(ctx.sinks, "test_complex_ray_serve", self.config)

        rewriter = ServeRewriter(graph)
        ray_serve_operators = rewriter.extract_serve_functions()
        rewritten = rewriter.rewrite()

        # 验证结果
        # 由于有两个连续的Ray Serve节点，应该会生成两个operator
        self.assertEqual(len(ray_serve_operators), 2)

        # 验证Ray Serve区域是否被正确识别并转换为一个EmbeddedProxyClient
        has_proxy_client = False
        for node in rewritten.vertices.values():
            if node.name.startswith("EmbeddedProxyClient"):
                has_proxy_client = True
                break

        self.assertTrue(has_proxy_client, "EmbeddedProxyClient节点未创建")

        # 测试管道中节点总数是否正确（原始4节点 source/node1/node2/sink → 转换后3节点 source/proxy/sink）
        expected_node_count = 3  # 转换后应为source, proxy, sink共3个节点
        self.assertEqual(len(rewritten.vertices), expected_node_count)

    def test_serve_operators_instantiation(self):
        """抽取的 Ray Serve 算子可直接进程内实例化并链式调用。"""
        ctx = KleinContext(self.config)
        input_data = [
            {"input_ids": np.array([1, 2, 3, 4])},
            {"input_ids": np.array([5, 6, 7, 8])},
        ]
        stream = ctx.from_items(input_data)
        preprocessed = stream.map_batches(simple_map_function, ray_serve_enabled=True)
        embedding = preprocessed.map_batches(embedding_function, ray_serve_enabled=True)
        embedding.show()

        graph = LogicalGraph.from_sinks(ctx.sinks, "test_instantiation", self.config)
        serve_fns = ServeRewriter(graph).extract_serve_functions()
        operators = instantiate_logical_functions(serve_fns)

        self.assertEqual(len(operators), len(serve_fns))

        test_input = {"input_ids": np.array([10, 20, 30])}
        result = test_input
        for op in operators:
            result = op(result)

        self.assertIn("embeddings", result, "嵌入向量未在结果中找到")
        self.assertIsInstance(result["embeddings"], np.ndarray, "嵌入向量不是numpy数组")
        self.assertEqual(result["embeddings"].shape[0], len(test_input["input_ids"]), "嵌入向量行数不匹配")
        self.assertEqual(result["embeddings"].shape[1], 128, "嵌入向量维度不匹配")

    def test_end_to_end_serve_workflow(self):
        """端到端：服务端 runpy 跑用户原脚本，在 execute() 处拦截抽取算子。

        用户脚本是普通的 main()+__main__+execute() 结构、一字不改；抽取必须在
        execute() 处中断，execute() 之后的副作用（这里是写 flag 文件）绝不能执行。
        """
        from ray.klein.runtime.serve_extract import run_extraction

        temp_dir = Path(self.temp_dir.name)
        workflow_path = temp_dir / "workflow.py"
        side_effect_flag = temp_dir / "post_execute.flag"
        # A normal script-style workflow: build in main(), run under __main__.
        workflow_src = (
            "import numpy as np\n"
            "from ray.klein.api.klein_context import KleinContext\n"
            "\n"
            "def embedding_function(batch):\n"
            "    result = {}\n"
            "    for key, value in batch.items():\n"
            "        if key == 'input_ids':\n"
            "            result['embeddings'] = np.random.random((len(value), 128))\n"
            "        else:\n"
            "            result[key] = value\n"
            "    return result\n"
            "\n"
            "def main():\n"
            "    ctx = KleinContext()\n"
            "    ctx.from_items([{'input_ids': np.array([1, 2, 3, 4])}]).map_batches(\n"
            "        embedding_function, ray_serve_enabled=True).show()\n"
            "    client = ctx.execute('e2e')\n"
            f"    open({str(side_effect_flag)!r}, 'w').close()  # must NOT run\n"
            "    client.wait()\n"
            "\n"
            "if __name__ == '__main__':\n"
            "    main()\n"
        )
        workflow_path.write_text(workflow_src, encoding="utf-8")

        operators = run_extraction(str(workflow_path))
        self.assertEqual(len(operators), 1)
        self.assertFalse(
            side_effect_flag.exists(),
            "execute() 之后的副作用被执行了，拦截失败",
        )

        test_input = {"input_ids": np.array([10, 20, 30])}
        result = test_input
        for op in operators:
            result = op(result)

        self.assertIn("embeddings", result, "嵌入向量未在结果中找到")
        self.assertIsInstance(result["embeddings"], np.ndarray, "嵌入向量不是numpy数组")
        self.assertEqual(result["embeddings"].shape[0], len(test_input["input_ids"]), "嵌入向量行数不匹配")
        self.assertEqual(result["embeddings"].shape[1], 128, "嵌入向量维度不匹配")

    @patch.dict(os.environ, {EnvironmentVariables.COMPILE_ONLY: "1"})
    def test_serve_config_not_setted(self):
        logical_graph_path = Path(self.temp_dir.name) / "test_serve_config_not_setted.json"
        os.environ[EnvironmentVariables.RESOURCE_PLAN_OUTPUT] = str(logical_graph_path)

        # 创建KleinContext
        ctx = KleinContext()

        # 构建数据管道
        input_data = [{"input_ids": np.array([1, 2, 3, 4])}]
        stream = ctx.from_items(input_data)
        transformed = stream.map_batches(embedding_function, ray_serve_enabled=True).map_batches(
            embedding_function, ray_serve_enabled=True
        )
        transformed.show()

        self.assertRaises(ValueError, ctx.execute)

    @patch.dict(os.environ, {EnvironmentVariables.COMPILE_ONLY: "1"})
    def test_obey_serve_config_for_multi_serve_node(self):
        logical_graph_path = Path(self.temp_dir.name) / "test_obey_serve_config_for_multi_serve_node.json"
        os.environ[EnvironmentVariables.RESOURCE_PLAN_OUTPUT] = str(logical_graph_path)

        # 创建KleinContext
        ctx = KleinContext(self.config)

        # 构建数据管道
        input_data = [{"input_ids": np.array([1, 2, 3, 4])}]
        stream = ctx.from_items(input_data)
        transformed = stream.map_batches(embedding_function, ray_serve_enabled=True).map_batches(
            embedding_function, ray_serve_enabled=True
        )
        transformed.show()

        # Compile LogicalGraph
        ctx.execute().wait()
        rp = ResourcePlan.read(logical_graph_path)
        self.assertEqual(1.0, rp["EmbeddedProxyClient[2]"].cpus)
        self.assertEqual(0.0, rp["EmbeddedProxyClient[2]"].gpus)
        self.assertEqual(2, rp["EmbeddedProxyClient[2]"].effective_concurrency)
        self.assertEqual(
            self.config.get(ServeOptions.CLIENT_BATCH_SIZE),
            rp["EmbeddedProxyClient[2]"].batch_size,
        )

    @patch.dict(os.environ, {EnvironmentVariables.COMPILE_ONLY: "1"})
    def test_obey_operator_config_for_single_serve_node(self):
        logical_graph_path = Path(self.temp_dir.name) / "test_obey_operator_config_for_single_serve_node.json"
        os.environ[EnvironmentVariables.RESOURCE_PLAN_OUTPUT] = str(logical_graph_path)

        # 创建KleinContext
        ctx = KleinContext(self.config)

        # 构建数据管道
        input_data = [{"input_ids": np.array([1, 2, 3, 4])}]
        stream = ctx.from_items(input_data)
        transformed = stream.map_batches(
            embedding_function,
            num_cpus=1.5,
            concurrency=4,
            batch_size=10,
            ray_serve_enabled=True,
        )
        transformed.show()

        # Compile LogicalGraph
        ctx.execute().wait()
        rp = ResourcePlan.read(logical_graph_path)
        self.assertEqual(1.5, rp["EmbeddedProxyClient[2]"].cpus)
        self.assertEqual(0.0, rp["EmbeddedProxyClient[2]"].gpus)
        self.assertEqual(4, rp["EmbeddedProxyClient[2]"].effective_concurrency)
        self.assertEqual(10, rp["EmbeddedProxyClient[2]"].batch_size)


class TestServeRequestPath(unittest.TestCase):
    def test_decode_batch_restores_numpy_semantics(self):
        """A JSON-decoded (list) batch must round-trip back to numpy columns so
        operators get element-wise semantics, not list semantics."""
        import orjson

        from ray.klein.runtime.serve import numpy_encoder

        payload = {"input_ids": np.array([1, 2, 3]), "scalar": 5}
        # Simulate the HTTP hop: encode on the proxy, decode on the deployment.
        wire = orjson.loads(orjson.dumps(payload, default=numpy_encoder))
        self.assertIsInstance(wire["input_ids"], list)

        decoded = decode_batch(wire)
        self.assertIsInstance(decoded["input_ids"], np.ndarray)
        # `* 2` is element-wise on the decoded column, not list repetition.
        np.testing.assert_array_equal(decoded["input_ids"] * 2, np.array([2, 4, 6]))

    def test_backoff_honors_runtime_cap(self):
        from unittest.mock import AsyncMock

        from ray.klein.runtime.serve import EmbeddedProxyClient

        client = EmbeddedProxyClient.__new__(EmbeddedProxyClient)
        client.retry_backoff_max = 10.0
        sleep = AsyncMock()
        with (
            patch("ray.klein.runtime.serve_client.random.uniform", return_value=10.0) as uniform,
            patch("ray.klein.runtime.serve_client.asyncio.sleep", sleep),
        ):
            asyncio.run(client._backoff(attempt=50))

        uniform.assert_called_once_with(0, 10.0)
        sleep.assert_awaited_once_with(10.0)


class TestServeDeployment(unittest.TestCase):
    """Drive the deployment path the serveConfigV2 YAML uses:

        import_path: ray.klein.runtime.serve:app
        deployments:
        - name: KleinServeDeployment
          user_config:
            workflow: /workspace/workflow.py

    Ray Serve constructs ``KleinServeDeployment`` then calls ``reconfigure`` with
    the ``user_config`` dict; requests then hit ``__call__``. We exercise that
    exact sequence against the real class (unwrapped from @serve.deployment),
    without standing up a live Serve cluster.
    """

    WORKFLOW_SRC = (
        "import numpy as np\n"
        "from ray.klein.api.klein_context import KleinContext\n"
        "\n"
        "def embedding_function(batch):\n"
        "    result = {}\n"
        "    for key, value in batch.items():\n"
        "        if key == 'input_ids':\n"
        "            result['embeddings'] = (np.asarray(value) * 2).tolist()\n"
        "        else:\n"
        "            result[key] = value\n"
        "    return result\n"
        "\n"
        "def main():\n"
        "    ctx = KleinContext()\n"
        "    ctx.from_items([{'input_ids': np.array([1, 2, 3])}]).map_batches(\n"
        "        embedding_function, ray_serve_enabled=True).show()\n"
        "    ctx.execute('yaml_deploy').wait()\n"
        "\n"
        "if __name__ == '__main__':\n"
        "    main()\n"
    )

    def _make_deployment(self):
        # Unwrap the @serve.deployment wrapper to get the plain class.
        from ray.klein.runtime.serve import KleinServeDeployment

        return KleinServeDeployment.func_or_class()

    def _write_workflow(self, src=None):
        temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(temp_dir.cleanup)
        path = Path(temp_dir.name) / "workflow.py"
        path.write_text(src or self.WORKFLOW_SRC, encoding="utf-8")
        return str(path)

    def _call(self, deployment, payload):
        """Invoke __call__ with a minimal stand-in for a Starlette request."""
        import json

        class _FakeRequest:
            def __init__(self, body):
                self._body = body
                self.headers = {}

            async def json(self):
                return self._body

        resp = asyncio.run(deployment(_FakeRequest(payload)))
        # JSONResponse stores the encoded body; decode it back for assertions.
        return json.loads(resp.body)

    def test_reconfigure_then_serve_request(self):
        """user_config.workflow → reconfigure loads operators → request returns result."""
        workflow_path = self._write_workflow()
        deployment = self._make_deployment()

        # Ray Serve hands the YAML user_config straight to reconfigure.
        deployment.reconfigure({"workflow": workflow_path})
        self.assertTrue(deployment.ready)
        self.assertEqual(len(deployment.operators), 1)

        # A request flows in as JSON (numpy already list-encoded over HTTP).
        result = self._call(deployment, {"input_ids": [10, 20, 30]})
        self.assertEqual(result["embeddings"], [20, 40, 60])

    def test_reconfigure_caches_unchanged_workflow(self):
        """Re-running reconfigure with an unchanged workflow reuses operators."""
        workflow_path = self._write_workflow()
        deployment = self._make_deployment()

        deployment.reconfigure({"workflow": workflow_path})
        ops_first = deployment.operators
        deployment.reconfigure({"workflow": workflow_path})
        # Same object identity → not re-extracted (no replay of the user script).
        self.assertIs(deployment.operators, ops_first)

    def test_not_ready_returns_503(self):
        """Before reconfigure, the deployment reports itself unavailable."""
        deployment = self._make_deployment()
        result = self._call(deployment, {"input_ids": [1, 2]})
        self.assertIn("error", result)

    def test_reconfigure_bad_workflow_raises(self):
        """A workflow with no serve region fails reconfigure loudly."""
        bad_src = (
            "from ray.klein.api.klein_context import KleinContext\n"
            "def main():\n"
            "    ctx = KleinContext()\n"
            "    ctx.from_items([{'x': [1]}]).map_batches(lambda b: b).show()\n"
            "    ctx.execute('no_serve')\n"
            "if __name__ == '__main__':\n"
            "    main()\n"
        )
        workflow_path = self._write_workflow(bad_src)
        deployment = self._make_deployment()
        with self.assertRaises(Exception):
            deployment.reconfigure({"workflow": workflow_path})
        self.assertFalse(deployment.ready)


class TestServeImportPath(unittest.TestCase):
    """Guard the static contract the serveConfigV2 YAML depends on.

        import_path: ray.klein.runtime.serve:app
        deployments:
        - name: KleinServeDeployment

    These checks catch the most common deployment failures without a live
    cluster: the module failing to import (missing/incompatible deps), ``app``
    not being a bound Serve Application, or the deployment name/callable
    drifting away from what the YAML names.
    """

    def test_import_path_loads_app(self):
        """`ray.klein.runtime.serve:app` imports and is a Serve Application."""
        import importlib

        from ray.serve.deployment import Application

        module = importlib.import_module("ray.klein.runtime.serve")
        self.assertTrue(hasattr(module, "app"), "module exposes no `app` symbol")
        self.assertIsInstance(module.app, Application, "`app` is not a bound Serve Application")

    def test_deployment_name_matches_yaml(self):
        """The bound deployment uses the class name in YAML deployments[].name."""
        import ray.klein.runtime.serve as module

        bound = module.app._bound_deployment
        self.assertEqual(bound.name, "KleinServeDeployment")

    def test_deployment_class_has_serve_hooks(self):
        """The deployment class exposes reconfigure + __call__ Serve drives."""
        import ray.klein.runtime.serve as module

        cls = module.app._bound_deployment.func_or_class
        self.assertIs(cls, module.KleinServeDeployment.func_or_class)
        self.assertTrue(callable(getattr(cls, "reconfigure", None)))
        # __call__ is defined on the class itself (not just inherited from object).
        self.assertIn("__call__", vars(cls))
