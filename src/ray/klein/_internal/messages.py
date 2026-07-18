# SPDX-License-Identifier: Apache-2.0
class ChineseMessages:
    """ChineseMessages."""

    SET_INTERACTIVE_MODE_FOR_TAKE = (
        "take方法只能在interactive模式下使用，且无需再调用execute方法即可直接执行并获取返回值："
        "\t1.设置interactive模式`context.enable_interactive_mode()`"
        "\t2.获取返回值`res = stream.take(5)`"
    )
