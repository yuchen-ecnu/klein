# SPDX-License-Identifier: Apache-2.0

import importlib.util
from pathlib import Path


def test_dco_trailer_requires_name_and_email(project_root: Path) -> None:
    spec = importlib.util.spec_from_file_location("check_dco", project_root / "scripts" / "check_dco.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    assert module._SIGN_OFF.search("Subject\n\nSigned-off-by: Ada Lovelace <ada@example.com>")
    assert not module._SIGN_OFF.search("Signed-off-by: Ada Lovelace")
