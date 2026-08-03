# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from io import BytesIO
from pathlib import Path
from typing import Any

import pytest
from ray.data import DataContext

from ray.klein import KleinContext
from ray.klein.api.collect_function import CollectFunction
from ray.klein.api.node_type import NodeType
from ray.klein.config.configuration import Configuration
from tests.support.terminal import execute_terminal


def _encode_image(image_format: str, *, size: tuple[int, int] = (8, 6)) -> bytes:
    image_module = pytest.importorskip("PIL.Image")
    output = BytesIO()
    image = image_module.new("RGB", size, (20, 80, 140))
    try:
        image.save(output, format=image_format)
    except (KeyError, OSError) as error:
        pytest.skip(f"Pillow build cannot encode {image_format}: {error}")
    finally:
        image.close()
    return output.getvalue()


def _decoded_image(value: Any) -> tuple[tuple[int, int], str]:
    image_module = pytest.importorskip("PIL.Image")
    with image_module.open(BytesIO(bytes(value))) as image:
        image.load()
        return image.size, str(image.format).upper()


def _blank_pdf(page_count: int = 2) -> bytes:
    pypdf = pytest.importorskip("pypdf")
    writer = pypdf.PdfWriter()
    for index in range(page_count):
        writer.add_blank_page(width=72 + index, height=72 + index)
    output = BytesIO()
    writer.write(output)
    return output.getvalue()


def _pdf_page_count(value: Any) -> int:
    pypdf = pytest.importorskip("pypdf")
    return len(pypdf.PdfReader(BytesIO(bytes(value))).pages)


def _describe_image_calls(calls: list[tuple[Any, ...]]) -> list[str]:
    from PIL import Image

    descriptions: list[str] = []
    for (payload,) in calls:
        with Image.open(BytesIO(bytes(payload))) as image:
            image.load()
            descriptions.append(f"{image.width}x{image.height}:{str(image.format).upper()}")
    return descriptions


def _describe_page_list_calls(calls: list[tuple[Any, ...]]) -> list[str]:
    from PIL import Image

    descriptions: list[str] = []
    for (pages,) in calls:
        formats = []
        for payload in pages:
            with Image.open(BytesIO(bytes(payload))) as image:
                image.load()
                formats.append(str(image.format).upper())
        descriptions.append(f"{len(pages)}:{','.join(formats)}")
    return descriptions


def test_common_image_formats_resize_in_batch_and_propagate_null() -> None:
    formats = ("PNG", "JPEG", "WEBP", "TIFF", "BMP", "GIF")
    context = KleinContext()
    source_rows = [
        {"id": index, "expected_format": image_format, "payload": _encode_image(image_format)}
        for index, image_format in enumerate(formats, start=1)
    ]
    source_rows.append({"id": len(source_rows) + 1, "expected_format": None, "payload": None})
    images = context.data.from_items(source_rows)

    result = context.sql(
        "SELECT id, expected_format, IMAGE_WIDTH(payload) AS width, "
        "IMAGE_HEIGHT(payload) AS height, IMAGE_FORMAT(payload) AS image_format, "
        "IMAGE_RESIZE(payload, 4, 4, 'contain', 'PNG') AS contained, "
        "IMAGE_RESIZE(payload, 4, 4, 'cover', 'PNG') AS covered, "
        "IMAGE_RESIZE(payload, 4, 3, 'stretch', 'PNG', 85) AS stretched "
        "FROM images ORDER BY id",
        tables={"images": images},
    )
    rows = execute_terminal(result.data.take_all(), job_name="sql-image-media-batch")

    for row in rows[:-1]:
        assert (row["width"], row["height"]) == (8, 6)
        assert row["image_format"] == row["expected_format"]
        assert _decoded_image(row["contained"]) == ((4, 3), "PNG")
        assert _decoded_image(row["covered"]) == ((4, 4), "PNG")
        assert _decoded_image(row["stretched"]) == ((4, 3), "PNG")
    assert rows[-1] == {
        "id": len(source_rows),
        "expected_format": None,
        "width": None,
        "height": None,
        "image_format": None,
        "contained": None,
        "covered": None,
        "stretched": None,
    }


def test_download_can_feed_image_resize_in_batch(tmp_path: Path) -> None:
    payload = _encode_image("PNG", size=(7, 5))
    image_path = tmp_path / "source.png"
    image_path.write_bytes(payload)
    context = KleinContext()
    images = context.data.from_items([{"id": 1, "uri": f"local://{image_path}"}])

    result = context.sql(
        "SELECT id, IMAGE_RESIZE(DOWNLOAD(uri), 3, 2, 'stretch', 'WEBP', 80) AS resized FROM images",
        tables={"images": images},
    )
    rows = execute_terminal(result.data.take_all(), job_name="sql-image-download-batch")

    assert rows[0]["id"] == 1
    assert _decoded_image(rows[0]["resized"]) == ((3, 2), "WEBP")


def test_streaming_image_media_supports_null_download_and_ai_composition(tmp_path: Path) -> None:
    payload = _encode_image("PNG", size=(6, 4))
    image_path = tmp_path / "source.png"
    image_path.write_bytes(payload)
    context = KleinContext(Configuration("execution.runtime.mode=streaming; state.backend.type=memory"))
    images = context.from_values(
        {"id": 1, "payload": payload, "uri": f"local://{image_path}"},
        {"id": 2, "payload": None, "uri": f"local://{image_path}"},
    )
    context.sql_session.register_ai_function("ai_generate", _describe_image_calls, batch_size=2)

    result = context.sql(
        "SELECT id, IMAGE_WIDTH(payload) AS width, "
        "IMAGE_RESIZE(DOWNLOAD(uri), 3, 2, 'stretch', 'PNG', 85) AS resized, "
        "AI_GENERATE(IMAGE_RESIZE(DOWNLOAD(uri), 2, 2, 'stretch', 'PNG', 85)) AS description "
        "FROM images",
        tables={"images": images},
    )
    sink = result.write(CollectFunction, concurrency=1, node_type=NodeType.TAKE, name="SQLMediaStreaming")

    handle = context.execute("streaming-sql-media", sinks=(sink,))
    handle.wait()
    rows = sorted(handle.get(), key=lambda row: row["id"])

    assert [row["width"] for row in rows] == [6, None]
    assert [_decoded_image(row["resized"]) for row in rows] == [((3, 2), "PNG"), ((3, 2), "PNG")]
    assert [row["description"] for row in rows] == ["2x2:PNG", "2x2:PNG"]


def test_pdf_page_count_and_split_execute_in_batch_and_propagate_null() -> None:
    pytest.importorskip("pypdf")
    pytest.importorskip("pypdfium2")
    context = KleinContext()
    documents = context.data.from_items(
        [
            {"id": 1, "payload": _blank_pdf(3)},
            {"id": 2, "payload": None},
        ]
    )

    result = context.sql(
        "SELECT id, PDF_PAGE_COUNT(payload) AS page_count, "
        "PDF_SPLIT(payload) AS pages, PDF_SPLIT(payload, 2, 3) AS subset FROM documents ORDER BY id",
        tables={"documents": documents},
    )
    rows = execute_terminal(result.data.take_all(), job_name="sql-pdf-split-batch")

    assert rows[0]["page_count"] == 3
    assert len(rows[0]["pages"]) == 3
    assert [_pdf_page_count(page) for page in rows[0]["pages"]] == [1, 1, 1]
    assert len(rows[0]["subset"]) == 2
    assert [_pdf_page_count(page) for page in rows[0]["subset"]] == [1, 1]
    assert rows[1] == {"id": 2, "page_count": None, "pages": None, "subset": None}


def test_pdf_render_functions_execute_in_batch() -> None:
    pytest.importorskip("PIL.Image")
    pytest.importorskip("pypdf")
    pytest.importorskip("pypdfium2")
    context = KleinContext()
    documents = context.data.from_items([{"payload": _blank_pdf(2)}])

    result = context.sql(
        "SELECT PDF_RENDER_PAGE(payload, 1) AS first_page, PDF_TO_IMAGES(payload, 72, 1, 2) AS pages FROM documents",
        tables={"documents": documents},
    )
    rows = execute_terminal(result.data.take_all(), job_name="sql-pdf-render-batch")

    assert _decoded_image(rows[0]["first_page"]) == ((144, 144), "PNG")
    assert len(rows[0]["pages"]) == 2
    assert [_decoded_image(page)[1] for page in rows[0]["pages"]] == ["PNG", "PNG"]


def test_pdf_array_stays_arrow_native_through_ai_batch() -> None:
    pytest.importorskip("PIL.Image")
    pytest.importorskip("pypdf")
    pytest.importorskip("pypdfium2")
    data_context = DataContext.get_current()
    previous_fallback = data_context.enable_fallback_to_arrow_object_ext_type
    data_context.enable_fallback_to_arrow_object_ext_type = False
    try:
        context = KleinContext()
        context.sql_session.register_ai_function("ai_generate", _describe_image_calls, batch_size=2)
        documents = context.data.from_items([{"id": 1, "payload": _blank_pdf(2)}, {"id": 2, "payload": None}])

        result = context.sql(
            "SELECT id, PDF_SPLIT(payload) AS pages, "
            "AI_GENERATE(PDF_RENDER_PAGE(payload, 1, 72)) AS description "
            "FROM documents ORDER BY id",
            tables={"documents": documents},
        )
        rows = execute_terminal(result.data.take_all(), job_name="sql-pdf-arrow-native")
    finally:
        data_context.enable_fallback_to_arrow_object_ext_type = previous_fallback

    assert [_pdf_page_count(page) for page in rows[0]["pages"]] == [1, 1]
    assert rows[0]["description"] == "72x72:PNG"
    assert rows[1] == {"id": 2, "pages": None, "description": None}


def test_streaming_pdf_image_list_stays_arrow_native_through_ai() -> None:
    pytest.importorskip("PIL.Image")
    pytest.importorskip("pypdf")
    pytest.importorskip("pypdfium2")
    context = KleinContext(Configuration("execution.runtime.mode=streaming; state.backend.type=memory"))
    documents = context.from_values({"id": 1, "payload": _blank_pdf(2)})
    context.sql_session.register_ai_function("ai_generate", _describe_page_list_calls, batch_size=1)

    result = context.sql(
        "SELECT id, PDF_TO_IMAGES(payload, 72, 1, 2) AS pages, "
        "AI_GENERATE(PDF_TO_IMAGES(payload, 72, 1, 2)) AS description "
        "FROM documents",
        tables={"documents": documents},
    )
    sink = result.write(CollectFunction, concurrency=1, node_type=NodeType.TAKE, name="SQLPDFStreaming")

    handle = context.execute("streaming-sql-pdf-arrow-native", sinks=(sink,))
    handle.wait()
    row = handle.get()[0]

    assert len(row["pages"]) == 2
    assert [_decoded_image(page)[1] for page in row["pages"]] == ["PNG", "PNG"]
    assert row["description"] == "2:PNG,PNG"
