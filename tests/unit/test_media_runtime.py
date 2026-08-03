# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

from io import BytesIO
from types import SimpleNamespace

import pytest

from ray.klein._internal.sql import media_runtime as media_runtime_module
from ray.klein._internal.sql.media_runtime import MediaLimits, MediaRuntime, _CacheKey
from ray.klein.api.sql_query_error import SQLQueryError


def _image_bytes(image_format: str = "PNG", *, size: tuple[int, int] = (8, 4), mode: str = "RGBA") -> bytes:
    image_module = pytest.importorskip("PIL.Image")
    image = image_module.new(mode, size, (220, 40, 80, 160) if mode == "RGBA" else (220, 40, 80))
    output = BytesIO()
    try:
        image.save(output, format=image_format)
        return output.getvalue()
    finally:
        image.close()
        output.close()


def _decoded_image(value: bytes) -> tuple[tuple[int, int], str, str]:
    image_module = pytest.importorskip("PIL.Image")
    with image_module.open(BytesIO(value)) as image:
        image.load()
        return image.size, str(image.format).upper(), image.mode


def _pdf_bytes(*sizes: tuple[int, int]) -> bytes:
    pypdf = pytest.importorskip("pypdf")
    writer = pypdf.PdfWriter()
    output = BytesIO()
    try:
        for width, height in sizes or ((72, 72),):
            writer.add_blank_page(width=width, height=height)
        writer.write(output)
        return output.getvalue()
    finally:
        close = getattr(writer, "close", None)
        if close is not None:
            close()
        output.close()


def _pdf_page_count(value: bytes) -> int:
    pypdf = pytest.importorskip("pypdf")
    return len(pypdf.PdfReader(BytesIO(value)).pages)


@pytest.mark.parametrize(
    "kwargs,match",
    [
        ({"max_input_bytes": 0}, "max_input_bytes"),
        ({"max_output_bytes": -1}, "max_output_bytes"),
        ({"max_batch_bytes": 0}, "max_batch_bytes"),
        ({"max_image_pixels": 0}, "max_image_pixels"),
        ({"max_pdf_pages": True}, "max_pdf_pages"),
        ({"max_pdf_dpi": float("inf")}, "max_pdf_dpi"),
        ({"max_pdf_dpi": True}, "max_pdf_dpi"),
        ({"max_pdf_dpi": "72"}, "max_pdf_dpi"),
        ({"max_pdf_pages": 2, "max_pdf_document_pages": 1}, "cannot exceed"),
    ],
)
def test_media_limits_reject_invalid_values(kwargs, match: str) -> None:
    with pytest.raises(ValueError, match=match):
        MediaLimits(**kwargs)


@pytest.mark.parametrize("native_threads", [True, 0, -1, 1.5])
def test_media_runtime_rejects_invalid_native_thread_counts(native_threads) -> None:
    with pytest.raises(ValueError, match="native_threads"):
        MediaRuntime(native_threads=native_threads)


def test_native_thread_limit_is_lazy_and_preserves_user_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    sentinel = object()
    imported: list[str] = []

    def fake_import(name: str):
        imported.append(name)
        return sentinel

    monkeypatch.delenv("VIPS_CONCURRENCY", raising=False)
    monkeypatch.setattr(media_runtime_module, "import_module", fake_import)
    runtime = MediaRuntime(native_threads=2)

    assert imported == []
    assert runtime._load_pyvips() is sentinel
    assert imported == ["pyvips"]
    assert media_runtime_module.os.environ["VIPS_CONCURRENCY"] == "2"

    monkeypatch.setenv("VIPS_CONCURRENCY", "7")
    assert MediaRuntime(native_threads=3)._load_pyvips() is sentinel
    assert media_runtime_module.os.environ["VIPS_CONCURRENCY"] == "7"


def test_execute_is_strict_and_validates_dispatch() -> None:
    runtime = MediaRuntime()

    assert runtime.execute("IMAGE_RESIZE", (None, 4, 4)) is None
    assert runtime.execute("IMAGE_WIDTH", (b"payload", None)) is None
    with pytest.raises(SQLQueryError, match="exactly 1"):
        runtime.execute("IMAGE_WIDTH", ())
    with pytest.raises(SQLQueryError, match="between 3 and 6"):
        runtime.execute("IMAGE_RESIZE", (b"payload", 1))
    with pytest.raises(SQLQueryError, match="Unsupported SQL media function"):
        runtime.execute("NOT_A_MEDIA_FUNCTION", ())


def test_image_metadata_accepts_all_binary_container_types() -> None:
    payload = _image_bytes("PNG", size=(9, 5))
    runtime = MediaRuntime()

    assert runtime.execute("IMAGE_WIDTH", (payload,)) == 9
    assert runtime.execute("IMAGE_HEIGHT", (bytearray(payload),)) == 5
    assert runtime.execute("IMAGE_FORMAT", (memoryview(payload),)) == "PNG"


@pytest.mark.parametrize(
    "fit,expected_size",
    [("contain", (4, 2)), ("cover", (4, 4)), ("stretch", (4, 4))],
)
def test_image_resize_supports_all_fit_modes(fit: str, expected_size: tuple[int, int]) -> None:
    payload = _image_bytes("PNG", size=(8, 4))
    result = MediaRuntime().execute("IMAGE_RESIZE", (payload, 4, 4, fit, "PNG", 85))

    assert _decoded_image(result)[:2] == (expected_size, "PNG")


@pytest.mark.parametrize(
    "requested,expected",
    [
        ("PNG", "PNG"),
        ("jpg", "JPEG"),
        ("WEBP", "WEBP"),
        ("tif", "TIFF"),
        ("GIF", "GIF"),
        ("BMP", "BMP"),
        ("ICO", "ICO"),
    ],
)
def test_image_resize_encodes_common_output_formats(requested: str, expected: str) -> None:
    payload = _image_bytes("PNG", size=(8, 4))
    result = MediaRuntime().image_resize(payload, 6, 3, "stretch", requested, 74)
    size, image_format, mode = _decoded_image(result)

    assert size == (6, 3)
    assert image_format == expected
    if expected in {"JPEG", "BMP"}:
        assert mode in {"L", "RGB"}


def test_pillow_fallback_handles_metadata_alpha_and_palette_output(monkeypatch: pytest.MonkeyPatch) -> None:
    pytest.importorskip("PIL.Image")
    payload = _image_bytes("PNG", size=(8, 4), mode="RGBA")
    runtime = MediaRuntime()
    monkeypatch.setattr(runtime, "_load_pyvips", lambda: None)

    assert runtime.image_width(payload) == 8
    assert runtime.image_height(payload) == 4
    assert runtime.image_format(payload) == "PNG"
    jpeg = runtime.image_resize(payload, 4, 4, "cover", "JPEG", 80)
    gif = runtime.image_resize(payload, 4, 4, "cover", "GIF", 80)

    assert _decoded_image(jpeg) == ((4, 4), "JPEG", "RGB")
    assert _decoded_image(gif)[:2] == ((4, 4), "GIF")


def test_vips_resize_failure_falls_back_to_pillow(monkeypatch: pytest.MonkeyPatch) -> None:
    try:
        pyvips = __import__("pyvips")
    except (ImportError, OSError):
        pytest.skip("libvips is unavailable")
    pytest.importorskip("PIL.Image")
    payload = _image_bytes("PNG")
    runtime = MediaRuntime()
    runtime._pyvips = pyvips

    def fail_resize(*_args, **_kwargs):
        raise RuntimeError("native resize failed")

    monkeypatch.setattr(runtime, "_resize_vips", fail_resize)
    result = runtime.image_resize(payload, 3, 2, "stretch", "PNG")

    assert _decoded_image(result)[:2] == ((3, 2), "PNG")


@pytest.mark.parametrize(
    "arguments,match",
    [
        ((b"payload", 0, 2), "width must be a positive integer"),
        ((b"payload", True, 2), "width must be a positive integer"),
        ((b"payload", 2.0, 2), "width must be a positive integer"),
        ((b"payload", 2, 2, "pad"), "fit must be"),
        ((b"payload", 2, 2, "contain", 123), "format must be a string"),
        ((b"payload", 2, 2, "contain", "PSD"), "unsupported output format"),
        ((b"payload", 2, 2, "contain", "PNG", 0), "quality must be a positive integer"),
        ((b"payload", 2, 2, "contain", "PNG", 101), "quality must be between"),
        ((b"payload", 257, 2, "contain", "ICO"), "must not exceed 256"),
    ],
)
def test_image_resize_validates_parameters_before_decoding(arguments: tuple[object, ...], match: str) -> None:
    with pytest.raises(SQLQueryError, match=match):
        MediaRuntime().execute("IMAGE_RESIZE", arguments)


def test_image_runtime_enforces_binary_input_pixel_and_output_limits() -> None:
    payload = _image_bytes("PNG", size=(2, 2))

    with pytest.raises(SQLQueryError, match="input must be binary"):
        MediaRuntime().image_width("not-bytes")
    with pytest.raises(SQLQueryError, match="must not be empty"):
        MediaRuntime().image_width(b"")
    with pytest.raises(SQLQueryError, match="input exceeds"):
        MediaRuntime(MediaLimits(max_input_bytes=4)).image_width(payload)
    with pytest.raises(SQLQueryError, match="pixel safety limit"):
        MediaRuntime(MediaLimits(max_image_pixels=4)).image_resize(payload, 3, 2)
    with pytest.raises(SQLQueryError, match="output exceeds"):
        MediaRuntime(MediaLimits(max_output_bytes=1)).image_resize(payload, 2, 2)


def test_missing_media_dependencies_return_install_guidance(monkeypatch: pytest.MonkeyPatch) -> None:
    runtime = MediaRuntime()
    monkeypatch.setattr(runtime, "_load_pyvips", lambda: None)
    monkeypatch.setattr(runtime, "_load_pillow", lambda: None)

    with pytest.raises(SQLQueryError, match=r"pyvips or Pillow.*ray-klein\[media\]"):
        runtime.image_width(b"image")

    monkeypatch.setattr(runtime, "_load_pdfium", lambda: None)
    with pytest.raises(SQLQueryError, match=r"pypdfium2.*ray-klein\[media\]"):
        runtime.pdf_page_count(b"%PDF-1.7\n")

    monkeypatch.setattr(runtime, "_load_pypdf", lambda: None)
    with pytest.raises(SQLQueryError, match=r"pypdf.*ray-klein\[media\]"):
        runtime.pdf_split(b"%PDF-1.7\n")


def test_decode_failures_redact_backend_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    secret = "secret-image-content"

    class FailingImage:
        @staticmethod
        def new_from_buffer(*_args, **_kwargs):
            raise RuntimeError(secret)

    runtime = MediaRuntime()
    monkeypatch.setattr(runtime, "_load_pyvips", lambda: SimpleNamespace(Image=FailingImage))
    monkeypatch.setattr(runtime, "_load_pillow", lambda: None)

    with pytest.raises(SQLQueryError, match="failed to decode image with RuntimeError") as raised:
        runtime.image_width(secret.encode())
    assert secret not in str(raised.value)


def test_row_cache_reuses_image_handle_and_cleanup_preserves_foreign_entries(monkeypatch: pytest.MonkeyPatch) -> None:
    pytest.importorskip("PIL.Image")
    payload = _image_bytes("PNG")
    runtime = MediaRuntime()
    monkeypatch.setattr(runtime, "_load_pyvips", lambda: None)
    opened = 0
    original = runtime._open_pillow_image

    def recording_open(*args, **kwargs):
        nonlocal opened
        opened += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(runtime, "_open_pillow_image", recording_open)
    cache = {"foreign": object()}

    assert runtime.image_width(payload, cache) == 8
    assert runtime.image_height(payload, cache) == 4
    source = next(value for key, value in cache.items() if isinstance(key, _CacheKey))
    assert opened == 1
    assert source.stream is not None and not source.stream.closed

    runtime.clear_cache(cache)

    assert set(cache) == {"foreign"}
    assert source.stream.closed


def test_clear_cache_suppresses_backend_close_failures() -> None:
    class FailingHandle:
        def close(self):
            raise RuntimeError("close failed")

    cache = {_CacheKey("test", 1): FailingHandle(), "foreign": "untouched"}

    MediaRuntime.clear_cache(cache)

    assert cache == {"foreign": "untouched"}


def test_pdf_count_split_render_and_to_images_use_inclusive_one_based_pages() -> None:
    pytest.importorskip("pypdfium2")
    pytest.importorskip("PIL.Image")
    payload = _pdf_bytes((72, 72), (144, 72), (72, 144))
    runtime = MediaRuntime()
    cache: dict[object, object] = {}
    try:
        assert runtime.execute("PDF_PAGE_COUNT", (payload,), cache) == 3
        split = runtime.execute("PDF_SPLIT", (payload, 2, 3), cache)
        assert len(split) == 2
        assert [_pdf_page_count(page) for page in split] == [1, 1]

        rendered = runtime.execute("PDF_RENDER_PAGE", (payload, 2, 72), cache)
        assert _decoded_image(rendered)[:2] == ((144, 72), "PNG")
        images = runtime.execute("PDF_TO_IMAGES", (payload, 72, 1, 3), cache)
        assert [_decoded_image(image)[0] for image in images] == [(72, 72), (144, 72), (72, 144)]
    finally:
        runtime.clear_cache(cache)
    assert cache == {}


def test_pdf_render_converts_once_and_closes_every_temporary_resource() -> None:
    events: list[str] = []

    class Image:
        def save(self, output, *, format: str) -> None:
            assert format == "PNG"
            output.write(b"png")

        def close(self) -> None:
            events.append("image")

    class Bitmap:
        def to_pil(self) -> Image:
            events.append("to_pil")
            return Image()

        def close(self) -> None:
            events.append("bitmap")

    class Page:
        def get_size(self) -> tuple[int, int]:
            return 72, 72

        def render(self, *, scale: float) -> Bitmap:
            assert scale == 2
            return Bitmap()

        def close(self) -> None:
            events.append("page")

    source = SimpleNamespace(document=[Page()])

    assert MediaRuntime()._render_pdfium_page(source, 0, 144, "PDF_RENDER_PAGE") == b"png"
    assert events == ["to_pil", "image", "bitmap", "page"]


def test_pdf_runtime_validates_ranges_pages_dpi_and_document_header() -> None:
    pytest.importorskip("pypdfium2")
    payload = _pdf_bytes((72, 72), (72, 72))
    runtime = MediaRuntime()

    with pytest.raises(SQLQueryError, match="not a PDF"):
        runtime.pdf_page_count(b"not-a-document")
    with pytest.raises(SQLQueryError, match="start page cannot exceed"):
        runtime.pdf_to_images(payload, 72, 2, 1)
    with pytest.raises(SQLQueryError, match="page range exceeds"):
        runtime.pdf_to_images(payload, 72, 1, 3)
    with pytest.raises(SQLQueryError, match="page 3 exceeds"):
        runtime.pdf_render_page(payload, 3, 72)
    with pytest.raises(SQLQueryError, match="DPI must be a number"):
        runtime.pdf_render_page(payload, 1, True)
    with pytest.raises(SQLQueryError, match="DPI must be greater"):
        runtime.pdf_render_page(payload, 1, 601)


def test_pdf_runtime_enforces_page_document_pixel_and_output_limits() -> None:
    pytest.importorskip("pypdfium2")
    pytest.importorskip("PIL.Image")
    payload = _pdf_bytes((72, 72), (72, 72))

    with pytest.raises(SQLQueryError, match="selects 2 pages"):
        MediaRuntime(MediaLimits(max_pdf_pages=1)).pdf_to_images(payload, 72)
    with pytest.raises(SQLQueryError, match="document safety limit"):
        MediaRuntime(MediaLimits(max_pdf_pages=1, max_pdf_document_pages=1)).pdf_page_count(payload)
    with pytest.raises(SQLQueryError, match="pixel safety limit"):
        MediaRuntime(MediaLimits(max_image_pixels=100)).pdf_render_page(payload, 1, 72)
    with pytest.raises(SQLQueryError, match="output exceeds"):
        MediaRuntime(MediaLimits(max_output_bytes=1)).pdf_render_page(payload, 1, 72)


@pytest.mark.parametrize(
    "blob,expected",
    [
        (b"\x00\x00\x00\x18ftypavif", "AVIF"),
        (b"\x00\x00\x00\x18ftypheic", "HEIF"),
        (b"\x00\x00\x00\x0cJXL \r\n\x87\n", "JXL"),
        (b"RIFF\x00\x00\x00\x00WEBP", "WEBP"),
        (b"  <svg xmlns='http://www.w3.org/2000/svg'/>", "SVG"),
    ],
)
def test_format_sniffing_prioritizes_container_magic(blob: bytes, expected: str) -> None:
    assert MediaRuntime._canonical_input_format("heifload_buffer", blob) == expected
