# SPDX-FileCopyrightText: 2024-2026 Klein Authors
#
# SPDX-License-Identifier: Apache-2.0
"""Lazy, resource-bounded image and PDF operations used by Klein SQL."""

from __future__ import annotations

import math
import operator
import os
import re
import warnings
from contextlib import suppress
from dataclasses import dataclass
from importlib import import_module
from io import BytesIO
from typing import Any

from ray.klein.api.sql_query_error import SQLQueryError

_UNSET = object()
_PDF_HEADER = b"%PDF-"
_EXIF_ORIENTATION = 274


@dataclass(frozen=True)
class MediaLimits:
    """Hard per-call and batch limits which protect workers from malformed media."""

    max_input_bytes: int = 256 * 1024 * 1024
    max_output_bytes: int = 256 * 1024 * 1024
    max_batch_bytes: int = 512 * 1024 * 1024
    max_image_pixels: int = 64_000_000
    max_pdf_pages: int = 100
    max_pdf_document_pages: int = 10_000
    max_pdf_dpi: float = 600.0

    def __post_init__(self) -> None:
        for name in (
            "max_input_bytes",
            "max_output_bytes",
            "max_batch_bytes",
            "max_image_pixels",
            "max_pdf_pages",
            "max_pdf_document_pages",
        ):
            value = getattr(self, name)
            if isinstance(value, bool):
                raise ValueError(f"{name} must be a positive integer")
            try:
                value = operator.index(value)
            except TypeError as error:
                raise ValueError(f"{name} must be a positive integer") from error
            if value <= 0:
                raise ValueError(f"{name} must be greater than zero")
            object.__setattr__(self, name, value)
        if isinstance(self.max_pdf_dpi, (bool, str, bytes)):
            raise ValueError("max_pdf_dpi must be a finite positive number")
        try:
            max_pdf_dpi = float(self.max_pdf_dpi)
        except (TypeError, ValueError) as error:
            raise ValueError("max_pdf_dpi must be a finite positive number") from error
        if not math.isfinite(max_pdf_dpi) or max_pdf_dpi <= 0:
            raise ValueError("max_pdf_dpi must be a finite positive number")
        object.__setattr__(self, "max_pdf_dpi", max_pdf_dpi)
        if self.max_pdf_pages > self.max_pdf_document_pages:
            raise ValueError("max_pdf_pages cannot exceed max_pdf_document_pages")


DEFAULT_MEDIA_LIMITS = MediaLimits()


@dataclass(frozen=True)
class _CacheKey:
    kind: str
    identity: int


@dataclass
class _ImageSource:
    backend: str
    image: Any
    stream: BytesIO | None
    data: bytes
    width: int
    height: int
    format: str

    def close(self) -> None:
        if self.backend == "pillow":
            self.image.close()
        if self.stream is not None:
            self.stream.close()


@dataclass
class _PdfiumSource:
    document: Any
    data: bytes
    page_count: int

    def close(self) -> None:
        self.document.close()


@dataclass
class _PypdfSource:
    reader: Any
    stream: BytesIO
    page_count: int

    def close(self) -> None:
        close = getattr(self.reader, "close", None)
        if close is not None:
            close()
        self.stream.close()


@dataclass(frozen=True)
class _OutputFormat:
    extension: str
    pillow_name: str
    accepts_quality: bool = False


_OUTPUT_FORMATS = {
    "AVIF": _OutputFormat("avif", "AVIF", True),
    "BMP": _OutputFormat("bmp", "BMP"),
    "GIF": _OutputFormat("gif", "GIF"),
    "HEIF": _OutputFormat("heif", "HEIF", True),
    "ICO": _OutputFormat("ico", "ICO"),
    "JPEG": _OutputFormat("jpg", "JPEG", True),
    "JPEG2000": _OutputFormat("jp2", "JPEG2000", True),
    "JXL": _OutputFormat("jxl", "JXL", True),
    "PNG": _OutputFormat("png", "PNG"),
    "PPM": _OutputFormat("ppm", "PPM"),
    "TIFF": _OutputFormat("tiff", "TIFF"),
    "WEBP": _OutputFormat("webp", "WEBP", True),
}
_OUTPUT_FORMAT_ALIASES = {
    "J2K": "JPEG2000",
    "JP2": "JPEG2000",
    "JPG": "JPEG",
    "TIF": "TIFF",
}
_FORMAT_PREFIXES: tuple[tuple[tuple[bytes, ...], str], ...] = (
    ((b"\xff\xd8\xff",), "JPEG"),
    ((b"\x89PNG\r\n\x1a\n",), "PNG"),
    ((b"GIF87a", b"GIF89a"), "GIF"),
    ((b"II*\x00", b"MM\x00*"), "TIFF"),
    ((b"BM",), "BMP"),
    ((b"\x00\x00\x01\x00",), "ICO"),
    ((b"\x00\x00\x00\x0cjP  \r\n\x87\n", b"\xffO\xffQ"), "JPEG2000"),
    ((b"\xff\x0a", b"\x00\x00\x00\x0cJXL \r\n\x87\n"), "JXL"),
    ((b"qoif",), "QOI"),
    ((b"v/1\x01",), "EXR"),
)


class MediaRuntime:
    """Execute image and PDF functions with lazy native backends.

    A runtime is intended to live for the lifetime of a Ray batch worker.  Pass a
    per-row ``cache`` to :meth:`execute` to reuse parsed image/PDF handles across
    several expressions, then call :meth:`clear_cache` in a ``finally`` block.
    """

    def __init__(self, limits: MediaLimits = DEFAULT_MEDIA_LIMITS, native_threads: int | None = None) -> None:
        if native_threads is not None:
            if isinstance(native_threads, bool):
                raise ValueError("native_threads must be a positive integer or None")
            try:
                native_threads = operator.index(native_threads)
            except TypeError as error:
                raise ValueError("native_threads must be a positive integer or None") from error
            if native_threads <= 0:
                raise ValueError("native_threads must be a positive integer or None")
        self._limits = limits
        self._native_threads = native_threads
        self._pyvips: Any = _UNSET
        self._pillow: Any = _UNSET
        self._pillow_ops: Any = _UNSET
        self._pypdf: Any = _UNSET
        self._pdfium: Any = _UNSET

    @property
    def limits(self) -> MediaLimits:
        return self._limits

    def execute(self, name: str, args: tuple[Any, ...], cache: dict[Any, Any] | None = None) -> Any:
        """Execute one strict media operation by its SQL name."""
        operation = name.upper()
        if any(argument is None for argument in args):
            return None
        if operation == "IMAGE_WIDTH":
            self._require_arity(operation, args, 1)
            return self.image_width(args[0], cache)
        if operation == "IMAGE_HEIGHT":
            self._require_arity(operation, args, 1)
            return self.image_height(args[0], cache)
        if operation == "IMAGE_FORMAT":
            self._require_arity(operation, args, 1)
            return self.image_format(args[0], cache)
        if operation == "IMAGE_RESIZE":
            self._require_arity(operation, args, 3, 6)
            return self.image_resize(
                args[0],
                args[1],
                args[2],
                args[3] if len(args) >= 4 else "contain",
                args[4] if len(args) >= 5 else "PNG",
                args[5] if len(args) >= 6 else 85,
                cache,
            )
        if operation == "PDF_PAGE_COUNT":
            self._require_arity(operation, args, 1)
            return self.pdf_page_count(args[0], cache)
        if operation == "PDF_SPLIT":
            self._require_arity(operation, args, 1, 3)
            return self.pdf_split(
                args[0],
                args[1] if len(args) >= 2 else 1,
                args[2] if len(args) >= 3 else None,
                cache,
            )
        if operation == "PDF_RENDER_PAGE":
            self._require_arity(operation, args, 2, 3)
            return self.pdf_render_page(args[0], args[1], args[2] if len(args) == 3 else 144, cache)
        if operation == "PDF_TO_IMAGES":
            self._require_arity(operation, args, 1, 4)
            return self.pdf_to_images(
                args[0],
                args[1] if len(args) >= 2 else 144,
                args[2] if len(args) >= 3 else 1,
                args[3] if len(args) >= 4 else None,
                cache,
            )
        raise SQLQueryError(f"Unsupported SQL media function {name!r}")

    def image_width(self, data: Any, cache: dict[Any, Any] | None = None) -> int:
        source, owned = self._acquire_image(data, "IMAGE_WIDTH", cache)
        try:
            return source.width
        finally:
            if owned:
                source.close()

    def image_height(self, data: Any, cache: dict[Any, Any] | None = None) -> int:
        source, owned = self._acquire_image(data, "IMAGE_HEIGHT", cache)
        try:
            return source.height
        finally:
            if owned:
                source.close()

    def image_format(self, data: Any, cache: dict[Any, Any] | None = None) -> str:
        source, owned = self._acquire_image(data, "IMAGE_FORMAT", cache)
        try:
            return source.format
        finally:
            if owned:
                source.close()

    def image_resize(
        self,
        data: Any,
        width: Any,
        height: Any,
        fit: Any = "contain",
        output_format: Any = "PNG",
        quality: Any = 85,
        cache: dict[Any, Any] | None = None,
    ) -> bytes:
        """Resize an image and return encoded bytes.

        ``contain`` preserves the aspect ratio inside the requested box,
        ``cover`` fills and center-crops it, and ``stretch`` returns exactly the
        requested dimensions.
        """
        target_width = self._positive_integer(width, "IMAGE_RESIZE width")
        target_height = self._positive_integer(height, "IMAGE_RESIZE height")
        self._check_pixels(target_width, target_height, "IMAGE_RESIZE output")
        fit_name = self._fit_name(fit)
        format_name, format_spec = self._output_format(output_format)
        self._validate_output_dimensions(format_name, target_width, target_height)
        quality_value = self._quality(quality)
        source, owned = self._acquire_image(data, "IMAGE_RESIZE", cache)
        try:
            if source.backend == "vips":
                try:
                    result = self._resize_vips(
                        source,
                        target_width,
                        target_height,
                        fit_name,
                        format_name,
                        format_spec,
                        quality_value,
                    )
                except SQLQueryError:
                    raise
                except Exception as vips_error:
                    pillow = self._load_pillow()
                    if pillow is None:
                        raise self._failure("IMAGE_RESIZE", vips_error) from vips_error
                    fallback = self._open_pillow_image(source.data, "IMAGE_RESIZE")
                    try:
                        result = self._resize_pillow(
                            fallback,
                            target_width,
                            target_height,
                            fit_name,
                            format_name,
                            format_spec,
                            quality_value,
                        )
                    except SQLQueryError:
                        raise
                    except Exception as pillow_error:
                        raise self._failure("IMAGE_RESIZE", pillow_error) from pillow_error
                    finally:
                        fallback.close()
            else:
                try:
                    result = self._resize_pillow(
                        source,
                        target_width,
                        target_height,
                        fit_name,
                        format_name,
                        format_spec,
                        quality_value,
                    )
                except SQLQueryError:
                    raise
                except Exception as error:
                    raise self._failure("IMAGE_RESIZE", error) from error
            self._check_output_size(result, "IMAGE_RESIZE")
            return result
        finally:
            if owned:
                source.close()

    def pdf_page_count(self, data: Any, cache: dict[Any, Any] | None = None) -> int:
        source, owned = self._acquire_pdfium(data, "PDF_PAGE_COUNT", cache)
        try:
            return source.page_count
        finally:
            if owned:
                source.close()

    def pdf_split(
        self,
        data: Any,
        start_page: Any = 1,
        end_page: Any | None = None,
        cache: dict[Any, Any] | None = None,
    ) -> list[bytes]:
        """Return one single-page PDF per page in the inclusive 1-based range."""
        source, owned = self._acquire_pypdf(data, "PDF_SPLIT", cache)
        try:
            page_indices = self._page_range(source.page_count, start_page, end_page, "PDF_SPLIT")
            results: list[bytes] = []
            total_bytes = 0
            for page_index in page_indices:
                writer = self._pypdf.PdfWriter()
                output = BytesIO()
                try:
                    writer.add_page(source.reader.pages[page_index])
                    writer.write(output)
                    value = output.getvalue()
                except SQLQueryError:
                    raise
                except Exception as error:
                    raise self._failure("PDF_SPLIT", error) from error
                finally:
                    close = getattr(writer, "close", None)
                    if close is not None:
                        close()
                    output.close()
                total_bytes += len(value)
                self._check_output_size(total_bytes, "PDF_SPLIT")
                results.append(value)
            return results
        finally:
            if owned:
                source.close()

    def pdf_render_page(
        self,
        data: Any,
        page: Any,
        dpi: Any = 144,
        cache: dict[Any, Any] | None = None,
    ) -> bytes:
        """Render one 1-based PDF page to PNG bytes."""
        source, owned = self._acquire_pdfium(data, "PDF_RENDER_PAGE", cache)
        try:
            page_number = self._page_number(page, source.page_count, "PDF_RENDER_PAGE")
            dpi_value = self._dpi(dpi, "PDF_RENDER_PAGE")
            result = self._render_pdfium_page(source, page_number - 1, dpi_value, "PDF_RENDER_PAGE")
            self._check_output_size(result, "PDF_RENDER_PAGE")
            return result
        finally:
            if owned:
                source.close()

    def pdf_to_images(
        self,
        data: Any,
        dpi: Any = 144,
        start_page: Any = 1,
        end_page: Any | None = None,
        cache: dict[Any, Any] | None = None,
    ) -> list[bytes]:
        """Render an inclusive 1-based PDF page range to PNG byte strings."""
        source, owned = self._acquire_pdfium(data, "PDF_TO_IMAGES", cache)
        try:
            dpi_value = self._dpi(dpi, "PDF_TO_IMAGES")
            page_indices = self._page_range(source.page_count, start_page, end_page, "PDF_TO_IMAGES")
            results: list[bytes] = []
            total_bytes = 0
            for page_index in page_indices:
                value = self._render_pdfium_page(source, page_index, dpi_value, "PDF_TO_IMAGES")
                total_bytes += len(value)
                self._check_output_size(total_bytes, "PDF_TO_IMAGES")
                results.append(value)
            return results
        finally:
            if owned:
                source.close()

    @staticmethod
    def clear_cache(cache: dict[Any, Any]) -> None:
        """Close and remove only media handles from a shared per-row cache."""
        for key in tuple(cache):
            if not isinstance(key, _CacheKey):
                continue
            value = cache.pop(key)
            with suppress(Exception):
                value.close()

    @staticmethod
    def _require_arity(operation: str, args: tuple[Any, ...], minimum: int, maximum: int | None = None) -> None:
        upper = minimum if maximum is None else maximum
        if minimum <= len(args) <= upper:
            return
        expected = f"exactly {minimum} argument(s)" if minimum == upper else f"between {minimum} and {upper} arguments"
        raise SQLQueryError(f"{operation} requires {expected}")

    def _acquire_image(
        self,
        data: Any,
        operation: str,
        cache: dict[Any, Any] | None,
    ) -> tuple[_ImageSource, bool]:
        key = _CacheKey("image", id(data))
        if cache is not None and key in cache:
            return cache[key], False
        blob = self._binary(data, operation)
        source = self._open_image(blob, operation)
        if cache is not None:
            cache[key] = source
            return source, False
        return source, True

    def _open_image(self, blob: bytes, operation: str) -> _ImageSource:
        vips_error: Exception | None = None
        pyvips = self._load_pyvips()
        if pyvips is not None:
            try:
                image = pyvips.Image.new_from_buffer(blob, "", access="sequential")
                image = image.autorot()
                width, height = int(image.width), int(image.height)
                self._check_pixels(width, height, f"{operation} input")
                return _ImageSource("vips", image, None, blob, width, height, self._vips_format(image, blob))
            except SQLQueryError:
                raise
            except Exception as error:
                vips_error = error
        if self._load_pillow() is not None:
            try:
                return self._open_pillow_image(blob, operation)
            except SQLQueryError:
                raise
            except Exception as error:
                raise self._failure(operation, error, "failed to decode image") from error
        if vips_error is not None:
            raise self._failure(operation, vips_error, "failed to decode image") from vips_error
        raise self._missing_dependency(operation, "pyvips or Pillow")

    def _open_pillow_image(self, blob: bytes, operation: str) -> _ImageSource:
        image_module = self._load_pillow()
        if image_module is None:
            raise self._missing_dependency(operation, "Pillow")
        stream = BytesIO(blob)
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("error", image_module.DecompressionBombWarning)
                image = image_module.open(stream)
            raw_width, raw_height = image.size
            self._check_pixels(raw_width, raw_height, f"{operation} input")
            orientation = image.getexif().get(_EXIF_ORIENTATION, 1)
            width, height = (raw_height, raw_width) if orientation in {5, 6, 7, 8} else (raw_width, raw_height)
            format_name = self._canonical_input_format(image.format, blob)
            return _ImageSource("pillow", image, stream, blob, width, height, format_name)
        except Exception:
            stream.close()
            raise

    def _resize_vips(
        self,
        source: _ImageSource,
        target_width: int,
        target_height: int,
        fit: str,
        format_name: str,
        format_spec: _OutputFormat,
        quality: int,
    ) -> bytes:
        try:
            options: dict[str, Any] = {"height": target_height, "size": "force" if fit == "stretch" else "both"}
            if fit == "cover":
                options["crop"] = "centre"
            resized = self._pyvips.Image.thumbnail_buffer(source.data, target_width, **options)
        except Exception:
            resized = self._resize_loaded_vips(source, target_width, target_height, fit)
        resized = self._prepare_vips_for_output(resized, format_name)
        options = {}
        if format_spec.accepts_quality:
            options["Q"] = quality
        return bytes(resized.write_to_buffer(f".{format_spec.extension}", **options))

    @staticmethod
    def _resize_loaded_vips(source: _ImageSource, target_width: int, target_height: int, fit: str) -> Any:
        image = source.image
        if fit == "stretch":
            return image.resize(
                target_width / source.width,
                vscale=target_height / source.height,
                kernel="lanczos3",
            )
        if fit == "contain":
            scale = min(target_width / source.width, target_height / source.height)
            return image.resize(scale, kernel="lanczos3")
        scale = max(target_width / source.width, target_height / source.height)
        resized = image.resize(scale, kernel="lanczos3")
        if resized.width < target_width or resized.height < target_height:
            correction = max(target_width / resized.width, target_height / resized.height)
            resized = resized.resize(correction, kernel="lanczos3")
        left = max(0, (resized.width - target_width) // 2)
        top = max(0, (resized.height - target_height) // 2)
        return resized.crop(left, top, target_width, target_height)

    @staticmethod
    def _prepare_vips_for_output(image: Any, format_name: str) -> Any:
        if format_name not in {"BMP", "JPEG", "PPM"}:
            return image
        has_alpha = getattr(image, "hasalpha", None)
        if has_alpha is not None and has_alpha():
            image = image.flatten(background=[255, 255, 255])
        if image.bands > 3:
            image = image.extract_band(0, n=3)
        return image

    def _resize_pillow(
        self,
        source: _ImageSource,
        target_width: int,
        target_height: int,
        fit: str,
        format_name: str,
        format_spec: _OutputFormat,
        quality: int,
    ) -> bytes:
        image_module = self._load_pillow()
        image_ops = self._load_pillow_ops()
        if image_module is None or image_ops is None:
            raise self._missing_dependency("IMAGE_RESIZE", "Pillow")
        oriented = image_ops.exif_transpose(source.image)
        try:
            method = image_module.Resampling.LANCZOS
            if fit == "stretch":
                resized = oriented.resize((target_width, target_height), resample=method)
            elif fit == "contain":
                resized = image_ops.contain(oriented, (target_width, target_height), method=method)
            else:
                resized = image_ops.fit(oriented, (target_width, target_height), method=method, centering=(0.5, 0.5))
            try:
                prepared = self._prepare_pillow_for_output(resized, format_name, image_module)
                output = BytesIO()
                try:
                    options: dict[str, Any] = {}
                    if format_spec.accepts_quality:
                        options["quality"] = quality
                    if format_name == "ICO":
                        options["sizes"] = [prepared.size]
                    prepared.save(output, format=format_spec.pillow_name, **options)
                    return output.getvalue()
                finally:
                    output.close()
                    if prepared is not resized:
                        prepared.close()
            finally:
                if resized is not oriented:
                    resized.close()
        finally:
            if oriented is not source.image:
                oriented.close()

    @staticmethod
    def _prepare_pillow_for_output(image: Any, format_name: str, image_module: Any) -> Any:
        if format_name == "GIF":
            return image.convert("P", palette=image_module.Palette.ADAPTIVE)
        if format_name not in {"BMP", "JPEG", "PPM"}:
            return image
        if image.mode in {"RGBA", "LA"} or (image.mode == "P" and "transparency" in image.info):
            rgba = image.convert("RGBA")
            background = image_module.new("RGB", rgba.size, (255, 255, 255))
            background.paste(rgba, mask=rgba.getchannel("A"))
            rgba.close()
            return background
        return image if image.mode in {"L", "RGB"} else image.convert("RGB")

    def _acquire_pdfium(
        self,
        data: Any,
        operation: str,
        cache: dict[Any, Any] | None,
    ) -> tuple[_PdfiumSource, bool]:
        key = _CacheKey("pdfium", id(data))
        if cache is not None and key in cache:
            return cache[key], False
        blob = self._pdf_binary(data, operation)
        pdfium = self._load_pdfium()
        if pdfium is None:
            raise self._missing_dependency(operation, "pypdfium2")
        document = None
        try:
            document = pdfium.PdfDocument(blob)
            page_count = len(document)
            self._check_document_pages(page_count, operation)
            source = _PdfiumSource(document, blob, page_count)
        except SQLQueryError:
            if document is not None:
                document.close()
            raise
        except Exception as error:
            if document is not None:
                document.close()
            raise self._failure(operation, error, "failed to read PDF") from error
        if cache is not None:
            cache[key] = source
            return source, False
        return source, True

    def _acquire_pypdf(
        self,
        data: Any,
        operation: str,
        cache: dict[Any, Any] | None,
    ) -> tuple[_PypdfSource, bool]:
        key = _CacheKey("pypdf", id(data))
        if cache is not None and key in cache:
            return cache[key], False
        blob = self._pdf_binary(data, operation)
        pypdf = self._load_pypdf()
        if pypdf is None:
            raise self._missing_dependency(operation, "pypdf")
        stream = BytesIO(blob)
        try:
            reader = pypdf.PdfReader(stream, strict=False)
            if reader.is_encrypted:
                raise SQLQueryError(f"{operation} does not support encrypted PDF inputs")
            page_count = len(reader.pages)
            self._check_document_pages(page_count, operation)
            source = _PypdfSource(reader, stream, page_count)
        except SQLQueryError:
            stream.close()
            raise
        except Exception as error:
            stream.close()
            raise self._failure(operation, error, "failed to read PDF") from error
        if cache is not None:
            cache[key] = source
            return source, False
        return source, True

    def _render_pdfium_page(self, source: _PdfiumSource, page_index: int, dpi: float, operation: str) -> bytes:
        try:
            page = source.document[page_index]
            try:
                width_points, height_points = page.get_size()
                scale = dpi / 72.0
                width = max(1, math.ceil(width_points * scale))
                height = max(1, math.ceil(height_points * scale))
                self._check_pixels(width, height, f"{operation} output")
                bitmap = page.render(scale=scale)
                try:
                    image = bitmap.to_pil()
                    try:
                        output = BytesIO()
                        try:
                            image.save(output, format="PNG")
                            return output.getvalue()
                        finally:
                            output.close()
                    finally:
                        image.close()
                finally:
                    bitmap.close()
            finally:
                page.close()
        except SQLQueryError:
            raise
        except Exception as error:
            raise self._failure(operation, error) from error

    def _binary(self, data: Any, operation: str) -> bytes:
        if isinstance(data, bytes):
            value = data
        elif isinstance(data, bytearray):
            value = bytes(data)
        elif isinstance(data, memoryview):
            value = data.tobytes()
        else:
            raise SQLQueryError(f"{operation} input must be binary data")
        if not value:
            raise SQLQueryError(f"{operation} input must not be empty")
        if len(value) > self._limits.max_input_bytes:
            raise SQLQueryError(f"{operation} input exceeds the {self._limits.max_input_bytes}-byte safety limit")
        return value

    def _pdf_binary(self, data: Any, operation: str) -> bytes:
        value = self._binary(data, operation)
        if _PDF_HEADER not in value[:1024]:
            raise SQLQueryError(f"{operation} input is not a PDF document")
        return value

    def _check_pixels(self, width: int, height: int, label: str) -> None:
        if width <= 0 or height <= 0:
            raise SQLQueryError(f"{label} has invalid dimensions")
        if width * height > self._limits.max_image_pixels:
            raise SQLQueryError(f"{label} exceeds the {self._limits.max_image_pixels}-pixel safety limit")

    def _check_output_size(self, value: bytes | int, operation: str) -> None:
        size = value if isinstance(value, int) else len(value)
        if size > self._limits.max_output_bytes:
            raise SQLQueryError(f"{operation} output exceeds the {self._limits.max_output_bytes}-byte safety limit")

    def _check_document_pages(self, page_count: int, operation: str) -> None:
        if page_count <= 0:
            raise SQLQueryError(f"{operation} PDF document has no pages")
        if page_count > self._limits.max_pdf_document_pages:
            raise SQLQueryError(
                f"{operation} PDF exceeds the {self._limits.max_pdf_document_pages}-page document safety limit"
            )

    def _page_range(self, page_count: int, start: Any, end: Any | None, operation: str) -> range:
        first = self._positive_integer(start, f"{operation} start page")
        last = page_count if end is None else self._positive_integer(end, f"{operation} end page")
        if first > last:
            raise SQLQueryError(f"{operation} start page cannot exceed end page")
        if last > page_count:
            raise SQLQueryError(f"{operation} page range exceeds the {page_count}-page document")
        selected = last - first + 1
        if selected > self._limits.max_pdf_pages:
            raise SQLQueryError(
                f"{operation} selects {selected} pages, exceeding the {self._limits.max_pdf_pages}-page safety limit"
            )
        return range(first - 1, last)

    def _page_number(self, value: Any, page_count: int, operation: str) -> int:
        page = self._positive_integer(value, f"{operation} page")
        if page > page_count:
            raise SQLQueryError(f"{operation} page {page} exceeds the {page_count}-page document")
        return page

    def _dpi(self, value: Any, operation: str) -> float:
        if isinstance(value, bool):
            raise SQLQueryError(f"{operation} DPI must be a number")
        try:
            dpi = float(value)
        except (TypeError, ValueError) as error:
            raise SQLQueryError(f"{operation} DPI must be a number") from error
        if not math.isfinite(dpi) or dpi <= 0 or dpi > self._limits.max_pdf_dpi:
            raise SQLQueryError(f"{operation} DPI must be greater than zero and at most {self._limits.max_pdf_dpi:g}")
        return dpi

    @staticmethod
    def _positive_integer(value: Any, label: str) -> int:
        if isinstance(value, bool):
            raise SQLQueryError(f"{label} must be a positive integer")
        try:
            result = operator.index(value)
        except TypeError as error:
            raise SQLQueryError(f"{label} must be a positive integer") from error
        if result <= 0:
            raise SQLQueryError(f"{label} must be a positive integer")
        return result

    @staticmethod
    def _fit_name(value: Any) -> str:
        if not isinstance(value, str):
            raise SQLQueryError("IMAGE_RESIZE fit must be CONTAIN, COVER, or STRETCH")
        fit = value.lower()
        if fit not in {"contain", "cover", "stretch"}:
            raise SQLQueryError("IMAGE_RESIZE fit must be CONTAIN, COVER, or STRETCH")
        return fit

    @staticmethod
    def _output_format(value: Any) -> tuple[str, _OutputFormat]:
        if not isinstance(value, str):
            raise SQLQueryError("IMAGE_RESIZE output format must be a string")
        name = value.upper().replace("-", "")
        name = _OUTPUT_FORMAT_ALIASES.get(name, name)
        try:
            return name, _OUTPUT_FORMATS[name]
        except KeyError as error:
            supported = ", ".join(_OUTPUT_FORMATS)
            raise SQLQueryError(
                f"IMAGE_RESIZE unsupported output format {value!r}; supported formats: {supported}"
            ) from error

    @staticmethod
    def _validate_output_dimensions(format_name: str, width: int, height: int) -> None:
        if format_name == "ICO" and (width > 256 or height > 256):
            raise SQLQueryError("IMAGE_RESIZE ICO output dimensions must not exceed 256 by 256 pixels")

    @staticmethod
    def _quality(value: Any) -> int:
        quality = MediaRuntime._positive_integer(value, "IMAGE_RESIZE quality")
        if quality > 100:
            raise SQLQueryError("IMAGE_RESIZE quality must be between 1 and 100")
        return quality

    def _load_pyvips(self) -> Any | None:
        if self._pyvips is _UNSET:
            try:
                if self._native_threads is not None:
                    os.environ.setdefault("VIPS_CONCURRENCY", str(self._native_threads))
                self._pyvips = import_module("pyvips")
            except (ImportError, OSError):
                self._pyvips = None
        return self._pyvips

    def _load_pillow(self) -> Any | None:
        if self._pillow is _UNSET:
            try:
                self._pillow = import_module("PIL.Image")
            except ImportError:
                self._pillow = None
        return self._pillow

    def _load_pillow_ops(self) -> Any | None:
        if self._pillow_ops is _UNSET:
            try:
                self._pillow_ops = import_module("PIL.ImageOps")
            except ImportError:
                self._pillow_ops = None
        return self._pillow_ops

    def _load_pypdf(self) -> Any | None:
        if self._pypdf is _UNSET:
            try:
                self._pypdf = import_module("pypdf")
            except ImportError:
                self._pypdf = None
        return self._pypdf

    def _load_pdfium(self) -> Any | None:
        if self._pdfium is _UNSET:
            try:
                self._pdfium = import_module("pypdfium2")
            except (ImportError, OSError):
                self._pdfium = None
        return self._pdfium

    @staticmethod
    def _vips_format(image: Any, blob: bytes) -> str:
        loader = ""
        try:
            if image.get_typeof("vips-loader"):
                loader = str(image.get("vips-loader"))
        except Exception:
            pass
        return MediaRuntime._canonical_input_format(loader, blob)

    @staticmethod
    def _canonical_input_format(value: Any, blob: bytes) -> str:
        detected = MediaRuntime._sniff_format(blob)
        if detected is not None:
            return detected
        candidate = str(value or "").upper()
        for token, canonical in (
            ("JPEG", "JPEG"),
            ("JPG", "JPEG"),
            ("PNG", "PNG"),
            ("WEBP", "WEBP"),
            ("GIF", "GIF"),
            ("TIFF", "TIFF"),
            ("HEIF", "HEIF"),
            ("HEIC", "HEIF"),
            ("AVIF", "AVIF"),
            ("JXL", "JXL"),
            ("JP2", "JPEG2000"),
            ("JPEG2000", "JPEG2000"),
            ("SVG", "SVG"),
            ("EXR", "EXR"),
            ("BMP", "BMP"),
            ("ICO", "ICO"),
            ("PPM", "PPM"),
            ("PDF", "PDF"),
        ):
            if token in candidate:
                return canonical
        normalized = re.sub(r"(?:FOREIGN|LOAD|BUFFER|FILE)", "", candidate).strip("_- ")
        return normalized or "UNKNOWN"

    @staticmethod
    def _sniff_format(blob: bytes) -> str | None:
        for prefixes, format_name in _FORMAT_PREFIXES:
            if blob.startswith(prefixes):
                return format_name
        if len(blob) >= 12 and blob[4:8] == b"ftyp":
            brand = blob[8:12]
            if brand in {b"avif", b"avis"}:
                return "AVIF"
            if brand in {b"heic", b"heix", b"hevc", b"hevx", b"mif1", b"msf1"}:
                return "HEIF"
        if len(blob) >= 12 and blob[:4] == b"RIFF" and blob[8:12] == b"WEBP":
            return "WEBP"
        if len(blob) >= 2 and blob[:1] == b"P" and blob[1:2] in b"1234567":
            return "PPM"
        prefix = blob[:1024].lstrip()
        if prefix.startswith(_PDF_HEADER):
            return "PDF"
        if b"<svg" in prefix.lower():
            return "SVG"
        return None

    @staticmethod
    def _missing_dependency(operation: str, dependency: str) -> SQLQueryError:
        return SQLQueryError(f"{operation} requires optional dependency {dependency}; install 'ray-klein[media]'")

    @staticmethod
    def _failure(operation: str, error: Exception, reason: str = "failed") -> SQLQueryError:
        return SQLQueryError(f"{operation} {reason} with {type(error).__name__}")
