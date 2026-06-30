"""
Driver-side splitting of a single large CSV into many record-aligned chunks.

Spark cannot split a single CSV file across tasks when ``multiLine=true`` —
a record may span several physical lines, so there is no safe byte offset to
seek to. The whole file is read by one task, serialising the most expensive
stage of the pipeline.

This module solves that by streaming the file once on the driver with Python's
``csv`` reader (which tracks quote state and therefore never breaks a record),
re-emitting it as N smaller chunk files under a staging path. Spark then reads
the staging directory with full parallelism.

The record-splitting algorithm (:func:`split_records`) is filesystem-agnostic
and unit-testable; the Hadoop streaming wrappers live in :class:`FileSplitter`.
"""
from __future__ import annotations

import csv
import io
import logging
from typing import TYPE_CHECKING, Callable, Optional

from saltmill.exceptions import ConfigurationError

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from saltmill.config import SaltmillConfig

log = logging.getLogger("saltmill")

# Files Spark/commit protocols leave behind that are not CSV data.
_NON_DATA_PREFIXES = (".", "_")


def build_dialect(csv_options: dict[str, str]) -> dict:
    """Translate Spark CSV options into Python ``csv`` dialect kwargs."""
    delim = csv_options.get("sep") or csv_options.get("delimiter") or ","
    if len(delim) != 1:
        raise ConfigurationError(
            f"File splitting requires a single-character delimiter, got {delim!r}. "
            "Disable split_large_files or pre-split the input."
        )
    quote = csv_options.get("quote", '"')
    escape = csv_options.get("escape")
    dialect: dict = {"delimiter": delim, "quotechar": quote}
    if escape and escape != quote:
        # Distinct escape char (e.g. backslash) → no doubled-quote escaping.
        dialect["escapechar"] = escape
        dialect["doublequote"] = False
    else:
        # escape == quote (the RFC 4180 "" convention) or unset.
        dialect["doublequote"] = True
    return dialect


class _RowFormatter:
    """Serialise a single CSV row to bytes using the configured dialect."""

    def __init__(self, dialect: dict, encoding: str) -> None:
        self._encoding = encoding
        self._buf = io.StringIO()
        self._writer = csv.writer(self._buf, lineterminator="\n", **dialect)

    def to_bytes(self, row: list[str]) -> bytes:
        self._buf.seek(0)
        self._buf.truncate(0)
        self._writer.writerow(row)
        return self._buf.getvalue().encode(self._encoding)


class Sink:
    """A chunk destination. Subclasses persist bytes; here is the contract."""

    def write_row(self, data: bytes) -> int:  # pragma: no cover - interface
        raise NotImplementedError

    def close(self) -> None:  # pragma: no cover - interface
        raise NotImplementedError


def split_records(
    text_in: io.TextIOBase,
    make_sink: Callable[[int], Sink],
    target_bytes: int,
    has_header: bool,
    dialect: dict,
    encoding: str = "UTF-8",
) -> int:
    """
    Read CSV records from ``text_in`` and fan them out into chunk sinks.

    A chunk boundary is only taken *between* complete records, so a quoted
    multiline field is never split. Every chunk is prefixed with the header
    (when present) so each output file is independently readable by Spark.

    ``make_sink(index)`` is called to open each new chunk. Returns the number
    of chunks written. Caller owns sink lifecycle only insofar as this function
    closes each sink before opening the next and closes the final one.
    """
    if target_bytes <= 0:
        raise ValueError("target_bytes must be > 0")

    reader = csv.reader(text_in, **dialect)
    fmt = _RowFormatter(dialect, encoding)

    header_bytes: Optional[bytes] = None
    if has_header:
        header_row = next(reader, None)
        if header_row is not None:
            header_bytes = fmt.to_bytes(header_row)

    sink: Optional[Sink] = None
    written = 0
    chunk_count = 0

    def open_chunk() -> None:
        nonlocal sink, written, chunk_count
        if sink is not None:
            sink.close()
        sink = make_sink(chunk_count)
        chunk_count += 1
        written = 0
        if header_bytes is not None:
            written += sink.write_row(header_bytes)

    for row in reader:
        if sink is None or written >= target_bytes:
            open_chunk()
        assert sink is not None
        written += sink.write_row(fmt.to_bytes(row))

    if sink is not None:
        sink.close()

    return chunk_count


# ── Hadoop FileSystem I/O ───────────────────────────────────────────────────


class _HadoopRawReader(io.RawIOBase):
    """Read-only file-like over a Hadoop FSDataInputStream, block-buffered.

    Bytes are pulled from the JVM in large blocks (one py4j round-trip each)
    via commons-io ``IOUtils.toByteArray``, sized exactly so the final block
    never overruns EOF.
    """

    def __init__(self, jvm, stream, total_len: int, block_size: int) -> None:
        self._jvm = jvm
        self._stream = stream
        self._total = total_len
        self._block = block_size
        self._pos = 0  # bytes pulled from JVM so far
        self._buf = b""
        self._bufpos = 0

    def readable(self) -> bool:
        return True

    def readinto(self, b) -> int:  # type: ignore[override]
        if self._bufpos >= len(self._buf):
            remaining = self._total - self._pos
            if remaining <= 0:
                return 0
            n = min(self._block, remaining)
            jbytes = self._jvm.org.apache.commons.io.IOUtils.toByteArray(self._stream, int(n))
            self._buf = bytes(jbytes)
            self._bufpos = 0
            self._pos += len(self._buf)
            if not self._buf:
                return 0
        take = min(len(b), len(self._buf) - self._bufpos)
        b[: take] = self._buf[self._bufpos : self._bufpos + take]
        self._bufpos += take
        return take

    def close(self) -> None:
        try:
            self._stream.close()
        except Exception:
            log.debug("[saltmill] error closing Hadoop input stream", exc_info=True)
        super().close()


class _HadoopSink(Sink):
    """Writes chunk bytes to a Hadoop FSDataOutputStream."""

    def __init__(self, jvm, fs, path_obj) -> None:
        self._jvm = jvm
        # overwrite=True: staging dir is cleaned beforehand, but be defensive.
        self._stream = fs.create(path_obj, True)

    def write_row(self, data: bytes) -> int:
        self._stream.write(bytearray(data))
        return len(data)

    def close(self) -> None:
        try:
            self._stream.close()
        except Exception:
            log.debug("[saltmill] error closing Hadoop output stream", exc_info=True)


class FileSplitter:
    """Inspects an input path and, when warranted, splits one large multiline
    CSV file into record-aligned chunks under a staging directory."""

    # 4 MB read blocks: large enough that py4j overhead is negligible.
    _READ_BLOCK_BYTES = 4 * 1024 * 1024

    def __init__(self, spark: "SparkSession", config: "SaltmillConfig") -> None:
        self._spark = spark
        self._config = config
        self._jvm = spark._jvm  # type: ignore[attr-defined]
        sc = spark.sparkContext
        self._hadoop_conf = sc._jsc.hadoopConfiguration()  # type: ignore[attr-defined]

    def _fs_for(self, path_str: str):
        path_obj = self._jvm.org.apache.hadoop.fs.Path(path_str)
        return path_obj, path_obj.getFileSystem(self._hadoop_conf)

    def list_data_files(self, path_str: str) -> list[tuple[str, int]]:
        """Return (path, size_bytes) for real CSV data files at ``path_str``.

        Hidden/metadata files (``_SUCCESS``, ``.crc``, ``_committed_*``) and
        directories are excluded so a commit-protocol folder with one data
        file is correctly seen as single-file.
        """
        path_obj, fs = self._fs_for(path_str)
        if not fs.exists(path_obj):
            return []
        if fs.isDirectory(path_obj):
            statuses = fs.listStatus(path_obj)
        else:
            statuses = fs.globStatus(path_obj)
        if statuses is None:
            return []
        out: list[tuple[str, int]] = []
        for st in statuses:
            if st.isDirectory():
                continue
            name = st.getPath().getName()
            if name.startswith(_NON_DATA_PREFIXES):
                continue
            out.append((st.getPath().toString(), int(st.getLen())))
        return out

    def should_split(self, data_files: list[tuple[str, int]]) -> bool:
        """Split only when: enabled, exactly one data file, multiLine is on
        (Spark can't split it natively), and it exceeds the threshold."""
        cfg = self._config
        if not cfg.split_large_files:
            return False
        if len(data_files) != 1:
            # 0 → let the downstream read raise a clear error.
            # >1 → already parallelisable; trust Spark.
            return False
        multiline = str(cfg.csv_options.get("multiLine", "false")).lower() == "true"
        if not multiline:
            # Spark splits a single non-multiline CSV natively via maxPartitionBytes.
            return False
        size_gb = data_files[0][1] / (1024 ** 3)
        return size_gb >= cfg.split_threshold_gb

    def _resolve_staging_path(self) -> str:
        cfg = self._config
        if cfg.staging_path:
            return cfg.staging_path.rstrip("/")
        if cfg.checkpoint_path:
            return f"{cfg.checkpoint_path.rstrip('/')}/_saltmill_split"
        raise ConfigurationError(
            "A single large multiLine CSV needs splitting, but no staging location "
            "is configured. Set SaltmillConfig.staging_path or checkpoint_path."
        )

    def split(self, file_path: str, file_size: int) -> str:
        """Split ``file_path`` into chunks under the staging dir; return that dir."""
        cfg = self._config
        staging = self._resolve_staging_path()
        target_mb = cfg.target_chunk_size_mb or cfg.max_partition_bytes_mb
        target_bytes = target_mb * 1024 * 1024
        encoding = cfg.csv_options.get("encoding", "UTF-8")
        has_header = str(cfg.csv_options.get("header", "false")).lower() == "true"
        dialect = build_dialect(cfg.csv_options)

        # Fresh staging dir to avoid mixing stale chunks from a prior run.
        staging_obj, fs = self._fs_for(staging)
        if fs.exists(staging_obj):
            fs.delete(staging_obj, True)
        fs.mkdirs(staging_obj)

        in_obj, in_fs = self._fs_for(file_path)
        in_stream = in_fs.open(in_obj)
        raw = _HadoopRawReader(self._jvm, in_stream, file_size, self._READ_BLOCK_BYTES)
        text_in = io.TextIOWrapper(io.BufferedReader(raw), encoding=encoding, newline="")

        def make_sink(index: int) -> Sink:
            chunk_path = f"{staging}/part-{index:05d}.csv"
            chunk_obj, chunk_fs = self._fs_for(chunk_path)
            return _HadoopSink(self._jvm, chunk_fs, chunk_obj)

        log.info(
            "[saltmill] splitting single %.2f GB multiLine file into ~%d MB chunks at %s",
            file_size / (1024 ** 3), target_mb, staging,
        )
        try:
            chunk_count = split_records(
                text_in, make_sink, target_bytes, has_header, dialect, encoding
            )
        finally:
            text_in.close()

        log.info("[saltmill] split complete: %d chunk(s) written to %s", chunk_count, staging)
        return staging

    def maybe_split(self) -> Optional[str]:
        """If the configured input warrants splitting, do it and return the
        staging path; otherwise return None (caller keeps the original path)."""
        data_files = self.list_data_files(self._config.input_path)
        if not self.should_split(data_files):
            if len(data_files) > 1:
                log.info(
                    "[saltmill] %d input files detected; reading in parallel without splitting",
                    len(data_files),
                )
            return None
        path, size = data_files[0]
        return self.split(path, size)
