"""Tests for the filesystem-agnostic CSV record-splitting algorithm.

These exercise the core guarantee — chunk boundaries fall only between whole
records, so quoted multiline fields are never broken — without needing Spark.
"""
import csv
import io

import pytest

from saltmill.config import SaltmillConfig
from saltmill.exceptions import ConfigurationError
from saltmill.splitter import (
    Sink,
    build_dialect,
    plan_split,
    projected_chunk_count,
    split_records,
)

_GB = 1024 ** 3


def _cfg(**kw):
    base = dict(input_path="/data/x.csv", csv_options={"header": "true", "multiLine": "true"})
    base.update(kw)
    return SaltmillConfig(**base)


class _StringSink(Sink):
    """In-memory chunk sink; records raw bytes written."""

    def __init__(self) -> None:
        self.data = b""

    def write_row(self, data: bytes) -> int:
        self.data += data
        return len(data)

    def close(self) -> None:
        pass


def _run(text: str, target_bytes: int, has_header: bool, csv_options=None):
    sinks: list[_StringSink] = []

    def make_sink(index: int) -> Sink:
        s = _StringSink()
        sinks.append(s)
        return s

    dialect = build_dialect(csv_options or {})
    count = split_records(
        io.StringIO(text), make_sink, target_bytes, has_header, dialect
    )
    return count, sinks


def _parse(sink: _StringSink, has_header: bool, csv_options=None):
    """Re-parse a chunk's bytes back into rows (excluding header)."""
    dialect = build_dialect(csv_options or {})
    text = sink.data.decode("utf-8")
    rows = list(csv.reader(io.StringIO(text), **dialect))
    return rows[1:] if has_header else rows


def test_multiline_record_not_broken():
    # The second record's first field contains an embedded newline.
    text = (
        'id,note,city\n'
        '1,"line one\nline two",NYC\n'
        '2,"plain",LA\n'
        '3,"another\nmulti\nline",SF\n'
    )
    # Tiny target forces a rotation after almost every record.
    count, sinks = _run(text, target_bytes=10, has_header=True)
    assert count >= 1

    # Reassemble all data rows across chunks and verify nothing split.
    all_rows = []
    for s in sinks:
        all_rows.extend(_parse(s, has_header=True))
    assert all_rows == [
        ["1", "line one\nline two", "NYC"],
        ["2", "plain", "LA"],
        ["3", "another\nmulti\nline", "SF"],
    ]


def test_header_replicated_to_every_chunk():
    text = "id,val\n" + "".join(f"{i},x\n" for i in range(20))
    count, sinks = _run(text, target_bytes=8, has_header=True)
    assert count > 1  # ensure we actually rotated
    for s in sinks:
        first_line = s.data.decode("utf-8").splitlines()[0]
        assert first_line == "id,val"


def test_all_rows_preserved_across_chunks():
    rows = [f"{i},name{i},{i * 10}" for i in range(100)]
    text = "id,name,amount\n" + "\n".join(rows) + "\n"
    count, sinks = _run(text, target_bytes=64, has_header=True)

    recovered = []
    for s in sinks:
        recovered.extend(_parse(s, has_header=True))
    assert len(recovered) == 100
    assert recovered[0] == ["0", "name0", "0"]
    assert recovered[-1] == ["99", "name99", "990"]


def test_no_header_mode():
    text = "1,a\n2,b\n3,c\n"
    count, sinks = _run(text, target_bytes=4, has_header=False)
    recovered = []
    for s in sinks:
        recovered.extend(_parse(s, has_header=False))
    assert recovered == [["1", "a"], ["2", "b"], ["3", "c"]]


def test_single_chunk_when_under_target():
    text = "id,v\n1,a\n2,b\n"
    count, sinks = _run(text, target_bytes=10_000, has_header=True)
    assert count == 1


def test_custom_delimiter():
    text = "id;city\n1;NYC\n2;LA\n"
    count, sinks = _run(text, target_bytes=6, has_header=True, csv_options={"sep": ";"})
    recovered = []
    for s in sinks:
        recovered.extend(_parse(s, has_header=True, csv_options={"sep": ";"}))
    assert recovered == [["1", "NYC"], ["2", "LA"]]


def test_quote_inside_field_preserved():
    text = 'id,note\n1,"she said ""hi"" today"\n'
    count, sinks = _run(text, target_bytes=10_000, has_header=True)
    recovered = _parse(sinks[0], has_header=True)
    assert recovered == [["1", 'she said "hi" today']]


def test_empty_input_produces_no_chunks():
    count, sinks = _run("", target_bytes=100, has_header=False)
    assert count == 0
    assert sinks == []


def test_header_only_input_produces_no_chunks():
    # Header present but zero data rows → nothing to write.
    count, sinks = _run("id,v\n", target_bytes=100, has_header=True)
    assert count == 0


# ── Split-decision guardrails (pure, no Spark) ────────────────────────────────

def test_plan_split_single_large_multiline_splits():
    action, path, size = plan_split(_cfg(), [("/data/big.csv", int(3 * _GB))])
    assert action == "split"
    assert path == "/data/big.csv"
    assert size == int(3 * _GB)


def test_plan_split_multi_file_skips():
    files = [("/data/a.csv", int(3 * _GB)), ("/data/b.csv", int(3 * _GB))]
    assert plan_split(_cfg(), files)[0] == "skip"


def test_plan_split_non_multiline_skips():
    cfg = _cfg(csv_options={"header": "true", "multiLine": "false"})
    assert plan_split(cfg, [("/data/big.csv", int(3 * _GB))])[0] == "skip"


def test_plan_split_below_threshold_skips():
    cfg = _cfg(split_threshold_gb=5.0)
    assert plan_split(cfg, [("/data/big.csv", int(3 * _GB))])[0] == "skip"


def test_plan_split_disabled_skips():
    cfg = _cfg(split_large_files=False)
    assert plan_split(cfg, [("/data/big.csv", int(3 * _GB))])[0] == "skip"


def test_plan_split_above_max_file_raises():
    cfg = _cfg(split_max_file_gb=10.0)
    with pytest.raises(ConfigurationError, match="above split_max_file_gb"):
        plan_split(cfg, [("/data/huge.csv", int(20 * _GB))])


def test_projected_chunk_count():
    assert projected_chunk_count(0, 100) == 1
    assert projected_chunk_count(100, 100) == 1
    assert projected_chunk_count(101, 100) == 2
    assert projected_chunk_count(10 * _GB, 128 * 1024 * 1024) == 80


def test_projected_chunk_count_rejects_zero_target():
    with pytest.raises(ValueError):
        projected_chunk_count(100, 0)
