"""Tests for the filesystem-agnostic CSV record-splitting algorithm.

These exercise the core guarantee — chunk boundaries fall only between whole
records, so quoted multiline fields are never broken — without needing Spark.
"""
import csv
import io

from saltmill.splitter import Sink, build_dialect, split_records


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
