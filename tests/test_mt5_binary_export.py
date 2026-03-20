from datetime import UTC, datetime
from pathlib import Path
import shutil
import struct

from tools.mt5_binary_export import read_codex_binary


HEADER = struct.Struct('<4sii')
ROW = struct.Struct('<qddddqiq')


def test_read_codex_binary(monkeypatch) -> None:
    sandbox = Path('.test_mt5_binary_export')
    if sandbox.exists():
        shutil.rmtree(sandbox)
    sandbox.mkdir(parents=True, exist_ok=True)
    monkeypatch.chdir(sandbox)

    target = Path('sample.bin')
    with target.open('wb') as handle:
        handle.write(HEADER.pack(b'CODX', 1, 2))
        handle.write(ROW.pack(1767225600, 1.0, 1.1, 0.9, 1.05, 10, 2, 1))
        handle.write(ROW.pack(1767225900, 2.0, 2.1, 1.9, 2.05, 20, 3, 2))

    frame = read_codex_binary(target)

    assert frame.height == 2
    assert frame[0, 'time'] == datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    assert frame[1, 'close'] == 2.05

    monkeypatch.chdir(Path(__file__).resolve().parents[1])
    shutil.rmtree(sandbox, ignore_errors=True)