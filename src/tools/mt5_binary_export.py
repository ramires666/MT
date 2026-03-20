from __future__ import annotations

import struct
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

_HEADER_STRUCT = struct.Struct('<4sii')
_HEADER_UTF16_STRUCT = struct.Struct('<8sii')
_ROW_STRUCT = struct.Struct('<qddddqiq')
_MAGIC_ASCII = b'CODX'
_MAGIC_UTF16 = 'CODX'.encode('utf-16-le')


def read_codex_binary(path: Path) -> pl.DataFrame:
    with path.open('rb') as handle:
        first8 = handle.read(8)
        if first8.startswith(_MAGIC_UTF16):
            remainder = handle.read(_HEADER_UTF16_STRUCT.size - 8)
            magic, version, count = _HEADER_UTF16_STRUCT.unpack(first8 + remainder)
            if magic != _MAGIC_UTF16:
                raise ValueError(f'Unexpected UTF-16 export magic: {magic!r}')
        else:
            remainder = handle.read(_HEADER_STRUCT.size - 8)
            magic, version, count = _HEADER_STRUCT.unpack(first8 + remainder)
            if magic != _MAGIC_ASCII:
                raise ValueError(f'Unexpected export magic: {magic!r}')

        rows = [
            _ROW_STRUCT.unpack(handle.read(_ROW_STRUCT.size))
            for _ in range(count)
        ]

    return pl.DataFrame(
        {
            'time': [datetime.fromtimestamp(row[0], tz=UTC) for row in rows],
            'open': [row[1] for row in rows],
            'high': [row[2] for row in rows],
            'low': [row[3] for row in rows],
            'close': [row[4] for row in rows],
            'tick_volume': [row[5] for row in rows],
            'spread': [row[6] for row in rows],
            'real_volume': [row[7] for row in rows],
        }
    )