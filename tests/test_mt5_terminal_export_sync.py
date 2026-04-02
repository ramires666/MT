from __future__ import annotations

from pathlib import Path

from tools.mt5_terminal_export_sync import coerce_platform_path, default_common_root


def test_coerce_platform_path_converts_windows_drive_to_wsl_mount() -> None:
    result = coerce_platform_path(r"C:\Program Files\Bybit MT5 Terminal\terminal64.exe")

    assert result == Path("/mnt/c/Program Files/Bybit MT5 Terminal/terminal64.exe")


def test_default_common_root_prefers_explicit_env_override(monkeypatch, tmp_path) -> None:
    override = tmp_path / "custom_common_root"
    monkeypatch.setenv("MT_SERVICE_MT5_COMMON_ROOT", str(override))
    monkeypatch.delenv("APPDATA", raising=False)
    monkeypatch.delenv("WSL_DISTRO_NAME", raising=False)

    assert default_common_root() == override


def test_default_common_root_uses_appdata_when_available(monkeypatch) -> None:
    monkeypatch.delenv("MT_SERVICE_MT5_COMMON_ROOT", raising=False)
    monkeypatch.setenv("APPDATA", r"C:\Users\TestUser\AppData\Roaming")
    monkeypatch.delenv("WSL_DISTRO_NAME", raising=False)

    assert default_common_root() == Path("/mnt/c/Users/TestUser/AppData/Roaming/MetaQuotes/Terminal/Common/Files")


def test_default_common_root_uses_existing_wsl_windows_common_root(monkeypatch, tmp_path) -> None:
    monkeypatch.delenv("MT_SERVICE_MT5_COMMON_ROOT", raising=False)
    monkeypatch.delenv("APPDATA", raising=False)
    monkeypatch.setenv("WSL_DISTRO_NAME", "Ubuntu")
    expected = tmp_path / "Trader" / "AppData" / "Roaming" / "MetaQuotes" / "Terminal" / "Common" / "Files"
    expected.mkdir(parents=True)
    fallback = tmp_path / "Other" / "AppData" / "Roaming" / "MetaQuotes" / "Terminal" / "Common" / "Files"
    monkeypatch.setattr(
        "tools.mt5_terminal_export_sync._iter_wsl_windows_common_root_candidates",
        lambda users_root=None: [expected, fallback],
    )

    assert default_common_root() == expected
