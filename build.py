"""
build.py — standalone executable builder for SheetQL
====================================================

Produces ``dist/sheetql.exe`` (Nuitka one-file) or ``dist/sheetql/sheetql.exe``
(PyInstaller folder build) via Nuitka or PyInstaller.

Backends
--------
- **nuitka** (default for ``python build.py``): best single-file binary; needs a C
  toolchain (Nuitka downloads MinGW winlibs when possible). **Windows Store Python**
  often breaks Nuitka's C backend; use **python.org** Python or choose ``pyinstaller``.
- **pyinstaller**: no C compiler; reliable on Store Python. **Default is ``--onedir``**
  (faster startup/exit than one-file, which unpacks to TEMP every run).
- **auto** (used by ``build.bat``): try Nuitka, then PyInstaller if Nuitka fails.

Requirements
------------
- Install deps: ``pip install -e ".[all,dev]"`` (includes Nuitka and PyInstaller).

Usage
-----
    python build.py                      # Nuitka (same as --backend nuitka)
    python build.py --backend pyinstaller
    python build.py --backend auto
    python build.py --debug              # Nuitka debug build
    python build.py --onefile            # single .exe (PyInstaller: slower start/exit)
    python build.py --no-onefile         # folder layout (default for PyInstaller)

Windows one-click: double-click ``build.bat`` (installs ``.[all,dev]``, then ``auto``).
"""

from __future__ import annotations

import argparse
import importlib.util
import os
import shutil
import subprocess
import sys
from pathlib import Path


def _print(*args, **kwargs) -> None:
    kwargs.setdefault("flush", True)
    print(*args, **kwargs)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).parent.resolve()
ENTRY_POINT = PROJECT_ROOT / "sheetql" / "__main__.py"
OUTPUT_DIR = PROJECT_ROOT / "dist"
BUILD_DIR = PROJECT_ROOT / "build"
EXE_NAME = "sheetql"
ICON_ICO = PROJECT_ROOT / "assets" / "SheetQL.ico"


def _nuitka_output_exe(onefile: bool) -> Path:
    """Expected main executable path after a Nuitka build."""
    if onefile:
        ext = ".exe" if sys.platform == "win32" else ""
        return OUTPUT_DIR / f"{EXE_NAME}{ext}"
    inner = f"{EXE_NAME}.exe" if sys.platform == "win32" else f"{EXE_NAME}.bin"
    return OUTPUT_DIR / f"{EXE_NAME}.dist" / inner


def _pyinstaller_output_exe(onefile: bool) -> Path:
    """Expected main executable path after a PyInstaller build."""
    ext = ".exe" if sys.platform == "win32" else ""
    if onefile:
        return OUTPUT_DIR / f"{EXE_NAME}{ext}"
    return OUTPUT_DIR / EXE_NAME / f"{EXE_NAME}{ext}"


def _want_onefile(
    *, nuitka_backend: bool, onefile_flag: bool, no_onefile_flag: bool
) -> bool:
    if onefile_flag:
        return True
    if no_onefile_flag:
        return False
    return nuitka_backend


INCLUDE_PACKAGES = [
    "duckdb",
    "python_calamine",
    "openpyxl",
    "xlsxwriter",
    "yaml",
    "prompt_toolkit",
    "pygments",
    "rich",
    "pandas",
    "numpy",
]

INCLUDE_PACKAGE_DATA = [
    "openpyxl",
    "duckdb",
]

# PyInstaller ``--collect-all`` import/top-level names (not always PyPI names).
PYINSTALLER_COLLECT_ALL = [
    "duckdb",
    "pandas",
    "numpy",
    "openpyxl",
    "xlsxwriter",
    "yaml",
    "prompt_toolkit",
    "pygments",
    "rich",
    "calamine",
    "tkinter",
]


# ---------------------------------------------------------------------------
# Checks
# ---------------------------------------------------------------------------


def check_nuitka() -> None:
    _print("  Checking for Nuitka ...", end=" ")
    if importlib.util.find_spec("nuitka") is None:
        _print("NOT FOUND\n")
        _print(
            "[ERROR] Nuitka is not installed. Run:\n\n" '    pip install -e ".[dev]"\n'
        )
        sys.exit(1)
    try:
        import nuitka  # noqa: F401

        version = getattr(nuitka, "__version__", "unknown")
    except Exception:
        version = "unknown"
    _print(f"found ({version})")


def check_pyinstaller() -> None:
    _print("  Checking for PyInstaller ...", end=" ")
    if importlib.util.find_spec("PyInstaller") is None:
        _print("NOT FOUND\n")
        _print(
            "[ERROR] PyInstaller is not installed. Run:\n\n"
            '    pip install -e ".[dev]"\n'
        )
        sys.exit(1)
    _print("ok")


def _require_windows_icon() -> None:
    if sys.platform == "win32" and not ICON_ICO.is_file():
        _print(f"\n[ERROR] Application icon not found: {ICON_ICO}")
        sys.exit(1)


def clean_dirs() -> None:
    for d in (BUILD_DIR, OUTPUT_DIR):
        if d.exists():
            _print(f"  Cleaning {d.relative_to(PROJECT_ROOT)} ...")
            shutil.rmtree(d)


def _windows_nuitka_env() -> dict[str, str]:
    """Drop MSYS/Cygwin GCC from PATH and clear CC so Nuitka can use winlibs/MSVC."""
    env = dict(os.environ)
    for key in ("CC", "CXX", "CPP", "CFLAGS", "CXXFLAGS", "LDFLAGS"):
        env.pop(key, None)

    path_parts = [p for p in env.get("PATH", "").split(os.pathsep) if p]
    lowered = [p.lower().replace("/", "\\") for p in path_parts]
    skip_tokens = ("\\msys64\\", "\\msys32\\", "\\cygwin64\\", "\\cygwin\\")
    filtered = [
        p
        for p, low in zip(path_parts, lowered)
        if not any(t in low for t in skip_tokens)
    ]
    env["PATH"] = os.pathsep.join(filtered)
    return env


# ---------------------------------------------------------------------------
# Nuitka
# ---------------------------------------------------------------------------


def build_nuitka(*, debug: bool = False, onefile: bool = True) -> int:
    _print("\nSheetQL — Nuitka build")
    _print("=" * 40)

    check_nuitka()
    _require_windows_icon()
    _print()
    clean_dirs()

    cmd = [sys.executable, "-m", "nuitka"]

    if onefile:
        cmd.append("--onefile")
        _print("  Mode: single .exe (--onefile)")
    else:
        cmd.append("--standalone")
        cmd.append(f"--output-folder-name={EXE_NAME}")
        _print("  Mode: directory (--standalone)")

    cmd += ["--windows-console-mode=force"]

    if sys.platform == "win32":
        cmd += [
            "--mingw64",
            "--assume-yes-for-downloads",
            f"--windows-icon-from-ico={ICON_ICO.resolve()}",
        ]
        _print(f"  Icon        : {ICON_ICO.relative_to(PROJECT_ROOT)}")

    cmd += ["--enable-plugin=tk-inter"]

    for pkg in INCLUDE_PACKAGES:
        cmd.append(f"--include-package={pkg}")
    for pkg in INCLUDE_PACKAGE_DATA:
        cmd.append(f"--include-package-data={pkg}")

    cmd += [
        f"--output-filename={EXE_NAME}",
        f"--output-dir={OUTPUT_DIR}",
    ]

    if debug:
        cmd += ["--debug", "--verbose"]
        _print("  Profile: debug (symbols + verbose)")
    else:
        cmd += ["--python-flag=no_docstrings", "--python-flag=no_asserts"]
        _print("  Profile: release (stripped)")

    cmd.append(str(ENTRY_POINT))

    _print(f"\n  Entry point : {ENTRY_POINT.relative_to(PROJECT_ROOT)}")
    _print(f"  Output dir  : {OUTPUT_DIR.relative_to(PROJECT_ROOT)}")
    rel_exe = _nuitka_output_exe(onefile).relative_to(OUTPUT_DIR)
    _print(f"  Executable  : {rel_exe}")
    _print("\nStarting Nuitka (may take many minutes) ...")
    _print("-" * 40)

    sub_env = _windows_nuitka_env() if sys.platform == "win32" else None
    ret = subprocess.call(cmd, env=sub_env)

    _print("-" * 40)
    if ret != 0:
        _print(f"\n[ERROR] Nuitka exited with code {ret}.")
        return ret

    exe_path = _nuitka_output_exe(onefile)
    _print(f"\n[OK] Build complete: {exe_path}")
    return 0


# ---------------------------------------------------------------------------
# PyInstaller
# ---------------------------------------------------------------------------


def build_pyinstaller(*, onefile: bool = False) -> int:
    _print("\nSheetQL — PyInstaller build")
    _print("=" * 40)

    check_pyinstaller()
    _require_windows_icon()
    _print()
    clean_dirs()

    work = BUILD_DIR / "pyinstaller"
    work.mkdir(parents=True, exist_ok=True)

    cmd: list[str] = [
        sys.executable,
        "-m",
        "PyInstaller",
        str(ENTRY_POINT),
        "--noconfirm",
        "--clean",
        f"--name={EXE_NAME}",
        f"--distpath={OUTPUT_DIR}",
        f"--workpath={work}",
        f"--specpath={BUILD_DIR}",
    ]
    if onefile:
        cmd.append("--onefile")
        _print("  Mode: single .exe (--onefile)")
    else:
        cmd.append("--onedir")
        _print("  Mode: directory (--onedir)")

    if sys.platform == "win32":
        cmd += [
            f"--icon={ICON_ICO.resolve()}",
            "--console",
        ]
        _print(f"  Icon        : {ICON_ICO.relative_to(PROJECT_ROOT)}")

    for pkg in PYINSTALLER_COLLECT_ALL:
        cmd.extend(["--collect-all", pkg])

    _print(f"\n  Entry point : {ENTRY_POINT.relative_to(PROJECT_ROOT)}")
    _print(f"  Output dir  : {OUTPUT_DIR.relative_to(PROJECT_ROOT)}")
    rel_exe = _pyinstaller_output_exe(onefile).relative_to(OUTPUT_DIR)
    _print(f"  Executable  : {rel_exe}")
    _print("\nStarting PyInstaller ...")
    _print("-" * 40)

    ret = subprocess.call(cmd, cwd=PROJECT_ROOT)

    _print("-" * 40)
    if ret != 0:
        _print(f"\n[ERROR] PyInstaller exited with code {ret}.")
        return ret

    exe_path = _pyinstaller_output_exe(onefile)
    _print(f"\n[OK] Build complete: {exe_path}")
    return 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build a standalone SheetQL executable (Nuitka or PyInstaller)",
    )
    parser.add_argument(
        "--backend",
        choices=("nuitka", "pyinstaller", "auto"),
        default="nuitka",
        help="Build tool: nuitka (default), pyinstaller, or auto (Nuitka then PyInstaller)",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Nuitka only: debug symbols and verbose output",
    )
    pack = parser.add_mutually_exclusive_group()
    pack.add_argument(
        "--onefile",
        action="store_true",
        help=(
            "Single-file executable. Nuitka: default anyway. PyInstaller: one .exe "
            "that unpacks to TEMP each run (slower startup and exit)."
        ),
    )
    pack.add_argument(
        "--no-onefile",
        action="store_true",
        help=(
            "Folder distribution. Nuitka: standalone .dist folder. PyInstaller: default; "
            "faster startup and exit than --onefile."
        ),
    )
    args = parser.parse_args()

    def _onefile_nuitka() -> bool:
        return _want_onefile(
            nuitka_backend=True,
            onefile_flag=args.onefile,
            no_onefile_flag=args.no_onefile,
        )

    def _onefile_pyinstaller() -> bool:
        return _want_onefile(
            nuitka_backend=False,
            onefile_flag=args.onefile,
            no_onefile_flag=args.no_onefile,
        )

    if args.backend == "pyinstaller":
        if args.debug:
            _print("[WARN] --debug applies to Nuitka only; ignoring for PyInstaller.")
        sys.exit(build_pyinstaller(onefile=_onefile_pyinstaller()))

    if args.backend == "nuitka":
        sys.exit(build_nuitka(debug=args.debug, onefile=_onefile_nuitka()))

    # auto
    if args.debug:
        _print("[WARN] --debug with --backend auto: only applies if Nuitka runs.")
    code = build_nuitka(debug=args.debug, onefile=_onefile_nuitka())
    if code == 0:
        sys.exit(0)
    _print(
        "\n[auto] Nuitka failed (common with Windows Store Python or toolchain issues)."
        "\n[auto] Retrying with PyInstaller ...\n",
    )
    sys.exit(build_pyinstaller(onefile=_onefile_pyinstaller()))


if __name__ == "__main__":
    main()
