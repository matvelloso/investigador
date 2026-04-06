from __future__ import annotations

from pathlib import Path

__all__ = ["__version__"]

__version__ = "0.1.0"

_src_package = Path(__file__).resolve().parent.parent / "src" / "investigador"
if _src_package.exists():
    __path__.append(str(_src_package))
