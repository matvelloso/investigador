from __future__ import annotations

import json
import re
import tomllib
from pathlib import Path
from typing import Any

from .messages import msg
from .models import Note


DELIMITER = "+++"
MACHINE_METADATA_HEADING = "## Metadados da Máquina"
MACHINE_METADATA_START = "<!-- investigador:metadata:start -->"
MACHINE_METADATA_END = "<!-- investigador:metadata:end -->"


def _toml_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None:
        return '""'
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, list):
        if not value:
            return "[]"
        if all(not isinstance(item, dict) for item in value):
            return "[" + ", ".join(_toml_value(item) for item in value) + "]"
        raise ValueError(msg("toml_lista_dicts"))
    raise TypeError(msg("valor_toml_nao_suportado", type_name=repr(type(value))))


def _emit_table(lines: list[str], data: dict[str, Any], path: list[str] | None = None) -> None:
    path = path or []
    scalar_items: dict[str, Any] = {}
    nested_dicts: dict[str, dict[str, Any]] = {}
    array_tables: dict[str, list[dict[str, Any]]] = {}

    for key, value in data.items():
        if isinstance(value, dict):
            nested_dicts[key] = value
        elif isinstance(value, list) and value and all(isinstance(item, dict) for item in value):
            array_tables[key] = value
        else:
            scalar_items[key] = value

    for key in sorted(scalar_items):
        lines.append(f"{key} = {_toml_value(scalar_items[key])}")

    for key in sorted(nested_dicts):
        if lines and lines[-1] != "":
            lines.append("")
        full_key = ".".join(path + [key])
        lines.append(f"[{full_key}]")
        _emit_table(lines, nested_dicts[key], path + [key])

    for key in sorted(array_tables):
        for item in array_tables[key]:
            if lines and lines[-1] != "":
                lines.append("")
            full_key = ".".join(path + [key])
            lines.append(f"[[{full_key}]]")
            _emit_table(lines, item, path + [key])


def dump_frontmatter(data: dict[str, Any]) -> str:
    lines: list[str] = []
    _emit_table(lines, data)
    rendered = "\n".join(lines).strip()
    return f"{DELIMITER}\n{rendered}\n{DELIMITER}\n"


def _dump_machine_metadata(data: dict[str, Any]) -> str:
    payload = dump_frontmatter(data).removeprefix(f"{DELIMITER}\n").removesuffix(f"\n{DELIMITER}\n").strip()
    return (
        f"{MACHINE_METADATA_HEADING}\n\n"
        f"{MACHINE_METADATA_START}\n"
        "```toml\n"
        f"{payload}\n"
        "```\n"
        f"{MACHINE_METADATA_END}"
    )


def _strip_machine_metadata(body: str) -> str:
    text = body.strip()
    if MACHINE_METADATA_START not in text:
        return text
    start = text.rfind(MACHINE_METADATA_START)
    prefix = text[:start].rstrip()
    prefix = re.sub(rf"\n{re.escape(MACHINE_METADATA_HEADING)}\s*$", "", prefix).rstrip()
    return prefix


def _parse_footer_metadata(text: str, path: Path) -> Note | None:
    start = text.rfind(MACHINE_METADATA_START)
    end = text.rfind(MACHINE_METADATA_END)
    if start == -1 and end == -1:
        return None
    if start == -1 or end == -1 or end < start:
        raise ValueError(msg("bloco_metadata_rodape_invalido", path=path))
    body_prefix = text[:start].rstrip()
    body = re.sub(rf"\n{re.escape(MACHINE_METADATA_HEADING)}\s*$", "", body_prefix).rstrip()
    metadata_block = text[start + len(MACHINE_METADATA_START) : end].strip()
    marker_start = "```toml"
    marker_end = "```"
    if not metadata_block.startswith(marker_start) or not metadata_block.endswith(marker_end):
        raise ValueError(msg("bloco_metadata_rodape_invalido", path=path))
    raw_frontmatter = metadata_block[len(marker_start) : -len(marker_end)].strip()
    frontmatter = tomllib.loads(raw_frontmatter) if raw_frontmatter else {}
    return Note(path=path, frontmatter=frontmatter, body=body, storage_format="footer")


def parse_markdown(text: str, path: Path) -> Note:
    if text.startswith(f"{DELIMITER}\n"):
        remainder = text[len(DELIMITER) + 1 :]
        marker = f"\n{DELIMITER}\n"
        if marker not in remainder:
            raise ValueError(msg("bloco_frontmatter_sem_fechamento", path=path))
        raw_frontmatter, body = remainder.split(marker, 1)
        frontmatter = tomllib.loads(raw_frontmatter)
        return Note(path=path, frontmatter=frontmatter, body=body.lstrip("\n").rstrip(), storage_format="legacy_frontmatter")
    footer_note = _parse_footer_metadata(text, path)
    if footer_note is not None:
        return footer_note
    return Note(path=path, frontmatter={}, body=text.rstrip(), storage_format="plain")


def read_note(path: Path) -> Note:
    return parse_markdown(path.read_text(encoding="utf-8"), path)


def write_note(path: Path, frontmatter: dict[str, Any], body: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    cleaned_body = _strip_machine_metadata(body)
    metadata_footer = _dump_machine_metadata(frontmatter)
    payload = f"{cleaned_body}\n\n{metadata_footer}\n" if cleaned_body else f"{metadata_footer}\n"
    path.write_text(payload, encoding="utf-8")
