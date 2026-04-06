from __future__ import annotations

import json
import os
import re
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable

from .agents import deterministic_proposals, get_provider
from .frontmatter import read_note, write_note
from .messages import msg
from .models import (
    DEFAULT_CONTEXTUAL_PLUGINS,
    DEFAULT_PORTFOLIO_BASELINE_PLUGINS,
    DEFAULT_WAVE_ONE_PLUGINS,
    AGENT_ROLES,
    BROAD_FACT_PLUGINS,
    EVIDENCE_LAYERS,
    EVIDENCE_ROLES,
    ENTITY_TYPES,
    IDENTITY_RESOLUTION_STATUSES,
    PRIORITY_LEVELS,
    EvidenceRecord,
    Note,
    ProposedChange,
    SOURCE_CLASSES,
)
from .plugins import PluginContext, get_plugin
from .templates import entity_body, evidence_body, project_body, target_body, task_body, workspace_readme_body


LAW_CATALOG = {
    "integridade-contratacoes-publicas": {
        "title": "Integridade em Contratações Públicas",
        "summary": "Padrão de atenção para direcionamento, favorecimento indevido, sobrepreço e triangulação em contratações públicas.",
        "references": [
            "Lei 14.133/2021",
            "Lei 8.429/1992",
        ],
    },
    "conflito-de-interesses-e-improbidade": {
        "title": "Conflito de Interesses e Improbidade Administrativa",
        "summary": "Padrão de atenção para uso do cargo, influência indevida, vantagem imprópria e rede de contraprestações.",
        "references": [
            "Lei 12.813/2013",
            "Lei 8.429/1992",
        ],
    },
    "uso-de-verbas-publicas": {
        "title": "Uso de Verbas e Recursos Públicos",
        "summary": "Padrão de atenção para uso anômalo de verbas parlamentares, reembolsos, cotas e execução de despesas públicas.",
        "references": [
            "Normas internas de ressarcimento parlamentar",
            "Lei 8.429/1992",
        ],
    },
    "sancoes-e-controle-externo": {
        "title": "Sanções, Controle Externo e Risco Processual",
        "summary": "Padrão de atenção para sanções, certidões, acórdãos, processos e apontamentos de controle com impacto reputacional ou investigativo.",
        "references": [
            "Lei 12.846/2013",
            "Jurisprudência de controle externo e registros públicos sancionatórios",
        ],
    },
}

HYPOTHESIS_CATALOG = {
    "official_signals_review": {
        "title": "Sinais Oficiais Sob Revisão Investigativa",
        "summary": "Padrão para casos com sinais oficiais que já justificam aprofundamento, contestação e consolidação probatória cuidadosa.",
    },
    "expense_anomaly": {
        "title": "Gastos Públicos com Anomalia Indiciária",
        "summary": "Padrão para concentração de despesas, contrapartes recorrentes, categorias dominantes e uso atípico de verbas públicas.",
    },
    "procurement_risk": {
        "title": "Contratações e Repasses Sob Risco",
        "summary": "Padrão para sinais de contratação, repasse, publicação ou execução com necessidade de verificar capacidade, direcionamento e integridade.",
    },
    "sanction_or_control_risk": {
        "title": "Sanções ou Controle Externo Relevante",
        "summary": "Padrão para processos, sanções, acórdãos e registros de controle que exigem contextualização e leitura cautelosa.",
    },
    "relationship_network_risk": {
        "title": "Rede de Relações e Contrapartes Relevantes",
        "summary": "Padrão para redes de fornecedores, organizações, partidos e outros vínculos que merecem expansão e teste de conflito de interesses.",
    },
}

PROJECT_RENDER_VERSION = 2
DOSSIER_RENDER_VERSION = 2

PUBLISH_LOCAL_WORKSPACE_DIRS = (".investigador", "projects", "registry", "portfolios")
PUBLISH_SCAN_DIRS = ("src", "tests", "scripts")
PUBLISH_SCAN_FILES = ("README.md", ".env", ".env.example", ".env.template", ".envrc")
PUBLISH_TEXT_EXTENSIONS = {".md", ".py", ".sh", ".toml", ".json", ".yml", ".yaml", ".txt", ".cfg", ".ini"}
PUBLISH_REQUIRED_README_SNIPPETS = (
    "source-only",
    "dados gerados são locais",
    "segredos nunca devem ser commitados",
    "clone limpo",
)
PUBLISH_ALLOWED_PLACEHOLDERS = {
    "sua-chave",
    "seu-token",
    "seu-cookie",
    "fake-key",
    "public-key-123",
    "valor-da-chave",
    "valor-da-chave-sem-o-prefixo-authorization",
    "maria exemplo",
    "joão da silva",
    "maria souza",
    "empresa exemplo",
    "fornecedor alpha",
    "fornecedor beta",
    "placeholder",
    "example",
    "exemplo",
    "redacted",
}
PUBLISH_FORBIDDEN_FIXTURE_TERMS = (
    "bacelar",
    "joão carlos bacelar batista",
)


def utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def slugify(text: str) -> str:
    lowered = text.strip().lower()
    lowered = re.sub(r"[^a-z0-9]+", "-", lowered)
    lowered = re.sub(r"-{2,}", "-", lowered)
    return lowered.strip("-") or "item"


def note_id(prefix: str, *parts: str) -> str:
    return prefix + "-" + slugify("-".join(parts))


def _dedupe_strings(values: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        ordered.append(text)
    return ordered


def _iter_publish_scan_paths(root: Path) -> list[Path]:
    paths: list[Path] = []
    for name in PUBLISH_SCAN_FILES:
        path = root / name
        if path.is_file():
            paths.append(path)
    local_env = root / ".investigador" / "env.sh"
    if local_env.is_file():
        paths.append(local_env)
    for directory_name in PUBLISH_SCAN_DIRS:
        directory = root / directory_name
        if not directory.is_dir():
            continue
        for path in sorted(directory.rglob("*")):
            if path.is_file() and path.suffix.lower() in PUBLISH_TEXT_EXTENSIONS:
                paths.append(path)
    return paths


def _looks_like_placeholder_secret(value: str) -> bool:
    normalized = value.strip().strip("\"'").strip()
    lowered = normalized.lower()
    if not lowered:
        return True
    if lowered.startswith("$"):
        return True
    if "..." in normalized:
        return True
    if lowered in PUBLISH_ALLOWED_PLACEHOLDERS:
        return True
    return any(token in lowered for token in PUBLISH_ALLOWED_PLACEHOLDERS)


def _extract_assigned_secret(line: str) -> tuple[str, str] | None:
    candidate = line.strip()
    if not candidate or candidate.startswith("#"):
        return None
    match = re.match(r"^(?:export\s+)?([A-Z0-9_]*(?:API_KEY|TOKEN|COOKIE|SECRET|PASSWORD)[A-Z0-9_]*)\s*[:=]\s*(.+)$", candidate)
    if not match:
        return None
    variable = match.group(1)
    value = match.group(2).strip()
    if " #" in value:
        value = value.split(" #", 1)[0].strip()
    if value.startswith(("\"", "'")) and value.endswith(("\"", "'")) and len(value) >= 2:
        value = value[1:-1].strip()
    return variable, value


def validate_publish_safety(root: Path) -> list[str]:
    errors: list[str] = []
    for directory_name in PUBLISH_LOCAL_WORKSPACE_DIRS:
        directory = root / directory_name
        if directory.is_dir() and any(directory.rglob("*")):
            errors.append(f"generated_workspace_present: diretório local com artefatos presente: {directory}")

    readme_path = root / "README.md"
    if not readme_path.is_file():
        errors.append("publish_readme_contract_missing: README.md ausente.")
    else:
        readme_text = readme_path.read_text(encoding="utf-8")
        lowered_readme = readme_text.lower()
        for snippet in PUBLISH_REQUIRED_README_SNIPPETS:
            if snippet not in lowered_readme:
                errors.append(f"publish_readme_contract_missing: README.md precisa mencionar `{snippet}`.")

    for path in _iter_publish_scan_paths(root):
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        for lineno, line in enumerate(text.splitlines(), start=1):
            assigned_secret = _extract_assigned_secret(line)
            if assigned_secret is not None:
                variable, value = assigned_secret
                if not _looks_like_placeholder_secret(value):
                    errors.append(f"secret_detected: {path}:{lineno}: `{variable}` parece conter credencial real.")
            lowered_line = line.lower()
            if "tests" in path.parts:
                for forbidden_term in PUBLISH_FORBIDDEN_FIXTURE_TERMS:
                    if forbidden_term in lowered_line:
                        errors.append(f"real_case_fixture_detected: {path}:{lineno}: termo proibido `{forbidden_term}`.")
        if re.search(r"(?m)^-----BEGIN [A-Z ]*PRIVATE KEY-----$", text):
            errors.append(f"secret_detected: {path}: bloco de chave privada detectado.")
    return errors


def _merge_metadata_value(existing: Any, incoming: Any) -> Any:
    if incoming in (None, ""):
        return existing
    if isinstance(existing, dict) and isinstance(incoming, dict):
        merged = dict(existing)
        for key, value in incoming.items():
            merged[key] = _merge_metadata_value(merged.get(key), value)
        return merged
    if isinstance(existing, list) and isinstance(incoming, list):
        serialized: set[str] = set()
        merged_list: list[Any] = []
        for item in [*existing, *incoming]:
            marker = json.dumps(item, sort_keys=True, ensure_ascii=False) if isinstance(item, (dict, list)) else str(item)
            if marker in serialized:
                continue
            serialized.add(marker)
            merged_list.append(item)
        return merged_list
    return incoming


def _merge_metadata(existing: dict[str, Any] | None, incoming: dict[str, Any] | None) -> dict[str, Any]:
    merged = dict(existing or {})
    for key, value in (incoming or {}).items():
        merged[key] = _merge_metadata_value(merged.get(key), value)
    return merged


def _source_ref_signature(ref: dict[str, Any]) -> tuple[str, ...]:
    return (
        str(ref.get("plugin", "")),
        str(ref.get("source_name", "")),
        str(ref.get("record_id", "")),
        str(ref.get("url", "")),
        str(ref.get("query", "")),
        str(ref.get("domain", "")),
        str(ref.get("publisher", "")),
        str(ref.get("published_at", "")),
        str(ref.get("retrieved_from", "")),
    )


def ensure_workspace(root: Path) -> None:
    if not (root / ".investigador").exists():
        raise FileNotFoundError("Workspace not initialized. Run `investigador init` first.")


def workspace_paths(root: Path) -> dict[str, Path]:
    return {
        "registry": root / "registry",
        "projects": root / "projects",
        "portfolios": root / "portfolios",
        "cache": root / ".investigador" / "cache",
        "locks": root / ".investigador" / "locks",
    }


def _workspace_note_roots(root: Path) -> list[Path]:
    paths = workspace_paths(root)
    roots = [paths["registry"], paths["projects"], paths["portfolios"]]
    readme_path = root / ".investigador" / "README.md"
    if readme_path.exists():
        roots.append(readme_path)
    return roots


def init_workspace(root: Path) -> dict[str, Path]:
    paths = workspace_paths(root)
    for directory in (
        paths["registry"] / "people",
        paths["registry"] / "organizations",
        paths["registry"] / "laws",
        paths["registry"] / "hypotheses",
        paths["projects"],
        paths["portfolios"],
        paths["cache"],
        paths["locks"],
    ):
        directory.mkdir(parents=True, exist_ok=True)
    readme_path = root / ".investigador" / "README.md"
    if not readme_path.exists():
        write_note(
            readme_path,
            {
                "id": "workspace-investigador",
                "type": "workspace",
                "title": "Investigador Workspace",
                "status": "active",
                "source_class": "derived_workspace",
                "source_refs": [],
                "related_ids": [],
                "project_ids": [],
                "confidence": 1.0,
                "updated_at": utc_now(),
            },
            workspace_readme_body(),
        )
    refresh_cache(root)
    return paths


def canonical_entity_path(root: Path, entity_type: str, identifier: str, title: str | None = None) -> Path:
    folder = ENTITY_TYPES[entity_type]
    base = slugify(title or identifier)
    preferred = root / "registry" / folder / f"{base}.md"
    if not preferred.exists():
        return preferred
    try:
        existing = read_note(preferred)
    except Exception:
        existing = None
    if existing is not None and identifier in existing.frontmatter.get("identifiers", []):
        return preferred
    return root / "registry" / folder / f"{base}-{slugify(identifier)}.md"


def list_markdown_notes(root: Path) -> list[Path]:
    notes: list[Path] = []
    for root_path in _workspace_note_roots(root):
        if root_path.is_file():
            notes.append(root_path)
            continue
        notes.extend(path for path in root_path.rglob("*.md") if ".git" not in path.parts)
    return sorted(dict.fromkeys(notes))


def load_notes(root: Path) -> list[Note]:
    notes: list[Note] = []
    for path in list_markdown_notes(root):
        try:
            notes.append(read_note(path))
        except Exception as exc:  # pragma: no cover - surfaced in validate
            notes.append(Note(path=path, frontmatter={"id": f"parse-error-{path.name}", "error": str(exc)}, body=""))
    return notes


def build_note_index(root: Path) -> dict[str, Note]:
    index: dict[str, Note] = {}
    for note in load_notes(root):
        note_id_value = note.frontmatter.get("id")
        if note_id_value:
            index[note_id_value] = note
    return index


def refresh_cache(root: Path) -> Path:
    paths = workspace_paths(root)
    paths["cache"].mkdir(parents=True, exist_ok=True)
    note_index = build_note_index(root)
    cache_payload = {
        note_id_value: {
            "path": str(note.path.relative_to(root)),
            "type": note.frontmatter.get("type", ""),
            "title": note.frontmatter.get("title", note.frontmatter.get("name", "")),
        }
        for note_id_value, note in note_index.items()
    }
    cache_path = paths["cache"] / "index.json"
    cache_path.write_text(json.dumps(cache_payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return cache_path


def create_project(root: Path, slug: str, title: str | None = None, metadata: dict[str, Any] | None = None) -> Path:
    ensure_workspace(root)
    project_slug = slugify(slug)
    project_root = root / "projects" / project_slug
    for directory in (
        project_root / "targets",
        project_root / "tasks",
        project_root / "evidence",
        project_root / "hypotheses",
        project_root / "runs",
        project_root / "runs" / "artifacts",
        project_root / "dossiers",
    ):
        directory.mkdir(parents=True, exist_ok=True)
    project_path = project_root / "project.md"
    project_title = title or slug.replace("-", " ").title()
    frontmatter = {
        "id": note_id("project", project_slug),
        "type": "project",
        "title": project_title,
        "status": "active",
        "project_slug": project_slug,
        "source_class": "derived_workspace",
        "source_refs": [],
        "related_ids": [],
        "project_ids": [project_slug],
        "target_ids": [],
        "plugin_names": DEFAULT_WAVE_ONE_PLUGINS,
        "contextual_plugin_names": DEFAULT_CONTEXTUAL_PLUGINS,
        "metadata": metadata or {},
        "aliases": [],
        "confidence": 1.0,
        "updated_at": utc_now(),
        "language": "pt-BR",
        "priority_thresholds": {
            "high_priority_min_official": 2,
            "high_priority_plus_corrob": 2,
        },
    }
    write_note(project_path, frontmatter, project_body(project_title))
    refresh_cache(root)
    return project_path


def get_project(root: Path, slug: str) -> Note:
    ensure_workspace(root)
    path = root / "projects" / slugify(slug) / "project.md"
    if not path.exists():
        raise FileNotFoundError(msg("projeto_inexistente", slug=slug))
    return read_note(path)


def _find_entity_by_identifier(root: Path, entity_type: str, identifier: str) -> Note | None:
    for note in load_notes(root):
        if note.frontmatter.get("type") != "entity":
            continue
        if note.frontmatter.get("entity_type") != entity_type:
            continue
        identifiers = note.frontmatter.get("identifiers", [])
        if identifier in identifiers:
            return note
    return None


def upsert_entity(
    root: Path,
    entity_type: str,
    identifier: str,
    title: str,
    project_slug: str,
    source_refs: list[dict[str, Any]] | None = None,
    metadata: dict[str, Any] | None = None,
    aliases: list[str] | None = None,
) -> Note:
    existing = _find_entity_by_identifier(root, entity_type, identifier)
    if existing is not None:
        frontmatter = dict(existing.frontmatter)
        project_ids = set(frontmatter.get("project_ids", []))
        project_ids.add(project_slug)
        frontmatter["project_ids"] = sorted(project_ids)
        identifiers = set(frontmatter.get("identifiers", []))
        identifiers.add(identifier)
        frontmatter["identifiers"] = sorted(identifiers)
        frontmatter["aliases"] = _dedupe_strings([*frontmatter.get("aliases", []), *(aliases or [])])
        frontmatter["metadata"] = _merge_metadata(frontmatter.get("metadata", {}), metadata or {})
        refs = frontmatter.get("source_refs", [])
        existing_signatures = {_source_ref_signature(ref) for ref in refs}
        for ref in source_refs or []:
            if _source_ref_signature(ref) not in existing_signatures:
                refs.append(ref)
                existing_signatures.add(_source_ref_signature(ref))
        frontmatter["source_refs"] = refs
        frontmatter["updated_at"] = utc_now()
        write_note(existing.path, frontmatter, existing.body)
        return read_note(existing.path)

    path = canonical_entity_path(root, entity_type, identifier, title)
    entity_id = note_id(entity_type, title, identifier)
    frontmatter = {
        "id": entity_id,
        "type": "entity",
        "entity_type": entity_type,
        "title": title,
        "name": title,
        "status": "active",
        "source_class": "derived_workspace",
        "source_refs": source_refs or [],
        "related_ids": [],
        "project_ids": [project_slug],
        "identifiers": [identifier],
        "aliases": sorted(set(alias for alias in aliases or [] if alias)),
        "metadata": metadata or {},
        "confidence": 0.6,
        "updated_at": utc_now(),
    }
    write_note(path, frontmatter, entity_body(title, entity_type))
    return read_note(path)


def _relative_link(from_path: Path, to_path: Path) -> str:
    return to_path.relative_to(from_path.parent.parent.parent if "projects" in from_path.parts else from_path.parent).as_posix()


def add_target(
    root: Path,
    project_slug: str,
    entity_type: str,
    identifier: str,
    title: str | None = None,
    aliases: list[str] | None = None,
    metadata: dict[str, Any] | None = None,
) -> Path:
    ensure_workspace(root)
    if entity_type not in ENTITY_TYPES:
        raise ValueError(msg("tipo_entidade_desconhecido", entity_type=entity_type))
    project = get_project(root, project_slug)
    canonical = upsert_entity(
        root,
        entity_type,
        identifier,
        title or identifier,
        project.frontmatter["project_slug"],
        metadata=metadata,
        aliases=aliases,
    )
    target_slug = slugify(identifier)
    target_path = root / "projects" / project.frontmatter["project_slug"] / "targets" / f"{entity_type}-{target_slug}.md"
    relative_link = Path("../../..") / canonical.path.relative_to(root)
    frontmatter = {
        "id": note_id("target", project.frontmatter["project_slug"], entity_type, identifier),
        "type": "target",
        "title": title or identifier,
        "status": "tracked",
        "entity_type": entity_type,
        "canonical_id": canonical.frontmatter["id"],
        "canonical_path": canonical.path.relative_to(root).as_posix(),
        "identifier": identifier,
        "aliases": sorted(set(alias for alias in aliases or [] if alias)),
        "metadata": metadata or {},
        "source_class": "derived_workspace",
        "source_refs": canonical.frontmatter.get("source_refs", []),
        "related_ids": [canonical.frontmatter["id"]],
        "project_ids": [project.frontmatter["project_slug"]],
        "confidence": 0.65,
        "updated_at": utc_now(),
    }
    write_note(target_path, frontmatter, target_body(identifier, relative_link.as_posix()))

    project_frontmatter = dict(project.frontmatter)
    target_ids = set(project_frontmatter.get("target_ids", []))
    target_ids.add(frontmatter["id"])
    project_frontmatter["target_ids"] = sorted(target_ids)
    project_frontmatter["related_ids"] = sorted(set(project_frontmatter.get("related_ids", [])) | {canonical.frontmatter["id"]})
    project_frontmatter["updated_at"] = utc_now()
    write_note(project.path, project_frontmatter, project.body)
    refresh_project_materialized_views(root, project.frontmatter["project_slug"])
    return target_path


def project_targets(root: Path, project_slug: str) -> list[Note]:
    project_root = root / "projects" / slugify(project_slug) / "targets"
    return [read_note(path) for path in sorted(project_root.glob("*.md"))]


def _find_project_target(root: Path, project_slug: str, selector: str) -> Note:
    candidates = project_targets(root, project_slug)
    selector_slug = slugify(selector)
    matches: list[Note] = []
    for target in candidates:
        comparable_values = [
            target.frontmatter.get("id", ""),
            target.frontmatter.get("identifier", ""),
            target.frontmatter.get("title", ""),
            target.frontmatter.get("canonical_id", ""),
            target.path.stem,
        ]
        comparable_values.extend(target.frontmatter.get("aliases", []))
        if any(slugify(str(value)) == selector_slug for value in comparable_values if value):
            matches.append(target)
    if not matches:
        raise FileNotFoundError(msg("alvo_inexistente_no_projeto", selector=selector, project_slug=project_slug))
    if len(matches) > 1:
        paths = ", ".join(str(note.path) for note in matches)
        raise ValueError(f"Target selector '{selector}' is ambiguous in project '{project_slug}': {paths}")
    return matches[0]


def update_target(
    root: Path,
    project_slug: str,
    selector: str,
    title: str | None = None,
    aliases: list[str] | None = None,
    metadata: dict[str, Any] | None = None,
) -> Path:
    ensure_workspace(root)
    project = get_project(root, project_slug)
    target = _find_project_target(root, project.frontmatter["project_slug"], selector)
    note_index = build_note_index(root)
    canonical_id = target.frontmatter["canonical_id"]
    if canonical_id not in note_index:
        raise FileNotFoundError(msg("entidade_canonica_inexistente", canonical_id=canonical_id, path=target.path))
    entity = note_index[canonical_id]

    merged_aliases = _dedupe_strings([*target.frontmatter.get("aliases", []), *(aliases or [])])
    merged_metadata = _merge_metadata(target.frontmatter.get("metadata", {}), metadata or {})
    target_frontmatter = dict(target.frontmatter)
    if title:
        target_frontmatter["title"] = title
    target_frontmatter["aliases"] = merged_aliases
    target_frontmatter["metadata"] = merged_metadata
    target_frontmatter["updated_at"] = utc_now()
    write_note(target.path, target_frontmatter, target.body)

    entity_frontmatter = dict(entity.frontmatter)
    if title:
        entity_frontmatter["title"] = title
        entity_frontmatter["name"] = title
    entity_frontmatter["aliases"] = _dedupe_strings([*entity.frontmatter.get("aliases", []), *merged_aliases])
    entity_frontmatter["metadata"] = _merge_metadata(entity.frontmatter.get("metadata", {}), metadata or {})
    entity_frontmatter["updated_at"] = utc_now()
    write_note(entity.path, entity_frontmatter, entity.body)

    refresh_project_materialized_views(root, project.frontmatter["project_slug"])
    return target.path


def _project_entities(root: Path, project_slug: str) -> dict[str, Note]:
    targets = project_targets(root, project_slug)
    all_notes = build_note_index(root)
    return {target.frontmatter["canonical_id"]: all_notes[target.frontmatter["canonical_id"]] for target in targets}


def _evidence_path(root: Path, project_slug: str, plugin_name: str, record_id: str) -> Path:
    slug = slugify(f"{plugin_name}-{record_id}")
    return root / "projects" / slugify(project_slug) / "evidence" / f"{slug}.md"


def _note_title(note: Note) -> str:
    return str(note.frontmatter.get("title") or note.frontmatter.get("name") or note.frontmatter.get("id") or note.path.stem)


def _relative_markdown_path(from_path: Path, to_path: Path) -> str:
    return Path(os.path.relpath(to_path, start=from_path.parent)).as_posix()


def _note_link(from_path: Path, note: Note, label: str | None = None) -> str:
    return f"[{label or _note_title(note)}]({_relative_markdown_path(from_path, note.path)})"


def _is_missing_value(value: Any) -> bool:
    return value in (None, "", [], {})


def _render_scalar(value: Any) -> str:
    if isinstance(value, bool):
        return "Sim" if value else "Não"
    if isinstance(value, list):
        return ", ".join(_render_scalar(item) for item in value)
    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return str(value)


def _path_is_within(path: Path, base: Path) -> bool:
    try:
        path.relative_to(base)
        return True
    except ValueError:
        return False


def _is_catalog_hypothesis(root: Path, note: Note) -> bool:
    return (
        note.frontmatter.get("type") == "entity"
        and note.frontmatter.get("entity_type") == "hypothesis"
        and _path_is_within(note.path, root / "registry" / "hypotheses")
    )


def _is_project_hypothesis(root: Path, project_slug: str, note: Note) -> bool:
    return (
        note.frontmatter.get("type") == "entity"
        and note.frontmatter.get("entity_type") == "hypothesis"
        and _path_is_within(note.path, root / "projects" / slugify(project_slug) / "hypotheses")
    )


def _evidence_layer_value(note: Note | dict[str, Any]) -> str:
    if isinstance(note, Note):
        frontmatter = note.frontmatter
    else:
        frontmatter = note
    explicit = str(frontmatter.get("evidence_layer", "")).strip()
    if explicit in EVIDENCE_LAYERS:
        return explicit
    role = str(frontmatter.get("evidence_role", "")).strip()
    if role == "investigative_signal":
        return "investigative_signal"
    if role == "contextual_lead":
        return "contextual_lead"
    return "identity_baseline"


def _identity_resolution_value(note: Note | dict[str, Any]) -> str:
    if isinstance(note, Note):
        frontmatter = note.frontmatter
    else:
        frontmatter = note
    explicit = str(frontmatter.get("identity_resolution_status", "")).strip()
    if explicit in IDENTITY_RESOLUTION_STATUSES:
        return explicit
    if str(frontmatter.get("plugin", "")).strip() == "tse":
        return "possible_identity_match"
    return ""


def _case_relevance_value(note: Note | dict[str, Any]) -> int:
    if isinstance(note, Note):
        frontmatter = note.frontmatter
    else:
        frontmatter = note
    try:
        explicit = int(frontmatter.get("case_relevance", 0) or 0)
    except (TypeError, ValueError):
        explicit = 0
    if explicit > 0:
        return explicit
    layer = _evidence_layer_value(frontmatter)
    confidence = float(frontmatter.get("confidence", 0.0) or 0.0)
    base = {
        "investigative_signal": 85,
        "case_support": 68,
        "identity_baseline": 35,
        "contextual_lead": 28,
    }.get(layer, 20)
    return min(100, int(base + confidence * 10))


def _supports_hypothesis_ids(note: Note | dict[str, Any]) -> list[str]:
    if isinstance(note, Note):
        frontmatter = note.frontmatter
    else:
        frontmatter = note
    return _dedupe_strings([str(item) for item in frontmatter.get("supports_hypothesis_ids", []) if str(item).strip()])


def _evidence_layer_rank(value: str) -> int:
    order = {
        "investigative_signal": 4,
        "case_support": 3,
        "identity_baseline": 2,
        "contextual_lead": 1,
        "": 0,
    }
    return order.get(str(value or "").strip(), 0)


def _metadata_lines(
    metadata: dict[str, Any],
    *,
    keys: list[str] | tuple[str, ...] | None = None,
    include_remaining: bool = False,
) -> list[str]:
    if not metadata:
        return []
    labels = {
        "ballot_name": "Nome de urna",
        "office": "Cargo",
        "party": "Partido",
        "uf": "UF",
        "election_year": "Ano eleitoral",
        "tse_candidate_id": "Identificador TSE",
        "tse_dataset": "Base TSE",
        "cnpj": "CNPJ",
        "registration_status": "Situação cadastral",
        "registered_city": "Município cadastral",
        "legal_nature": "Natureza jurídica",
        "municipality_ibge": "Município IBGE",
        "territory_ids": "Territórios",
        "tribunal_aliases": "Aliases DataJud",
        "broad_fact_status": "Origem dos fatos básicos",
        "legislature_level": "Nível legislativo",
        "current_mandate": "Mandato atual",
        "portfolio_slug": "Portfolio",
        "roster_plugin": "Fonte de roster",
        "roster_member_id": "Identificador no roster",
        "roster_url": "URL do roster",
        "assembly_name": "Órgão legislativo",
        "camara_id": "Identificador Câmara",
        "lead_score": "Pontuação de indícios",
        "official_evidence_count": "Evidências oficiais",
        "official_signal_count": "Sinais oficiais",
        "official_identity_count": "Evidências de identidade",
        "case_support_count": "Evidências de suporte ao caso",
        "official_signal_source_count": "Fontes oficiais independentes",
        "contextual_evidence_count": "Evidências contextuais",
        "contextual_domain_count": "Domínios contextuais",
        "proposed_official_link_count": "Links oficiais sugeridos",
        "organization_count": "Organizações materializadas",
        "hypothesis_count": "Hipóteses materializadas",
        "law_count": "Leis/padrões vinculados",
        "priority_snapshot": "Prioridade derivada",
        "portfolio_elevated": "Caso elevado no portfolio",
        "strong_context_reasons": "Contexto forte",
        "crossref_alert_count": "Alertas cross-project",
        "evidence_layer": "Camada da evidência",
        "identity_resolution_status": "Resolução de identidade",
        "case_relevance": "Relevância no caso",
        "supports_hypothesis_types": "Hipóteses reforçadas",
    }
    preferred_order = list(keys or labels)
    lines: list[str] = []
    seen: set[str] = set()
    for key in preferred_order:
        value = metadata.get(key)
        if _is_missing_value(value):
            continue
        lines.append(f"- {labels[key]}: `{_render_scalar(value)}`")
        seen.add(key)
    if include_remaining:
        for key in sorted(metadata):
            if key in seen or _is_missing_value(metadata.get(key)):
                continue
            lines.append(f"- {key}: `{_render_scalar(metadata[key])}`")
    return lines


def _format_source_ref(ref: dict[str, Any]) -> str:
    label = str(ref.get("source_name", "")).strip() or "fonte"
    url = ref.get("url", "")
    parts = [f"[{label}]({url})" if url else label]
    if ref.get("plugin"):
        parts.append(f"plugin `{ref['plugin']}`")
    if ref.get("record_id"):
        parts.append(f"registro `{ref['record_id']}`")
    if ref.get("publisher"):
        parts.append(f"publicador `{ref['publisher']}`")
    if ref.get("domain"):
        parts.append(f"domínio `{ref['domain']}`")
    if ref.get("query"):
        parts.append(f"consulta `{ref['query']}`")
    if ref.get("published_at"):
        parts.append(f"publicado em `{ref['published_at']}`")
    if ref.get("retrieved_from"):
        parts.append(f"coletado via `{ref['retrieved_from']}`")
    if ref.get("collected_at"):
        parts.append(f"coletado em `{ref['collected_at']}`")
    return " | ".join(parts)


def _resolved_note_for_proposed_entity(note_index: dict[str, Note], proposed: dict[str, Any]) -> Note | None:
    entity_type = str(proposed.get("entity_type", "")).strip()
    identifier = str(proposed.get("identifier", "")).strip()
    if not entity_type or not identifier:
        return None
    for note in note_index.values():
        if note.frontmatter.get("type") != "entity":
            continue
        if note.frontmatter.get("entity_type") != entity_type:
            continue
        if identifier in note.frontmatter.get("identifiers", []):
            return note
    return None


def _render_proposed_entity_line(from_path: Path, proposed: dict[str, Any], note_index: dict[str, Note]) -> str:
    label = str(proposed.get("name", "")).strip() or str(proposed.get("identifier", "")).strip() or "entidade proposta"
    relation = str(proposed.get("relation", "")).strip()
    resolved = _resolved_note_for_proposed_entity(note_index, proposed)
    link = _note_link(from_path, resolved, label) if resolved is not None else label
    suffix = f": {relation}" if relation else ""
    if resolved is not None:
        return f"- `{proposed.get('entity_type', '')}` {link}{suffix}"
    return f"- `{proposed.get('entity_type', '')}` {link}{suffix} (pendente de materialização)"


def strong_context_reasons_from_metrics(metrics: dict[str, Any], *, crossref_alert_count: int = 0) -> list[str]:
    reasons: list[str] = []
    if int(metrics.get("hypothesis_count", 0) or 0) >= 1:
        reasons.append("hipóteses materializadas")
    if int(crossref_alert_count or 0) >= 1:
        reasons.append("alertas cross-project")
    if (
        int(metrics.get("contextual_domain_count", 0) or 0) >= 2
        and int(metrics.get("proposed_official_link_count", 0) or 0) >= 1
        and int(metrics.get("official_identity_count", 0) or 0) >= 1
    ):
        reasons.append("corroboração contextual com link oficial")
    if (
        int(metrics.get("lead_score", 0) or 0) >= 60
        and (
            int(metrics.get("official_signal_count", 0) or 0) >= 1
            or int(metrics.get("official_identity_count", 0) or 0) >= 1
        )
    ):
        reasons.append("pontuação alta de indícios")
    return reasons


def portfolio_elevated_from_metrics(metrics: dict[str, Any], *, crossref_alert_count: int = 0) -> bool:
    if int(metrics.get("official_signal_source_count", 0) or 0) >= 2:
        return True
    if str(metrics.get("priority", "")).strip() == "alta_prioridade_investigativa":
        return True
    if int(metrics.get("official_signal_count", 0) or 0) < 1:
        return False
    return bool(strong_context_reasons_from_metrics(metrics, crossref_alert_count=crossref_alert_count))


def _dedupe_proposed_entities(proposed_entities: list[Any]) -> list[Any]:
    seen: set[str] = set()
    deduped: list[Any] = []
    for item in proposed_entities:
        marker = json.dumps(item.to_dict(), sort_keys=True, ensure_ascii=False)
        if marker in seen:
            continue
        seen.add(marker)
        deduped.append(item)
    return deduped


def _evidence_role_rank(value: str) -> int:
    order = {
        "investigative_signal": 3,
        "identity_baseline": 2,
        "contextual_lead": 1,
        "": 0,
    }
    return order.get(str(value or "").strip(), 0)


def _dedupe_records(records: list[EvidenceRecord]) -> list[EvidenceRecord]:
    ordered: list[EvidenceRecord] = []
    index: dict[tuple[str, str], EvidenceRecord] = {}
    for record in records:
        key = (record.plugin, record.record_id)
        existing = index.get(key)
        if existing is None:
            index[key] = record
            ordered.append(record)
            continue
        existing.related_ids = _dedupe_strings([*existing.related_ids, *record.related_ids])
        existing.proposed_entities = _dedupe_proposed_entities([*existing.proposed_entities, *record.proposed_entities])
        existing.metadata_updates = _merge_metadata(existing.metadata_updates, record.metadata_updates)
        existing.confidence = max(existing.confidence, record.confidence)
        if _evidence_role_rank(record.evidence_role) > _evidence_role_rank(existing.evidence_role):
            existing.evidence_role = record.evidence_role
        if _evidence_layer_rank(record.evidence_layer) > _evidence_layer_rank(existing.evidence_layer):
            existing.evidence_layer = record.evidence_layer
        if record.identity_resolution_status == "confirmed_identity_match":
            existing.identity_resolution_status = "confirmed_identity_match"
        elif not existing.identity_resolution_status:
            existing.identity_resolution_status = record.identity_resolution_status
        existing.case_relevance = max(int(existing.case_relevance or 0), int(record.case_relevance or 0))
        existing.supports_hypothesis_ids = _dedupe_strings([*existing.supports_hypothesis_ids, *record.supports_hypothesis_ids])
        existing.supports_hypothesis_types = _dedupe_strings([*existing.supports_hypothesis_types, *record.supports_hypothesis_types])
        if len(record.excerpt) > len(existing.excerpt):
            existing.excerpt = record.excerpt
        if len(record.claim) > len(existing.claim):
            existing.claim = record.claim
    return ordered


def _plugin_stage(plugin_name: str) -> str:
    return "broad_facts" if plugin_name in BROAD_FACT_PLUGINS else "specific"


def _ordered_plugins(plugin_names: list[str]) -> list[str]:
    indexed = list(dict.fromkeys(plugin_names))
    return sorted(indexed, key=lambda item: (item not in BROAD_FACT_PLUGINS, indexed.index(item)))


def _project_target_index(root: Path, project_slug: str) -> dict[str, list[Note]]:
    index: dict[str, list[Note]] = {}
    for target in project_targets(root, project_slug):
        index.setdefault(target.frontmatter["canonical_id"], []).append(target)
    return index


def _record_reference(record: EvidenceRecord) -> dict[str, Any]:
    payload = {
        "plugin": record.plugin,
        "source_name": record.source_name,
        "record_id": record.record_id,
        "url": record.url,
        "collected_at": utc_now(),
        "evidence_role": record.evidence_role,
        "evidence_layer": record.evidence_layer,
        "identity_resolution_status": record.identity_resolution_status,
    }
    payload.update({key: value for key, value in record.source_metadata.items() if value not in ("", None, [], {})})
    return payload


def _write_evidence_note(root: Path, project_slug: str, record: EvidenceRecord) -> Path:
    path = _evidence_path(root, project_slug, record.plugin, record.record_id)
    frontmatter = {
        "id": note_id("evidence", project_slug, record.plugin, record.record_id),
        "type": "evidence",
        "title": record.title,
        "status": "collected",
        "plugin": record.plugin,
        "source_class": record.source_class,
        "source_refs": [_record_reference(record)],
        "related_ids": list(dict.fromkeys(record.related_ids)),
        "project_ids": [project_slug],
        "confidence": record.confidence,
        "updated_at": utc_now(),
        "evidence_role": record.evidence_role,
        "evidence_layer": record.evidence_layer or (
            "contextual_lead"
            if record.evidence_role == "contextual_lead"
            else "investigative_signal"
            if record.evidence_role == "investigative_signal"
            else "identity_baseline"
        ),
        "identity_resolution_status": record.identity_resolution_status,
        "case_relevance": record.case_relevance,
        "supports_hypothesis_ids": list(dict.fromkeys(record.supports_hypothesis_ids)),
        "supports_hypothesis_types": list(dict.fromkeys(record.supports_hypothesis_types)),
        "claim": record.claim,
        "excerpt": record.excerpt,
        "chronology_date": record.chronology_date,
        "proposed_entities": [item.to_dict() for item in record.proposed_entities],
        "metadata_updates": dict(record.metadata_updates),
    }
    write_note(path, frontmatter, evidence_body(record.claim, record.excerpt, record.source_name))
    return path


def _write_artifacts(root: Path, project_slug: str, plugin_name: str, artifacts: list[dict[str, Any]]) -> None:
    artifact_root = root / "projects" / slugify(project_slug) / "runs" / "artifacts" / plugin_name
    artifact_root.mkdir(parents=True, exist_ok=True)
    for index, artifact in enumerate(artifacts):
        raw_name = artifact.get("filename") or f"{plugin_name}-{index}.json"
        suffix = Path(raw_name).suffix or ".json"
        stem = slugify(Path(raw_name).stem)
        artifact_path = artifact_root / f"{stem}{suffix}"
        if "json" in artifact:
            artifact_path.write_text(json.dumps(artifact["json"], indent=2, ensure_ascii=False), encoding="utf-8")
        elif "text" in artifact:
            artifact_path.write_text(str(artifact["text"]), encoding="utf-8")
        else:
            artifact_path.write_text(json.dumps(artifact, indent=2, ensure_ascii=False), encoding="utf-8")


def _apply_metadata_updates(
    root: Path,
    record: EvidenceRecord,
    target_index: dict[str, list[Note]],
    entity_index: dict[str, Note],
) -> list[dict[str, Any]]:
    if not record.metadata_updates:
        return []
    applied: list[dict[str, Any]] = []
    record_ref = _record_reference(record)
    keys = sorted(record.metadata_updates)
    for related_id in record.related_ids:
        if related_id not in entity_index:
            continue
        entity = entity_index[related_id]
        entity_frontmatter = dict(entity.frontmatter)
        merged_entity_metadata = _merge_metadata(entity_frontmatter.get("metadata", {}), record.metadata_updates)
        entity_changed = merged_entity_metadata != entity_frontmatter.get("metadata", {})
        refs = list(entity_frontmatter.get("source_refs", []))
        ref_signatures = {_source_ref_signature(ref) for ref in refs}
        if _source_ref_signature(record_ref) not in ref_signatures:
            refs.append(record_ref)
            entity_changed = True
        if entity_changed:
            entity_frontmatter["metadata"] = merged_entity_metadata
            entity_frontmatter["source_refs"] = refs
            entity_frontmatter["updated_at"] = utc_now()
            write_note(entity.path, entity_frontmatter, entity.body)
            applied.append(
                {
                    "scope": "entity",
                    "canonical_id": related_id,
                    "path": entity.path.relative_to(root).as_posix(),
                    "metadata_keys": keys,
                }
            )
        for target in target_index.get(related_id, []):
            target_frontmatter = dict(target.frontmatter)
            merged_target_metadata = _merge_metadata(target_frontmatter.get("metadata", {}), record.metadata_updates)
            target_changed = merged_target_metadata != target_frontmatter.get("metadata", {})
            target_refs = list(target_frontmatter.get("source_refs", []))
            target_ref_signatures = {_source_ref_signature(ref) for ref in target_refs}
            if _source_ref_signature(record_ref) not in target_ref_signatures:
                target_refs.append(record_ref)
                target_changed = True
            if target_changed:
                target_frontmatter["metadata"] = merged_target_metadata
                target_frontmatter["source_refs"] = target_refs
                target_frontmatter["updated_at"] = utc_now()
                write_note(target.path, target_frontmatter, target.body)
                applied.append(
                    {
                        "scope": "target",
                        "canonical_id": related_id,
                        "path": target.path.relative_to(root).as_posix(),
                        "metadata_keys": keys,
                    }
                )
    return applied


def sync_sources_detailed(
    root: Path,
    project_slug: str,
    plugin_names: list[str] | None = None,
    *,
    progress: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    ensure_workspace(root)
    project = get_project(root, project_slug)
    targets = project_targets(root, project_slug)
    if not targets:
        raise ValueError(msg("sem_alvos_no_projeto"))
    entity_index = _project_entities(root, project_slug)
    target_index = _project_target_index(root, project_slug)
    plugins = _ordered_plugins(plugin_names or list(project.frontmatter.get("plugin_names", [])))
    written: list[Path] = []
    summary: dict[str, Any] = {"project": project.frontmatter["project_slug"], "written_paths": [], "plugins": []}
    for plugin_name in plugins:
        plugin_summary = {
            "plugin": plugin_name,
            "stage": _plugin_stage(plugin_name),
            "record_count": 0,
            "written_paths": [],
            "next_queries": [],
            "applied_metadata_updates": [],
            "error": "",
        }
        if progress is not None:
            progress(
                {
                    "event": "plugin_start",
                    "project_slug": project_slug,
                    "plugin": plugin_name,
                    "stage": plugin_summary["stage"],
                    "at": utc_now(),
                }
            )
        plugin = get_plugin(plugin_name)
        context = PluginContext(root=root, project=project, targets=targets, entities=entity_index)
        try:
            bundle = plugin.run(context)
        except Exception as exc:
            plugin_summary["error"] = str(exc)
            plugin_summary["next_queries"] = [f"{plugin_name}: coleta interrompida por erro de rede/configuração."]
            run_payload = {
                "plugin": plugin_name,
                "records": [],
                "next_queries": plugin_summary["next_queries"],
                "proposed_links": [],
                "artifacts": [],
                "error": str(exc),
                "stage": plugin_summary["stage"],
                "applied_metadata_updates": [],
            }
            run_path = root / "projects" / project.frontmatter["project_slug"] / "runs" / f"sync-{plugin_name}.json"
            run_path.write_text(json.dumps(run_payload, indent=2, ensure_ascii=False), encoding="utf-8")
            summary["plugins"].append(plugin_summary)
            if progress is not None:
                progress(
                    {
                        "event": "plugin_finish",
                        "project_slug": project_slug,
                        "plugin": plugin_name,
                        "stage": plugin_summary["stage"],
                        "status": "error",
                        "record_count": 0,
                        "error": str(exc),
                        "at": utc_now(),
                    }
                )
            continue
        bundle.records = _dedupe_records(bundle.records)
        plugin_written: list[Path] = []
        applied_updates: list[dict[str, Any]] = []
        for record in bundle.records:
            path = _write_evidence_note(root, project.frontmatter["project_slug"], record)
            written.append(path)
            plugin_written.append(path)
            applied_updates.extend(_apply_metadata_updates(root, record, target_index, entity_index))
        _write_artifacts(root, project.frontmatter["project_slug"], plugin_name, bundle.artifacts)
        run_path = root / "projects" / project.frontmatter["project_slug"] / "runs" / f"sync-{plugin_name}.json"
        run_payload = bundle.to_dict()
        run_payload["stage"] = plugin_summary["stage"]
        run_payload["applied_metadata_updates"] = applied_updates
        run_path.write_text(json.dumps(run_payload, indent=2, ensure_ascii=False), encoding="utf-8")
        plugin_summary["record_count"] = len(bundle.records)
        plugin_summary["written_paths"] = [str(path) for path in plugin_written]
        plugin_summary["next_queries"] = list(bundle.next_queries)
        plugin_summary["applied_metadata_updates"] = applied_updates
        summary["plugins"].append(plugin_summary)
        if progress is not None:
            progress(
                {
                    "event": "plugin_finish",
                    "project_slug": project_slug,
                    "plugin": plugin_name,
                    "stage": plugin_summary["stage"],
                    "status": "records" if bundle.records else "zero_records",
                    "record_count": len(bundle.records),
                    "at": utc_now(),
                }
            )
        if applied_updates:
            project = get_project(root, project_slug)
            targets = project_targets(root, project_slug)
            entity_index = _project_entities(root, project_slug)
            target_index = _project_target_index(root, project_slug)
    materialized_paths = _materialize_proposed_entities(root, project_slug)
    written.extend(materialized_paths)
    summary["materialized_entity_paths"] = [str(path) for path in materialized_paths]
    metrics = update_project_case_metrics(root, project_slug)
    refreshed_paths = refresh_project_materialized_views(root, project_slug)
    summary["refreshed_paths"] = [str(path) for path in refreshed_paths]
    summary["project_metrics"] = metrics
    summary["written_paths"] = [str(path) for path in written]
    return summary


def sync_sources(root: Path, project_slug: str, plugin_names: list[str] | None = None) -> list[Path]:
    return list(sync_sources_detailed(root, project_slug, plugin_names)["written_paths"])


def _task_paths(root: Path, project_slug: str) -> set[Path]:
    task_root = root / "projects" / slugify(project_slug) / "tasks"
    return {path.resolve() for path in task_root.glob("*.md")}


def _evidence_paths(root: Path, project_slug: str) -> set[Path]:
    evidence_root = root / "projects" / slugify(project_slug) / "evidence"
    return {path.resolve() for path in evidence_root.glob("*.md")}


def _project_plugin_context(root: Path, project_slug: str) -> tuple[Note, list[Note], dict[str, Note]]:
    project = get_project(root, project_slug)
    targets = project_targets(root, project_slug)
    entities = _project_entities(root, project_slug)
    return project, targets, entities


def _plugin_profiles(root: Path, project_slug: str, plugin_name: str) -> tuple[Any, list[Any]]:
    project, targets, entities = _project_plugin_context(root, project_slug)
    plugin = get_plugin(plugin_name)
    context = PluginContext(root=root, project=project, targets=targets, entities=entities)
    return plugin, plugin._profiles(context)


def _project_contextual_plugins(project: Note) -> list[str]:
    return list(project.frontmatter.get("contextual_plugin_names", DEFAULT_CONTEXTUAL_PLUGINS))


def _ensure_project_contextual_defaults(root: Path, project: Note) -> Note:
    if "contextual_plugin_names" in project.frontmatter:
        return project
    frontmatter = dict(project.frontmatter)
    frontmatter["contextual_plugin_names"] = DEFAULT_CONTEXTUAL_PLUGINS
    frontmatter["updated_at"] = utc_now()
    write_note(project.path, frontmatter, project.body)
    refresh_cache(root)
    return read_note(project.path)


def _advance_skip_reason(root: Path, project_slug: str, plugin_name: str) -> str:
    _plugin, profiles = _plugin_profiles(root, project_slug, plugin_name)
    if not profiles:
        return "nenhum alvo configurado"
    if plugin_name == "tse":
        if not any(profile.entity_type == "person" and profile.search_terms for profile in profiles):
            return msg("motivo_busca_pessoa")
        return ""
    if plugin_name == "cnpj-qsa":
        if not any(profile.cnpjs for profile in profiles):
            return msg("motivo_cnpj")
        return ""
    if plugin_name == "dou":
        if not any(profile.search_terms for profile in profiles):
            return msg("motivo_termo_busca")
        return ""
    if plugin_name == "pncp":
        if not any(profile.cnpjs for profile in profiles):
            return msg("motivo_pncp")
        return ""
    if plugin_name == "datajud":
        if not any(profile.search_terms for profile in profiles):
            return msg("motivo_datajud_previo")
        if not any(profile.cpfs or profile.tribunal_aliases or profile.metadata.get("election_year") for profile in profiles):
            return msg("motivo_datajud")
        return ""
    if plugin_name == "portal-transparencia":
        if not (
            os.environ.get("INVESTIGADOR_PORTAL_API_KEY")
            or os.environ.get("PORTAL_TRANSPARENCIA_API_KEY")
            or os.environ.get("TRANSPARENCIA_API_KEY")
        ):
            return msg("motivo_portal_token")
        if not any(profile.cpfs or profile.cnpjs for profile in profiles):
            return msg("motivo_portal_documento")
        return ""
    if plugin_name == "transferegov":
        if not any(profile.cnpjs or profile.territory_ids for profile in profiles):
            return msg("motivo_transferegov")
        return ""
    if plugin_name == "tcu":
        if not any(profile.cpfs or profile.cnpjs or profile.search_terms for profile in profiles):
            return msg("motivo_tcu")
        return ""
    if plugin_name == "querido-diario":
        if not any(profile.territory_ids for profile in profiles):
            return msg("motivo_querido_diario")
        return ""
    if plugin_name in {"camara-profile", "camara-expenses", "camara-organs"}:
        if not any(
            str(profile.metadata.get("legislature_level", "")).strip().lower() == "federal"
            and (profile.metadata.get("camara_id") or profile.metadata.get("roster_member_id"))
            for profile in profiles
        ):
            return msg("motivo_camara")
        return ""
    if plugin_name == "web-search":
        if not any(profile.search_terms for profile in profiles):
            return msg("motivo_termo_busca")
        return ""
    return ""


def _empty_sync_report(project_slug: str) -> dict[str, Any]:
    return {"project": project_slug, "written_paths": [], "plugins": []}


def format_progress_event(event: dict[str, Any]) -> str:
    event_name = str(event.get("event", "")).strip()
    stage = str(event.get("stage", "")).strip()
    plugin_name = str(event.get("plugin", "")).strip()
    if event_name == "stage_start":
        return msg("progresso_etapa_inicio", stage=stage)
    if event_name == "stage_finish":
        return msg("progresso_etapa_fim", stage=stage)
    if event_name == "plugin_start":
        return msg("progresso_plugin_inicio", plugin=plugin_name, stage=stage or "etapa desconhecida")
    if event_name == "plugin_skip":
        return msg("progresso_plugin_pulado", plugin=plugin_name, reason=event.get("reason", ""))
    if event_name == "plugin_finish":
        status = str(event.get("status", "")).strip() or "concluído"
        record_count = int(event.get("record_count", 0) or 0)
        if status == "error":
            return msg("progresso_plugin_erro", plugin=plugin_name, error=event.get("error", ""))
        return msg("progresso_plugin_fim", plugin=plugin_name, status=status, record_count=record_count)
    if event_name == "metadata_gap":
        return msg("progresso_lacunas_fim")
    if event_name == "materialize_finish":
        return msg("progresso_materializacao_fim")
    return ""


def _run_plugin_stage(
    root: Path,
    project_slug: str,
    plugin_names: list[str],
    *,
    progress: Callable[[dict[str, Any]], None] | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    aggregate = _empty_sync_report(project_slug)
    skipped: list[dict[str, Any]] = []
    for plugin_name in plugin_names:
        reason = _advance_skip_reason(root, project_slug, plugin_name)
        if reason:
            skipped.append({"plugin": plugin_name, "reason": reason})
            if progress is not None:
                progress(
                    {
                        "event": "plugin_skip",
                        "project_slug": project_slug,
                        "plugin": plugin_name,
                        "stage": _plugin_stage(plugin_name),
                        "reason": reason,
                        "at": utc_now(),
                    }
                )
            continue
        report = sync_sources_detailed(root, project_slug, [plugin_name], progress=progress)
        aggregate["written_paths"].extend(report["written_paths"])
        aggregate["plugins"].extend(report["plugins"])
    aggregate["written_paths"] = sorted(dict.fromkeys(aggregate["written_paths"]))
    return aggregate, skipped


def _metadata_gap_report(root: Path, project_slug: str, promoted_updates: list[dict[str, Any]]) -> dict[str, Any]:
    targets = project_targets(root, project_slug)
    return {
        "targets": [
            {
                "target_id": target.frontmatter["id"],
                "canonical_id": target.frontmatter["canonical_id"],
                "metadata_keys": sorted(target.frontmatter.get("metadata", {}).keys()),
                "missing_hints": _target_missing_hints(target),
            }
            for target in targets
        ],
        "next_queries": _project_next_queries(root, project_slug),
        "promoted_updates": promoted_updates,
    }


def _summary_metadata_updates(sync_reports: list[dict[str, Any]]) -> list[dict[str, Any]]:
    updates: list[dict[str, Any]] = []
    for report in sync_reports:
        for plugin in report.get("plugins", []):
            updates.extend(plugin.get("applied_metadata_updates", []))
    unique: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in updates:
        marker = json.dumps(item, sort_keys=True, ensure_ascii=False)
        if marker in seen:
            continue
        seen.add(marker)
        unique.append(item)
    return unique


def _project_metadata_snapshot(root: Path, project_slug: str) -> dict[str, dict[str, Any]]:
    note_index = build_note_index(root)
    snapshot: dict[str, dict[str, Any]] = {}
    for target in project_targets(root, project_slug):
        snapshot[target.frontmatter["id"]] = {
            "path": target.path.relative_to(root).as_posix(),
            "metadata": dict(target.frontmatter.get("metadata", {})),
        }
        canonical_id = target.frontmatter["canonical_id"]
        if canonical_id in note_index:
            entity = note_index[canonical_id]
            snapshot[canonical_id] = {
                "path": entity.path.relative_to(root).as_posix(),
                "metadata": dict(entity.frontmatter.get("metadata", {})),
            }
    return snapshot


def _net_metadata_changes(before: dict[str, dict[str, Any]], after: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    changes: list[dict[str, Any]] = []
    for note_id_value in sorted(set(before) | set(after)):
        before_metadata = before.get(note_id_value, {}).get("metadata", {})
        after_metadata = after.get(note_id_value, {}).get("metadata", {})
        if before_metadata == after_metadata:
            continue
        changed_keys = sorted(
            key
            for key in set(before_metadata) | set(after_metadata)
            if before_metadata.get(key) != after_metadata.get(key)
        )
        path = after.get(note_id_value, before.get(note_id_value, {})).get("path", "")
        changes.append({"note_id": note_id_value, "path": path, "metadata_keys": changed_keys})
    return changes


def advance_project(
    root: Path,
    project_slug: str,
    provider_name: str | None = None,
    mode: str = "deep",
    progress: Callable[[dict[str, Any]], None] | None = None,
) -> Path:
    ensure_workspace(root)
    if mode not in {"baseline", "deep"}:
        raise ValueError(msg("modo_advance_invalido"))
    started_at = utc_now()
    project = _ensure_project_contextual_defaults(root, get_project(root, project_slug))
    before_evidence = _evidence_paths(root, project_slug)
    before_tasks = _task_paths(root, project_slug)
    before_metadata = _project_metadata_snapshot(root, project_slug)
    stage_order = ["broad_facts", "metadata_gap_check", "specific_official", "contextual_search"]
    if mode == "deep":
        stage_order.append("hypothesis_engine")
        stage_order.append("review")
    stage_order.append("materialize")
    stages: list[dict[str, Any]] = []
    sync_reports: list[dict[str, Any]] = []

    official_plugins = list(project.frontmatter.get("plugin_names", []))
    if mode == "baseline":
        baseline_plugins = list(project.frontmatter.get("baseline_plugin_names", DEFAULT_PORTFOLIO_BASELINE_PLUGINS))
        official_plugins = [name for name in _ordered_plugins(official_plugins) if name in baseline_plugins]
    broad_plugins = [name for name in _ordered_plugins(official_plugins) if name in BROAD_FACT_PLUGINS]
    if progress is not None:
        progress({"event": "stage_start", "project_slug": project_slug, "stage": "broad_facts", "mode": mode, "at": utc_now()})
    broad_report, broad_skipped = _run_plugin_stage(root, project_slug, broad_plugins, progress=progress)
    sync_reports.append(broad_report)
    stages.append(
        {
            "name": "broad_facts",
            "plugins_run": broad_report["plugins"],
            "plugins_skipped": broad_skipped,
            "written_paths": broad_report["written_paths"],
        }
    )
    if progress is not None:
        progress({"event": "stage_finish", "project_slug": project_slug, "stage": "broad_facts", "mode": mode, "at": utc_now()})

    promoted_updates = _summary_metadata_updates([broad_report])
    gap_report = _metadata_gap_report(root, project_slug, promoted_updates)
    stages.append({"name": "metadata_gap_check", "report": gap_report})
    if progress is not None:
        progress({"event": "metadata_gap", "project_slug": project_slug, "stage": "metadata_gap_check", "mode": mode, "at": utc_now()})

    specific_plugins = [name for name in _ordered_plugins(official_plugins) if name not in BROAD_FACT_PLUGINS]
    if progress is not None:
        progress({"event": "stage_start", "project_slug": project_slug, "stage": "specific_official", "mode": mode, "at": utc_now()})
    specific_report, specific_skipped = _run_plugin_stage(root, project_slug, specific_plugins, progress=progress)
    sync_reports.append(specific_report)
    stages.append(
        {
            "name": "specific_official",
            "plugins_run": specific_report["plugins"],
            "plugins_skipped": specific_skipped,
            "written_paths": specific_report["written_paths"],
        }
    )
    if progress is not None:
        progress({"event": "stage_finish", "project_slug": project_slug, "stage": "specific_official", "mode": mode, "at": utc_now()})

    project = get_project(root, project_slug)
    contextual_plugins = _ordered_plugins(_project_contextual_plugins(project))
    if progress is not None:
        progress({"event": "stage_start", "project_slug": project_slug, "stage": "contextual_search", "mode": mode, "at": utc_now()})
    contextual_report, contextual_skipped = _run_plugin_stage(root, project_slug, contextual_plugins, progress=progress)
    sync_reports.append(contextual_report)
    stages.append(
        {
            "name": "contextual_search",
            "plugins_run": contextual_report["plugins"],
            "plugins_skipped": contextual_skipped,
            "written_paths": contextual_report["written_paths"],
        }
    )
    if progress is not None:
        progress({"event": "stage_finish", "project_slug": project_slug, "stage": "contextual_search", "mode": mode, "at": utc_now()})

    if mode == "deep":
        hypothesis_paths = run_hypothesis_engine(root, project_slug)
        stages.append(
            {
                "name": "hypothesis_engine",
                "written_paths": [str(path) for path in hypothesis_paths],
            }
        )
        before_agent_tasks = _task_paths(root, project_slug)
        review_roles = ["entity_resolver", "collector_analyst", "skeptic"]
        agent_runs: list[dict[str, Any]] = []
        for role in review_roles:
            path = run_agent(root, project_slug, role, provider_name)
            payload = json.loads(path.read_text(encoding="utf-8"))
            agent_runs.append(
                {
                    "role": role,
                    "path": str(path),
                    "provider": payload.get("provider", ""),
                    "mode": payload.get("mode", ""),
                    "task_like_changes": len(payload.get("proposed_changes", [])),
                }
            )
        after_agent_tasks = _task_paths(root, project_slug)
        stages.append(
            {
                "name": "review",
                "roles": review_roles,
                "agent_runs": agent_runs,
                "tasks_created": [str(path) for path in sorted(after_agent_tasks - before_agent_tasks)],
            }
        )

    metrics = update_project_case_metrics(root, project_slug)
    refreshed_paths = refresh_project_materialized_views(root, project_slug)
    dossier_path = build_dossier(root, project_slug) if mode == "deep" else None
    metrics = update_project_case_metrics(root, project_slug)
    validation_errors = validate_workspace(root)
    if progress is not None:
        progress(
            {
                "event": "materialize_finish",
                "project_slug": project_slug,
                "stage": "materialize",
                "mode": mode,
                "at": utc_now(),
                "priority": metrics.get("priority", ""),
            }
        )
    stages.append(
        {
            "name": "materialize",
            "refreshed_paths": [str(path) for path in refreshed_paths],
            "dossier_path": str(dossier_path) if dossier_path else "",
            "project_metrics": metrics,
            "validation": {"ok": not validation_errors, "errors": validation_errors},
        }
    )

    after_evidence = _evidence_paths(root, project_slug)
    after_tasks = _task_paths(root, project_slug)
    after_metadata = _project_metadata_snapshot(root, project_slug)
    new_evidence_paths = sorted(after_evidence - before_evidence)
    new_task_paths = sorted(after_tasks - before_tasks)
    metadata_updates = _net_metadata_changes(before_metadata, after_metadata)
    new_evidence_notes = [read_note(path) for path in new_evidence_paths]
    new_official_count = sum(
        1
        for note in new_evidence_notes
        if note.frontmatter.get("source_class") in {"official_structured", "official_document"}
        and note.frontmatter.get("evidence_role") == "investigative_signal"
    )
    new_contextual_count = sum(
        1
        for note in new_evidence_notes
        if note.frontmatter.get("source_class") == "contextual_web"
        or note.frontmatter.get("evidence_role") == "contextual_lead"
    )
    if validation_errors:
        stop_reason = "validation_failed"
    elif not new_evidence_paths and not metadata_updates:
        stop_reason = "no_new_evidence_or_metadata"
    elif new_contextual_count > 0 and new_official_count == 0:
        stop_reason = "contextual_hits_only"
    else:
        stop_reason = "advance_completed"

    summary = {
        "project": project_slug,
        "mode": mode,
        "started_at": started_at,
        "completed_at": utc_now(),
        "provider": provider_name or os.environ.get("INVESTIGADOR_AGENT_PROVIDER", "mock"),
        "stage_order": stage_order,
        "stages": stages,
        "plugins_run": [plugin["plugin"] for report in sync_reports for plugin in report.get("plugins", [])],
        "plugins_skipped": [
            skipped
            for stage in stages
            for skipped in stage.get("plugins_skipped", [])
        ],
        "new_evidence_count": len(new_evidence_paths),
        "new_official_evidence_count": new_official_count,
        "new_contextual_evidence_count": new_contextual_count,
        "new_evidence_paths": [str(path) for path in new_evidence_paths],
        "metadata_updates_count": len(metadata_updates),
        "metadata_updates": metadata_updates,
        "tasks_created_count": len(new_task_paths),
        "tasks_created": [str(path) for path in new_task_paths],
        "project_metrics": metrics,
        "validation": {"ok": not validation_errors, "errors": validation_errors},
        "stop_reason": stop_reason,
    }
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    path = root / "projects" / slugify(project_slug) / "runs" / f"advance-{timestamp}.json"
    path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    return path


def _load_project_evidence(root: Path, project_slug: str) -> list[Note]:
    evidence_root = root / "projects" / slugify(project_slug) / "evidence"
    return [read_note(path) for path in sorted(evidence_root.glob("*.md"))]


def _load_project_runs(root: Path, project_slug: str) -> list[dict[str, Any]]:
    run_root = root / "projects" / slugify(project_slug) / "runs"
    runs: list[dict[str, Any]] = []
    for path in sorted(run_root.glob("sync-*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            continue
        payload["_path"] = path
        runs.append(payload)
    return runs


def project_case_metrics(root: Path, project_slug: str, *, note_index: dict[str, Note] | None = None) -> dict[str, Any]:
    evidence = _load_project_evidence(root, project_slug)
    runs = _load_project_runs(root, project_slug)
    official_notes = [
        note
        for note in evidence
        if note.frontmatter.get("source_class") in {"official_structured", "official_document"}
    ]
    official_identity_notes = [
        note
        for note in official_notes
        if note.frontmatter.get("evidence_role") == "identity_baseline"
        and _identity_resolution_value(note) != "rejected_homonym"
        and not (
            str(note.frontmatter.get("plugin", "")).strip() == "tse"
            and _identity_resolution_value(note) != "confirmed_identity_match"
        )
    ]
    official_signal_notes = [note for note in official_notes if note.frontmatter.get("evidence_role") == "investigative_signal"]
    case_support_notes = [note for note in official_notes if _evidence_layer_value(note) == "case_support"]
    contextual_notes = [
        note
        for note in evidence
        if note.frontmatter.get("source_class") == "contextual_web" or note.frontmatter.get("evidence_role") == "contextual_lead"
    ]
    contextual_domains = {
        str(ref.get("domain", "")).strip().lower()
        for note in contextual_notes
        for ref in note.frontmatter.get("source_refs", [])
        if str(ref.get("domain", "")).strip()
    }
    official_source_keys = {
        (
            str(ref.get("plugin", "")).strip(),
            str(ref.get("url", "")).strip() or str(ref.get("source_name", "")).strip(),
        )
        for note in official_signal_notes
        for ref in note.frontmatter.get("source_refs", [])
        if str(ref.get("url", "")).strip() or str(ref.get("source_name", "")).strip()
    }
    proposed_official_links = {
        str(link.get("url", "")).strip()
        for payload in runs
        for link in payload.get("proposed_links", [])
        if str(link.get("url", "")).strip()
        and (
            str(link.get("domain", "")).strip().endswith(".gov.br")
            or str(link.get("domain", "")).strip().endswith(".jus.br")
            or str(link.get("domain", "")).strip().endswith(".leg.br")
            or str(link.get("domain", "")).strip().endswith(".mp.br")
            or str(link.get("url", "")).strip().startswith("https://www.gov.br/")
        )
    }
    entities = _collect_related_entities(root, project_slug, note_index=note_index)
    organization_count = sum(1 for entity in entities if entity.frontmatter.get("entity_type") == "organization")
    hypothesis_count = sum(1 for entity in entities if _is_project_hypothesis(root, project_slug, entity))
    law_count = sum(1 for entity in entities if entity.frontmatter.get("entity_type") == "law")
    crossref_alert_count = len(_project_alert_notes_filtered(root, project_slug))
    priority = _priority_from_evidence(evidence)
    signal_note_weight = min(len(official_signal_notes), 2)
    lead_score = min(
        100,
        signal_note_weight * 12
        + len(case_support_notes) * 7
        + len(official_source_keys) * 20
        + len(official_identity_notes) * 6
        + len(contextual_notes) * 4
        + len(contextual_domains) * 6
        + len(proposed_official_links) * 8,
    )
    metrics = {
        "priority": priority,
        "lead_score": lead_score,
        "official_evidence_count": len(official_notes),
        "official_signal_count": len(official_signal_notes),
        "official_identity_count": len(official_identity_notes),
        "case_support_count": len(case_support_notes),
        "official_source_count": len(official_source_keys),
        "official_signal_source_count": len(official_source_keys),
        "contextual_evidence_count": len(contextual_notes),
        "contextual_domain_count": len(contextual_domains),
        "proposed_official_link_count": len(proposed_official_links),
        "organization_count": organization_count,
        "hypothesis_count": hypothesis_count,
        "law_count": law_count,
        "crossref_alert_count": crossref_alert_count,
    }
    strong_context_reasons = strong_context_reasons_from_metrics(metrics, crossref_alert_count=crossref_alert_count)
    metrics["strong_context_reasons"] = strong_context_reasons
    metrics["portfolio_elevated_candidate"] = portfolio_elevated_from_metrics(metrics, crossref_alert_count=crossref_alert_count)
    metrics["needs_rebuild"] = _project_needs_rebuild(root, project_slug, metrics)
    return metrics


def project_case_metrics_batch(
    root: Path,
    project_slugs: list[str] | tuple[str, ...] | set[str],
    *,
    note_index: dict[str, Note] | None = None,
) -> dict[str, dict[str, Any]]:
    shared_note_index = note_index or build_note_index(root)
    metrics_by_project: dict[str, dict[str, Any]] = {}
    for project_slug in sorted(dict.fromkeys(str(item) for item in project_slugs if str(item).strip())):
        metrics_by_project[project_slug] = project_case_metrics(root, project_slug, note_index=shared_note_index)
    return metrics_by_project


def update_project_case_metrics(root: Path, project_slug: str, *, note_index: dict[str, Note] | None = None, refresh_cache_enabled: bool = True) -> dict[str, Any]:
    metrics = project_case_metrics(root, project_slug, note_index=note_index)
    project = get_project(root, project_slug)
    frontmatter = dict(project.frontmatter)
    metadata = _merge_metadata(
        frontmatter.get("metadata", {}),
        {
            "lead_score": metrics["lead_score"],
            "official_evidence_count": metrics["official_evidence_count"],
            "official_signal_count": metrics["official_signal_count"],
            "official_identity_count": metrics["official_identity_count"],
            "case_support_count": metrics["case_support_count"],
            "official_signal_source_count": metrics["official_signal_source_count"],
            "contextual_evidence_count": metrics["contextual_evidence_count"],
            "contextual_domain_count": metrics["contextual_domain_count"],
            "proposed_official_link_count": metrics["proposed_official_link_count"],
            "organization_count": metrics["organization_count"],
            "hypothesis_count": metrics["hypothesis_count"],
            "law_count": metrics["law_count"],
            "crossref_alert_count": metrics["crossref_alert_count"],
            "priority_snapshot": metrics["priority"],
            "strong_context_reasons": metrics["strong_context_reasons"],
            "portfolio_elevated_candidate": metrics["portfolio_elevated_candidate"],
            "needs_rebuild": metrics["needs_rebuild"],
        },
    )
    if metadata != frontmatter.get("metadata", {}):
        frontmatter["metadata"] = metadata
        frontmatter["updated_at"] = utc_now()
        write_note(project.path, frontmatter, project.body)
    if refresh_cache_enabled:
        refresh_cache(root)
    return metrics


def update_project_case_metrics_batch(
    root: Path,
    project_slugs: list[str] | tuple[str, ...] | set[str],
    *,
    note_index: dict[str, Note] | None = None,
    refresh_cache_enabled: bool = True,
) -> dict[str, dict[str, Any]]:
    shared_note_index = note_index or build_note_index(root)
    metrics_by_project = project_case_metrics_batch(root, project_slugs, note_index=shared_note_index)
    for project_slug, metrics in metrics_by_project.items():
        project = get_project(root, project_slug)
        frontmatter = dict(project.frontmatter)
        metadata = _merge_metadata(
            frontmatter.get("metadata", {}),
            {
                "lead_score": metrics["lead_score"],
                "official_evidence_count": metrics["official_evidence_count"],
                "official_signal_count": metrics["official_signal_count"],
                "official_identity_count": metrics["official_identity_count"],
                "case_support_count": metrics["case_support_count"],
                "official_signal_source_count": metrics["official_signal_source_count"],
                "contextual_evidence_count": metrics["contextual_evidence_count"],
                "contextual_domain_count": metrics["contextual_domain_count"],
                "proposed_official_link_count": metrics["proposed_official_link_count"],
                "organization_count": metrics["organization_count"],
                "hypothesis_count": metrics["hypothesis_count"],
                "law_count": metrics["law_count"],
                "crossref_alert_count": metrics["crossref_alert_count"],
                "priority_snapshot": metrics["priority"],
                "strong_context_reasons": metrics["strong_context_reasons"],
                "portfolio_elevated_candidate": metrics["portfolio_elevated_candidate"],
                "needs_rebuild": metrics["needs_rebuild"],
            },
        )
        if metadata != frontmatter.get("metadata", {}):
            frontmatter["metadata"] = metadata
            frontmatter["updated_at"] = utc_now()
            write_note(project.path, frontmatter, project.body)
    if refresh_cache_enabled:
        refresh_cache(root)
    return metrics_by_project


def diagnose_project(root: Path, project_slug: str) -> dict[str, Any]:
    ensure_workspace(root)
    project = get_project(root, project_slug)
    targets = project_targets(root, project_slug)
    metrics = project_case_metrics(root, project_slug)
    latest_runs: dict[str, dict[str, Any]] = {}
    for payload in _load_project_runs(root, project_slug):
        plugin_name = str(payload.get("plugin", "")).strip()
        if plugin_name:
            latest_runs[plugin_name] = payload
    configured_plugins = _ordered_plugins(
        [
            *project.frontmatter.get("baseline_plugin_names", []),
            *project.frontmatter.get("plugin_names", []),
            *_project_contextual_plugins(project),
        ]
    )
    plugin_reports: list[dict[str, Any]] = []
    successful_plugins = 0
    baseline_plugins = set(project.frontmatter.get("baseline_plugin_names", []))
    for plugin_name in configured_plugins:
        payload = latest_runs.get(plugin_name)
        if payload is None:
            baseline_missing = (
                plugin_name in {"camara-profile", "camara-expenses"}
                and plugin_name in baseline_plugins
                and str(project.frontmatter.get("metadata", {}).get("legislature_level", "")).strip().lower() == "federal"
            )
            plugin_reports.append(
                {
                    "plugin": plugin_name,
                    "status": "not_run",
                    "reason": (
                        msg("razao_reparo_sem_rerun")
                        if baseline_missing
                        else _advance_skip_reason(root, project_slug, plugin_name)
                    ),
                    "stage": _plugin_stage(plugin_name),
                }
            )
            continue
        record_count = len(payload.get("records", []))
        error = str(payload.get("error", "")).strip()
        if record_count > 0:
            successful_plugins += 1
        plugin_reports.append(
            {
                "plugin": plugin_name,
                "status": "error" if error else ("records" if record_count > 0 else "zero_records"),
                "stage": payload.get("stage", _plugin_stage(plugin_name)),
                "record_count": record_count,
                "error": error,
                "next_queries": list(payload.get("next_queries", []))[:5],
                "proposed_links_count": len(payload.get("proposed_links", [])),
                "run_path": str(payload.get("_path", "")),
            }
        )

    blockers: list[str] = []
    if metrics["official_signal_count"] < 1:
        blockers.append("Nenhum sinal oficial investigativo materializado ainda.")
    if metrics["official_identity_count"] < 1:
        blockers.append("Ainda faltam fatos oficiais mínimos de identidade/contexto.")
    if metrics["contextual_domain_count"] < 2:
        blockers.append("Ainda não há diversidade suficiente de domínios contextuais.")
    if metrics["proposed_official_link_count"] < 1:
        blockers.append("Ainda não há link oficial proposto suficiente para aprofundamento contextual.")
    if metrics["official_signal_count"] >= 1:
        blockers = []
    elif (
        metrics["contextual_domain_count"] >= 2
        and metrics["proposed_official_link_count"] >= 1
        and metrics["official_identity_count"] >= 1
    ):
        blockers = []

    required_baseline_plugins_not_run = [
        plugin["plugin"]
        for plugin in plugin_reports
        if plugin.get("plugin") in {"camara-profile", "camara-expenses"}
        and plugin.get("reason") == msg("razao_reparo_sem_rerun")
    ]
    if required_baseline_plugins_not_run:
        blockers.insert(0, f"Configuração de baseline reparada, mas ainda sem rerun dos plugins: {', '.join(required_baseline_plugins_not_run)}.")

    return {
        "project": project_slug,
        "title": project.frontmatter.get("title", project_slug),
        "metrics": metrics,
        "deep_ready": not blockers,
        "deep_blockers": blockers,
        "successful_plugin_count": successful_plugins,
        "targets": [
            {
                "target_id": target.frontmatter["id"],
                "canonical_id": target.frontmatter["canonical_id"],
                "title": target.frontmatter.get("title", ""),
                "missing_hints": _target_missing_hints(target),
                "metadata_keys": sorted(target.frontmatter.get("metadata", {}).keys()),
            }
            for target in targets
        ],
        "next_queries": _project_next_queries(root, project_slug),
        "plugins": plugin_reports,
    }


def _write_note_if_changed(note: Note, frontmatter: dict[str, Any], body: str) -> bool:
    rendered = body.strip()
    if note.frontmatter == frontmatter and note.body.strip() == rendered and note.storage_format == "footer":
        return False
    updated_frontmatter = dict(frontmatter)
    updated_frontmatter["updated_at"] = utc_now()
    write_note(note.path, updated_frontmatter, rendered)
    return True


def _prune_missing_related_ids(note: Note, note_index: dict[str, Note]) -> bool:
    frontmatter = dict(note.frontmatter)
    related_ids = list(frontmatter.get("related_ids", []))
    valid_related = [related_id for related_id in related_ids if related_id in note_index]
    if valid_related == related_ids:
        return False
    frontmatter["related_ids"] = valid_related
    return _write_note_if_changed(note, frontmatter, note.body)


def _target_missing_hints(target: Note) -> list[str]:
    metadata = dict(target.frontmatter.get("metadata", {}))
    hints: list[str] = []
    if target.frontmatter.get("entity_type") == "person" and _is_missing_value(metadata.get("election_year")):
        hints.append("- Informar `metadata.election_year` ou um identificador eleitoral público.")
    if _is_missing_value(metadata.get("tribunal_aliases")):
        hints.append("- Informar `metadata.tribunal_aliases` para ampliar a cobertura do DataJud.")
    if _is_missing_value(metadata.get("territory_ids")) and _is_missing_value(metadata.get("municipality_ibge")):
        hints.append("- Informar `metadata.territory_ids` ou `metadata.municipality_ibge` para diários oficiais locais.")
    if target.frontmatter.get("entity_type") == "organization" and _is_missing_value(metadata.get("cnpj")):
        hints.append("- Informar `metadata.cnpj` para consultas cadastrais e contratações.")
    return hints


def _project_next_queries(root: Path, project_slug: str) -> list[str]:
    suggestions: list[str] = []
    for payload in _load_project_runs(root, project_slug):
        plugin = payload.get("plugin", "")
        for item in payload.get("next_queries", []):
            text = str(item).strip()
            if not text:
                continue
            suggestions.append(f"- `{plugin}`: {text}" if plugin else f"- {text}")
    return _dedupe_strings(suggestions)


def _render_target_body(
    root: Path,
    project_slug: str,
    target: Note,
    canonical: Note,
    evidence_notes: list[Note],
    note_index: dict[str, Note],
) -> str:
    metadata_lines = _metadata_lines(
        target.frontmatter.get("metadata", {}),
        keys=[
            "office",
            "assembly_name",
            "party",
            "uf",
            "election_year",
            "ballot_name",
            "legislature_level",
            "current_mandate",
            "camara_id",
            "cnpj",
            "registration_status",
            "registered_city",
            "municipality_ibge",
            "territory_ids",
        ],
    )
    source_lines = [
        f"- {_format_source_ref(ref)}"
        for ref in target.frontmatter.get("source_refs", [])
        if ref.get("url") or ref.get("source_name")
    ]
    evidence_lines = [
        f"- {_note_link(target.path, note)}: {note.frontmatter.get('claim', _note_title(note))}"
        for note in evidence_notes[:8]
    ]
    related_lines = [
        f"- {_note_link(target.path, note_index[related_id])}"
        for related_id in canonical.frontmatter.get("related_ids", [])
        if related_id in note_index and related_id != canonical.frontmatter.get("id")
    ]
    missing_lines = _target_missing_hints(target)
    if not missing_lines:
        missing_lines = ["- Continuar triangulação com outras bases oficiais e revisão humana."]
    body_lines = [
        f"# {_note_title(target)}",
        "",
        "## Identificação",
        "",
        f"- Identificador informado: `{target.frontmatter.get('identifier', '')}`",
        f"- Entidade canônica: {_note_link(target.path, canonical)}",
    ]
    aliases = target.frontmatter.get("aliases", [])
    if aliases:
        body_lines.append(f"- Aliases locais: `{', '.join(aliases)}`")
    body_lines.extend(
        [
            "",
            "## Mandato e fatos básicos",
            "",
            *(metadata_lines or ["- Ainda não há fatos básicos materializados neste alvo."]),
            "",
            "## Vínculos relevantes",
            "",
            *(related_lines or ["- Ainda não há vínculos materializados além da entidade canônica."]),
            "",
            "## Evidências recentes",
            "",
            *(evidence_lines or ["- Nenhuma evidência consolidada para este alvo ainda."]),
            "",
            "## Fontes recentes",
            "",
            *(source_lines or ["- Nenhuma fonte recente consolidada no alvo ainda."]),
            "",
            "## Lacunas imediatas",
            "",
            *missing_lines,
        ]
    )
    return "\n".join(body_lines)


def _render_entity_body(root: Path, project_slug: str, entity: Note, evidence_notes: list[Note], related_entities: list[Note]) -> str:
    entity_type = entity.frontmatter.get("entity_type", "")
    if entity_type == "law":
        metadata = dict(entity.frontmatter.get("metadata", {}))
        references = [f"- `{item}`" for item in metadata.get("references", [])] or ["- Referências em curadoria."]
        related_lines = [f"- {_note_link(entity.path, related)}" for related in related_entities[:10]] or ["- Sem vínculos explícitos ainda."]
        return "\n".join(
            [
                f"# {_note_title(entity)}",
                "",
                "## Padrão jurídico-investigativo",
                "",
                metadata.get("summary", "Padrão canônico para enquadramento cauteloso de hipóteses investigativas."),
                "",
                "## Referências",
                "",
                *references,
                "",
                "## Hipóteses e entidades relacionadas",
                "",
                *related_lines,
            ]
        )
    if entity_type == "hypothesis":
        metadata = dict(entity.frontmatter.get("metadata", {}))
        if _is_catalog_hypothesis(root, entity):
            law_lines = [
                f"- {_note_link(entity.path, related)}"
                for related in related_entities
                if related.frontmatter.get("entity_type") == "law"
            ] or ["- Sem padrões jurídicos vinculados ainda."]
            return "\n".join(
                [
                    f"# {_note_title(entity)}",
                    "",
                    "## Padrão investigativo canônico",
                    "",
                    metadata.get("summary", "Catálogo canônico de hipótese investigativa."),
                    "",
                    f"- Tipo: `{metadata.get('hypothesis_type', '')}`",
                    "",
                    "## Padrões / leis relacionados",
                    "",
                    *law_lines,
                ]
            )
        trigger_lines = [
            f"- {_note_link(entity.path, related)}"
            for related in related_entities
            if related.frontmatter.get("type") == "evidence"
        ] or ["- Nenhuma evidência gatilho materializada ainda."]
        alert_lines = [
            f"- {_note_link(entity.path, related)}"
            for related in related_entities
            if related.frontmatter.get("type") == "portfolio_alert"
        ] or ["- Nenhum alerta cross-project relevante vinculado ainda."]
        organization_lines = [
            f"- {_note_link(entity.path, related)}"
            for related in related_entities
            if related.frontmatter.get("entity_type") == "organization"
        ] or ["- Nenhuma contraparte central vinculada ainda."]
        pattern_lines = [
            f"- {_note_link(entity.path, related)}"
            for related in related_entities
            if related.frontmatter.get("entity_type") == "hypothesis" and _is_catalog_hypothesis(root, related)
        ] or ["- Padrão canônico ainda não vinculado."]
        law_lines = [
            f"- {_note_link(entity.path, related)}"
            for related in related_entities
            if related.frontmatter.get("entity_type") == "law"
        ] or ["- Nenhum padrão jurídico vinculado ainda."]
        counterevidence = [f"- {item}" for item in metadata.get("counterevidence", [])] or ["- Falta registrar contraevidências."]
        missing_proof = [f"- {item}" for item in metadata.get("missing_proof", [])] or ["- Falta registrar lacunas probatórias."]
        return "\n".join(
            [
                f"# {_note_title(entity)}",
                "",
                "## Hipótese candidata",
                "",
                metadata.get("summary", "Hipótese investigativa em aberto; linguagem cautelosa e revisão humana obrigatória."),
                "",
                f"- Tipo: `{metadata.get('hypothesis_type', '')}`",
                "",
                "## Padrão canônico",
                "",
                *pattern_lines,
                "",
                "## Contrapartes e relações centrais",
                "",
                *organization_lines,
                "",
                "## Evidências gatilho",
                "",
                *trigger_lines,
                "",
                "## Alertas cross-project relevantes",
                "",
                *alert_lines,
                "",
                "## Padrões / leis relacionados",
                "",
                *law_lines,
                "",
                "## Contraevidência obrigatória",
                "",
                *counterevidence,
                "",
                "## Prova ainda faltante",
                "",
                *missing_proof,
            ]
        )
    metadata_lines = _metadata_lines(entity.frontmatter.get("metadata", {}))
    timeline_lines = [
        f"- {note.frontmatter.get('chronology_date', 'sem data')}: {_note_link(entity.path, note, note.frontmatter.get('title', 'evidência'))}"
        for note in sorted(evidence_notes, key=lambda item: item.frontmatter.get("chronology_date", "9999-99-99"))[:10]
    ]
    relationship_lines = [
        f"- {_note_link(entity.path, related)}"
        for related in related_entities[:10]
        if related.path != entity.path
    ]
    evidence_lines = [
        f"- {_note_link(entity.path, note)}: {note.frontmatter.get('claim', _note_title(note))}"
        for note in evidence_notes[:10]
    ]
    source_lines = [
        f"- {_format_source_ref(ref)}"
        for ref in entity.frontmatter.get("source_refs", [])
        if ref.get("url") or ref.get("source_name")
    ]
    lacunas = _project_next_queries(root, project_slug)[:6] or ["- Necessário confirmar vínculos com fontes primárias adicionais."]
    body_lines = [
        f"# {_note_title(entity)}",
        "",
        "## Resumo",
        "",
        f"Entidade canônica do tipo `{entity.frontmatter.get('entity_type', 'desconhecido')}`.",
        "",
        "## Fatos confirmados",
        "",
        *(metadata_lines or ["- Ainda não há fatos básicos consolidados para esta entidade."]),
        "",
        "## Linha do tempo",
        "",
        *(timeline_lines or ["- Aguardando eventos corroborados."]),
        "",
        "## Relações",
        "",
        *(relationship_lines or ["- Aguardando ligações públicas documentadas."]),
        "",
        "## Evidências",
        "",
        *(evidence_lines or ["- Aguardando coleta."]),
        "",
        "## Fontes consolidadas",
        "",
        *(source_lines or ["- Aguardando fontes consolidadas."]),
        "",
        "## Lacunas",
        "",
        *lacunas,
    ]
    return "\n".join(body_lines)


def _render_evidence_body(root: Path, evidence: Note, note_index: dict[str, Note]) -> str:
    metadata_lines = _metadata_lines(
        evidence.frontmatter.get("metadata_updates", {}),
        keys=[
            "office",
            "assembly_name",
            "party",
            "uf",
            "election_year",
            "ballot_name",
            "legislature_level",
            "current_mandate",
            "camara_id",
            "tse_candidate_id",
            "tse_dataset",
            "cnpj",
            "registration_status",
            "registered_city",
            "legal_nature",
        ],
        include_remaining=False,
    )
    excerpt = str(evidence.frontmatter.get("excerpt") or "").strip()
    if not excerpt and "> " in evidence.body:
        excerpt = evidence.body.split("> ", 1)[1].splitlines()[0]
    if not excerpt:
        excerpt = evidence.frontmatter.get("claim", "")
    proposed_lines = [
        _render_proposed_entity_line(evidence.path, item, note_index)
        for item in evidence.frontmatter.get("proposed_entities", [])
    ]
    related_lines = [
        f"- {_note_link(evidence.path, note_index[related_id])}"
        for related_id in evidence.frontmatter.get("related_ids", [])
        if related_id in note_index
    ]
    source_lines = [
        f"- {_format_source_ref(ref)}"
        for ref in evidence.frontmatter.get("source_refs", [])
        if ref.get("url") or ref.get("source_name")
    ]
    supported_hypothesis_lines = []
    for related_id in _supports_hypothesis_ids(evidence):
        if related_id in note_index:
            supported_hypothesis_lines.append(f"- {_note_link(evidence.path, note_index[related_id])}")
    context_lines = [
        f"- Classe da fonte: `{evidence.frontmatter.get('source_class', '')}`",
        f"- Papel da evidência: `{evidence.frontmatter.get('evidence_role', '') or 'não classificado'}`",
        f"- Camada no caso: `{_evidence_layer_value(evidence)}`",
    ]
    identity_resolution = _identity_resolution_value(evidence)
    if identity_resolution:
        context_lines.append(f"- Resolução de identidade: `{identity_resolution}`")
    if evidence.frontmatter.get("chronology_date"):
        context_lines.append(f"- Data de referência: `{evidence.frontmatter.get('chronology_date', '')}`")
    if evidence.frontmatter.get("case_relevance"):
        context_lines.append(f"- Relevância no caso: `{evidence.frontmatter.get('case_relevance')}`")
    body_lines = [
        f"# {_note_title(evidence)}",
        "",
        "## Afirmação observável",
        "",
        evidence.frontmatter.get("claim", ""),
        "",
        "## Trecho utilizado",
        "",
        f"> {excerpt}",
        "",
        "## Contexto",
        "",
        *context_lines,
        "",
        "## Enriquecimento aplicado",
        "",
        *(metadata_lines or ["- Esta evidência não promoveu metadados estruturados adicionais."]),
        "",
        "## Entidades relacionadas",
        "",
        *(related_lines or ["- Nenhuma entidade relacionada consolidada ainda."]),
        "",
        "## Entidades propostas",
        "",
        *(proposed_lines or ["- Nenhuma entidade proposta nesta evidência."]),
        "",
        "## Hipóteses potencialmente sustentadas",
        "",
        *(supported_hypothesis_lines or ["- Esta evidência ainda não foi vinculada a uma hipótese específica do caso."]),
        "",
        "## Fonte",
        "",
        *(source_lines or ["- Fonte sem URL consolidada."]),
        "",
        "## Próximos passos",
        "",
        "- Classificar o peso desta evidência dentro do contexto do caso.",
        "- Buscar documentos primários e corroboradores independentes antes de qualquer conclusão pública.",
    ]
    return "\n".join(body_lines)


def _render_project_body(
    root: Path,
    project: Note,
    targets: list[Note],
    evidence: list[Note],
    tasks: list[Note],
    *,
    note_index: dict[str, Note] | None = None,
) -> str:
    project_slug = project.frontmatter["project_slug"]
    project_metadata = dict(project.frontmatter.get("metadata", {}))
    all_entities = _collect_related_entities(root, project.frontmatter["project_slug"], note_index=note_index)
    hypothesis_entities = _project_hypothesis_notes(root, project_slug, note_index=note_index)
    organization_entities = [entity for entity in all_entities if entity.frontmatter.get("entity_type") == "organization"]
    law_entities = [entity for entity in all_entities if entity.frontmatter.get("entity_type") == "law"]
    alert_notes = _project_alert_notes_filtered(root, project_slug)
    evidence_groups = _project_evidence_groups(evidence)
    metric_lines = [
        f"- Prioridade derivada: `{project_metadata.get('priority_snapshot', 'pista')}`",
        f"- Pontuação de indícios: `{project_metadata.get('lead_score', 0)}`",
        f"- Sinais oficiais: `{project_metadata.get('official_signal_count', 0)}` em `{project_metadata.get('official_signal_source_count', 0)}` fonte(s) independente(s)",
        f"- Hipóteses materializadas: `{project_metadata.get('hypothesis_count', 0)}`",
        f"- Organizações materializadas: `{project_metadata.get('organization_count', 0)}`",
        f"- Alertas úteis: `{project_metadata.get('crossref_alert_count', 0)}`",
        f"- Caso elevado no portfolio: `{'Sim' if project_metadata.get('portfolio_elevated', False) else 'Não'}`",
        f"- Precisa de rebuild narrativo: `{'Sim' if project_metadata.get('needs_rebuild', False) else 'Não'}`",
    ]
    target_lines = [f"- {_note_link(project.path, target)}" for target in targets] or ["- Nenhum alvo cadastrado."]
    top_signals = [*evidence_groups["signals"][:3], *evidence_groups["support"][:2]]
    key_signal_lines = [
        f"- {_note_link(project.path, note)}: {note.frontmatter.get('claim', _note_title(note))}"
        for note in top_signals
    ] or ["- Nenhum sinal investigativo prioritário consolidado ainda."]
    identity_lines = [
        f"- {_note_link(project.path, note)}: {note.frontmatter.get('claim', _note_title(note))}"
        for note in evidence_groups["identity"][:5]
    ] or ["- Identidade pública confirmada ainda em consolidação."]
    hypothesis_lines = [f"- {_note_link(project.path, entity)}" for entity in hypothesis_entities[:8]] or ["- Nenhuma hipótese consolidada ainda."]
    top_organizations = _top_project_organizations(root, project_slug, evidence, note_index or build_note_index(root))
    organization_lines = [f"- {_note_link(project.path, entity)}" for entity in (top_organizations or organization_entities[:8])] or ["- Nenhuma organização materializada ainda."]
    law_lines = [f"- {_note_link(project.path, entity)}" for entity in law_entities[:8]] or ["- Nenhuma lei ou padrão canônico vinculado ainda."]
    alert_lines = [
        f"- {_note_link(project.path, alert)}: {_alert_explainer(alert)}"
        for alert in alert_notes[:3]
    ] or ["- Nenhum alerta cross-project relevante vinculado ainda."]
    task_lines = [f"- {_note_link(project.path, task)}" for task in tasks[:10]] or ["- Nenhuma tarefa derivada ainda."]
    next_queries = _project_next_queries(root, project_slug) or ["- Rodar fontes públicas prioritárias."]
    official_next_steps = _preferred_next_steps(root, project_slug, limit=3, official_only=True) or ["- Consolidar a próxima rodada de coleta oficial."]
    headline_signal = _headline_signal_text(_headline_signal_note(evidence_groups))
    headline_counterparty = _headline_organization_text(top_organizations or organization_entities[:1])
    headline_alert = _headline_alert_text(alert_notes)
    headline_official_step = _headline_next_official_step(root, project_slug)
    why_case_lines = [
        f"- O caso está em `{project_metadata.get('priority_snapshot', 'pista')}` com pontuação de indícios `{project_metadata.get('lead_score', 0)}`.",
        f"- Sinal principal: {headline_signal}" if headline_signal else "- Sinal principal ainda em consolidação.",
        f"- Contraparte principal: `{headline_counterparty}`" if headline_counterparty else "- Ainda não há contraparte principal claramente destacada.",
        f"- Alerta mais útil: {headline_alert}" if headline_alert else "- Ainda não há alerta cross-project forte o suficiente para liderar a narrativa.",
        f"- Próximo passo oficial decisivo: `{headline_official_step}`" if headline_official_step else "- Ainda falta consolidar o próximo passo oficial mais decisivo.",
    ]
    return "\n".join(
        [
            f"# {_note_title(project)}",
            "",
            "## Por que este caso importa",
            "",
            *why_case_lines,
            "",
            "## Resumo operacional",
            "",
            *metric_lines,
            "",
            "## Alvos acompanhados",
            "",
            *target_lines,
            "",
            "## Top sinais concretos",
            "",
            *key_signal_lines,
            "",
            "## Contrapartes relevantes",
            "",
            *organization_lines,
            "",
            "## Alertas mais úteis",
            "",
            *alert_lines,
            "",
            "## Hipóteses do caso",
            "",
            *hypothesis_lines,
            "",
            "## Próximos passos oficiais",
            "",
            *official_next_steps,
            "",
            "## Leis e padrões vinculados",
            "",
            *law_lines,
            "",
            "## Identidade e histórico confirmado",
            "",
            *identity_lines,
            "",
            "## Próximas ações",
            "",
            *next_queries,
            "",
            "## Tarefas abertas",
            "",
            *task_lines,
        ]
    )


def _refresh_project_materialized_views_impl(
    root: Path,
    project_slug: str,
    *,
    note_index: dict[str, Note] | None = None,
    refresh_cache_enabled: bool = True,
) -> tuple[list[Path], dict[str, Note]]:
    ensure_workspace(root)
    project = get_project(root, project_slug)
    targets = project_targets(root, project_slug)
    evidence = _load_project_evidence(root, project_slug)
    task_notes = [read_note(path) for path in sorted((root / "projects" / slugify(project_slug) / "tasks").glob("*.md"))]
    working_note_index = note_index or build_note_index(root)
    touched: list[Path] = []

    candidates: list[Note] = [project, *targets, *evidence]
    entity_ids = {target.frontmatter["canonical_id"] for target in targets}
    for evidence_note in evidence:
        entity_ids.update(evidence_note.frontmatter.get("related_ids", []))
    for note_id_value, note in working_note_index.items():
        if note.frontmatter.get("type") == "entity" and project_slug in note.frontmatter.get("project_ids", []):
            entity_ids.add(note_id_value)
    entities = [working_note_index[item] for item in sorted(entity_ids) if item in working_note_index]
    candidates.extend(entities)

    for note in candidates:
        if _prune_missing_related_ids(note, working_note_index):
            touched.append(note.path)
    if touched:
        working_note_index = build_note_index(root)
    project = get_project(root, project_slug)
    targets = project_targets(root, project_slug)
    evidence = _load_project_evidence(root, project_slug)
    task_notes = [read_note(path) for path in sorted((root / "projects" / slugify(project_slug) / "tasks").glob("*.md"))]

    for target in targets:
        canonical = working_note_index[target.frontmatter["canonical_id"]]
        target_evidence = [item for item in evidence if target.frontmatter["canonical_id"] in item.frontmatter.get("related_ids", [])]
        if _write_note_if_changed(
            target,
            dict(target.frontmatter),
            _render_target_body(root, project_slug, target, canonical, target_evidence, working_note_index),
        ):
            touched.append(target.path)

    for entity_id in entity_ids:
        if entity_id not in working_note_index:
            continue
        entity = working_note_index[entity_id]
        entity_evidence = [item for item in evidence if entity_id in item.frontmatter.get("related_ids", [])]
        related_entities = [
            working_note_index[related_id]
            for related_id in entity.frontmatter.get("related_ids", [])
            if related_id in working_note_index
        ]
        if _write_note_if_changed(entity, dict(entity.frontmatter), _render_entity_body(root, project_slug, entity, entity_evidence, related_entities)):
            touched.append(entity.path)

    for evidence_note in evidence:
        if _write_note_if_changed(evidence_note, dict(evidence_note.frontmatter), _render_evidence_body(root, evidence_note, working_note_index)):
            touched.append(evidence_note.path)

    project = get_project(root, project_slug)
    project_frontmatter = dict(project.frontmatter)
    evidence_groups = _project_evidence_groups(evidence)
    top_alerts = _project_alert_notes_filtered(root, project_slug)
    top_organizations = _top_project_organizations(root, project_slug, evidence, working_note_index)
    project_frontmatter["metadata"] = _merge_metadata(
        project_frontmatter.get("metadata", {}),
        {
            "render_version": PROJECT_RENDER_VERSION,
            "headline_signal": _headline_signal_text(_headline_signal_note(evidence_groups)),
            "headline_counterparty": _headline_organization_text(top_organizations),
            "headline_alert": _headline_alert_text(top_alerts),
            "next_official_step": _headline_next_official_step(root, project_slug),
        },
    )
    project_note = Note(project.path, project_frontmatter, project.body, project.storage_format)
    if _write_note_if_changed(project, project_frontmatter, _render_project_body(root, project_note, targets, evidence, task_notes, note_index=working_note_index)):
        touched.append(project.path)

    if refresh_cache_enabled:
        refresh_cache(root)
    return sorted(dict.fromkeys(touched)), working_note_index


def refresh_project_materialized_views(
    root: Path,
    project_slug: str,
    *,
    note_index: dict[str, Note] | None = None,
    refresh_cache_enabled: bool = True,
) -> list[Path]:
    touched, _note_index = _refresh_project_materialized_views_impl(
        root,
        project_slug,
        note_index=note_index,
        refresh_cache_enabled=refresh_cache_enabled,
    )
    return touched


def refresh_project_materialized_views_batch(
    root: Path,
    project_slugs: list[str] | tuple[str, ...] | set[str],
    *,
    refresh_cache_enabled: bool = True,
) -> list[Path]:
    working_note_index = build_note_index(root)
    touched: list[Path] = []
    for project_slug in sorted(dict.fromkeys(str(item) for item in project_slugs if str(item).strip())):
        project_touched, working_note_index = _refresh_project_materialized_views_impl(
            root,
            project_slug,
            note_index=working_note_index,
            refresh_cache_enabled=False,
        )
        if project_touched:
            working_note_index = build_note_index(root)
        touched.extend(project_touched)
    if refresh_cache_enabled:
        refresh_cache(root)
    return sorted(dict.fromkeys(touched))


def _write_task(root: Path, project_slug: str, change: ProposedChange) -> Path:
    task_title = change.payload["title"]
    path = root / "projects" / slugify(project_slug) / "tasks" / f"{slugify(task_title)}.md"
    frontmatter = {
        "id": note_id("task", project_slug, task_title),
        "type": "task",
        "title": task_title,
        "status": "open",
        "source_class": "derived_workspace",
        "source_refs": [],
        "related_ids": [],
        "project_ids": [project_slug],
        "confidence": change.confidence,
        "updated_at": utc_now(),
    }
    write_note(path, frontmatter, task_body(task_title, change.payload["instructions"]))
    return path


def _materialize_proposed_entities(root: Path, project_slug: str) -> list[Path]:
    written: list[Path] = []
    evidence_notes = _load_project_evidence(root, project_slug)
    for evidence in evidence_notes:
        entity_index = build_note_index(root)
        for proposed in evidence.frontmatter.get("proposed_entities", []):
            identifier = proposed.get("identifier")
            if not identifier:
                continue
            related_entity = upsert_entity(
                root,
                proposed["entity_type"],
                identifier,
                proposed["name"],
                project_slug,
                source_refs=proposed.get("source_refs", evidence.frontmatter.get("source_refs", [])),
            )
            if related_entity.frontmatter["id"] not in evidence.frontmatter.get("related_ids", []):
                updated = dict(evidence.frontmatter)
                updated["related_ids"] = sorted(set(updated.get("related_ids", [])) | {related_entity.frontmatter["id"]})
                updated["updated_at"] = utc_now()
                write_note(evidence.path, updated, evidence.body)
                written.append(evidence.path)
            entity_index = build_note_index(root)
            for seed_id in evidence.frontmatter.get("related_ids", []):
                if seed_id not in entity_index:
                    continue
                seed = entity_index[seed_id]
                frontmatter = dict(seed.frontmatter)
                related_ids = set(frontmatter.get("related_ids", []))
                related_ids.add(related_entity.frontmatter["id"])
                frontmatter["related_ids"] = sorted(related_ids)
                frontmatter["updated_at"] = utc_now()
                write_note(seed.path, frontmatter, seed.body)
                written.append(seed.path)
    refresh_cache(root)
    return written


def run_agent(root: Path, project_slug: str, role: str, provider_name: str | None = None) -> Path:
    ensure_workspace(root)
    if role not in AGENT_ROLES:
        raise ValueError(msg("papel_desconhecido", role=role))
    project = get_project(root, project_slug)
    targets = project_targets(root, project_slug)
    evidence = _load_project_evidence(root, project_slug)
    context = {
        "project_slug": project.frontmatter["project_slug"],
        "target_ids": [target.frontmatter["canonical_id"] for target in targets],
        "evidence_ids": [item.frontmatter["id"] for item in evidence],
        "plugin_names": project.frontmatter.get("plugin_names", []),
        "contextual_plugin_names": project.frontmatter.get("contextual_plugin_names", DEFAULT_CONTEXTUAL_PLUGINS),
    }
    provider = get_provider(provider_name)
    result = provider.run(role, project, context)
    deterministic = deterministic_proposals(role, context)
    result.proposed_changes.extend(deterministic)
    if role == "entity_resolver":
        _materialize_proposed_entities(root, project_slug)
    else:
        for change in result.proposed_changes:
            if change.action == "create_task":
                _write_task(root, project.frontmatter["project_slug"], change)
    run_path = root / "projects" / project.frontmatter["project_slug"] / "runs" / f"agent-{role}.json"
    run_path.write_text(json.dumps(result.to_dict(), indent=2, ensure_ascii=False), encoding="utf-8")
    refresh_project_materialized_views(root, project_slug)
    return run_path


def _priority_from_evidence(evidence_notes: list[Note]) -> str:
    signal_notes = [
        item
        for item in evidence_notes
        if item.frontmatter.get("source_class") in {"official_structured", "official_document"}
        and item.frontmatter.get("evidence_role") == "investigative_signal"
    ]
    signal_sources = {
        (
            str(ref.get("plugin", "")).strip(),
            str(ref.get("url", "")).strip() or str(ref.get("source_name", "")).strip(),
        )
        for note in signal_notes
        for ref in note.frontmatter.get("source_refs", [])
        if str(ref.get("url", "")).strip() or str(ref.get("source_name", "")).strip()
    }
    if len(signal_sources) >= 2:
        return PRIORITY_LEVELS[2]
    if signal_notes:
        return PRIORITY_LEVELS[1]
    return PRIORITY_LEVELS[0]


def _collect_related_entities(root: Path, project_slug: str, *, note_index: dict[str, Note] | None = None) -> list[Note]:
    all_notes = note_index or build_note_index(root)
    seen = set()
    resolved: list[Note] = []
    targets = project_targets(root, project_slug)
    note_ids: set[str] = set()
    for target in targets:
        note_ids.add(target.frontmatter["canonical_id"])
    for evidence in _load_project_evidence(root, project_slug):
        note_ids.update(evidence.frontmatter.get("related_ids", []))
    for note_id_value, note in all_notes.items():
        if (
            note.frontmatter.get("type") == "entity"
            and project_slug in note.frontmatter.get("project_ids", [])
            and not _is_catalog_hypothesis(root, note)
        ):
            note_ids.add(note_id_value)
    for note_id_value in sorted(note_ids):
        if note_id_value in seen or note_id_value not in all_notes:
            continue
        note = all_notes[note_id_value]
        if _is_catalog_hypothesis(root, note):
            continue
        resolved.append(note)
        seen.add(note_id_value)
    return resolved


def _project_hypothesis_notes(root: Path, project_slug: str, *, note_index: dict[str, Note] | None = None) -> list[Note]:
    all_notes = note_index or build_note_index(root)
    notes = [
        note
        for note in all_notes.values()
        if _is_project_hypothesis(root, project_slug, note) and note.frontmatter.get("status") != "inactive"
    ]
    return sorted(notes, key=lambda item: (_case_relevance_value(item), _note_title(item)), reverse=True)


def _project_alert_notes(root: Path, project_slug: str) -> list[Note]:
    return _project_alert_notes_filtered(root, project_slug)


def _alert_relevance_value(note: Note | str) -> str:
    if isinstance(note, str):
        value = note
    else:
        value = str(note.frontmatter.get("metadata", {}).get("alert_relevance", "high_signal") or "high_signal").strip()
    return value if value in {"high_signal", "contextual", "trivial_shared_source"} else "high_signal"


def _alert_relevance_rank(note: Note | str) -> int:
    return {
        "high_signal": 3,
        "contextual": 2,
        "trivial_shared_source": 0,
    }.get(_alert_relevance_value(note), 0)


def _alert_is_visible(note: Note) -> bool:
    return _alert_relevance_rank(note) > 0


def _alert_explainer(note: Note) -> str:
    metadata = dict(note.frontmatter.get("metadata", {}))
    return str(metadata.get("explainer", "")).strip() or "Alerta derivado a partir de elo determinístico entre projetos."


def _project_alert_notes_filtered(root: Path, project_slug: str, *, include_trivial: bool = False) -> list[Note]:
    project = get_project(root, project_slug)
    portfolio_slug = str(project.frontmatter.get("metadata", {}).get("portfolio_slug", "")).strip()
    if not portfolio_slug:
        return []
    alert_root = root / "portfolios" / slugify(portfolio_slug) / "alerts"
    if not alert_root.exists():
        return []
    notes: list[Note] = []
    for path in sorted(alert_root.glob("*.md")):
        note = read_note(path)
        if note.frontmatter.get("status") != "active":
            continue
        if project_slug in note.frontmatter.get("project_ids", []):
            if not include_trivial and not _alert_is_visible(note):
                continue
            notes.append(note)
    return sorted(
        notes,
        key=lambda item: (
            _alert_relevance_rank(item),
            _project_alert_strength(item),
            _note_title(item),
        ),
        reverse=True,
    )


def _project_next_query_items(root: Path, project_slug: str) -> list[dict[str, str]]:
    items: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    plugin_priority = {
        "camara-expenses": 0,
        "camara-organs": 1,
        "pncp": 2,
        "portal-transparencia": 3,
        "transferegov": 4,
        "cnpj-qsa": 5,
        "tcu": 6,
        "datajud": 7,
        "dou": 8,
        "tse": 9,
        "querido-diario": 10,
        "web-search": 99,
    }
    for payload in _load_project_runs(root, project_slug):
        plugin = str(payload.get("plugin", "")).strip()
        for item in payload.get("next_queries", []):
            text = str(item or "").strip()
            if not text:
                continue
            marker = (plugin, text)
            if marker in seen:
                continue
            seen.add(marker)
            items.append(
                {
                    "plugin": plugin,
                    "text": text,
                    "priority": str(plugin_priority.get(plugin, 50)),
                }
            )
    return sorted(items, key=lambda item: (int(item["priority"]), item["plugin"], item["text"]))


def _preferred_next_steps(root: Path, project_slug: str, *, limit: int = 3, official_only: bool = False) -> list[str]:
    items = _project_next_query_items(root, project_slug)
    if official_only:
        items = [item for item in items if item["plugin"] != "web-search"]
    lines = [
        f"- `{item['plugin']}`: {item['text']}"
        for item in items[:limit]
    ]
    return lines


def _headline_signal_note(evidence_groups: dict[str, list[Note]]) -> Note | None:
    for group_name in ("signals", "support"):
        if evidence_groups[group_name]:
            return evidence_groups[group_name][0]
    return None


def _headline_signal_text(note: Note | None) -> str:
    if note is None:
        return ""
    return str(note.frontmatter.get("claim", "")).strip() or _note_title(note)


def _headline_organization_text(organizations: list[Note]) -> str:
    return _note_title(organizations[0]) if organizations else ""


def _headline_alert_text(alerts: list[Note]) -> str:
    return _alert_explainer(alerts[0]) if alerts else ""


def _headline_next_official_step(root: Path, project_slug: str) -> str:
    steps = _preferred_next_steps(root, project_slug, limit=1, official_only=True)
    return steps[0].removeprefix("- ").strip() if steps else ""


def _note_render_version(note: Note) -> int:
    try:
        return int(note.frontmatter.get("metadata", {}).get("render_version", 0) or 0)
    except (TypeError, ValueError):
        return 0


def _project_needs_rebuild(root: Path, project_slug: str, metrics: dict[str, Any]) -> bool:
    project = get_project(root, project_slug)
    interesting_case = (
        int(metrics.get("official_signal_count", 0) or 0) >= 1
        or int(metrics.get("hypothesis_count", 0) or 0) >= 1
        or portfolio_elevated_from_metrics(metrics, crossref_alert_count=int(metrics.get("crossref_alert_count", 0) or 0))
    )
    if not interesting_case:
        return False
    if _note_render_version(project) < PROJECT_RENDER_VERSION:
        return True
    dossier_path = root / "projects" / slugify(project_slug) / "dossiers" / "draft.md"
    if not dossier_path.exists():
        return True
    dossier = read_note(dossier_path)
    if _note_render_version(dossier) < DOSSIER_RENDER_VERSION:
        return True
    if int(metrics.get("official_signal_count", 0) or 0) >= 1 and int(metrics.get("hypothesis_count", 0) or 0) == 0:
        return True
    return False


def _project_evidence_groups(evidence: list[Note]) -> dict[str, list[Note]]:
    filtered_identity = [
        note
        for note in evidence
        if _evidence_layer_value(note) == "identity_baseline"
        and (
            str(note.frontmatter.get("plugin", "")).strip() != "tse"
            or _identity_resolution_value(note) == "confirmed_identity_match"
        )
    ]
    return {
        "identity": sorted(filtered_identity, key=lambda item: (_case_relevance_value(item), item.frontmatter.get("chronology_date", "")), reverse=True),
        "signals": sorted(
            [note for note in evidence if _evidence_layer_value(note) == "investigative_signal"],
            key=lambda item: (_case_relevance_value(item), float(item.frontmatter.get("confidence", 0.0) or 0.0)),
            reverse=True,
        ),
        "support": sorted(
            [note for note in evidence if _evidence_layer_value(note) == "case_support"],
            key=lambda item: (_case_relevance_value(item), float(item.frontmatter.get("confidence", 0.0) or 0.0)),
            reverse=True,
        ),
        "contextual": sorted(
            [note for note in evidence if _evidence_layer_value(note) == "contextual_lead"],
            key=lambda item: (_case_relevance_value(item), float(item.frontmatter.get("confidence", 0.0) or 0.0)),
            reverse=True,
        ),
    }


def _top_project_organizations(root: Path, project_slug: str, evidence: list[Note], note_index: dict[str, Note]) -> list[Note]:
    counts: Counter[str] = Counter()
    for note in evidence:
        for related_id in note.frontmatter.get("related_ids", []):
            related = note_index.get(related_id)
            if related is None or related.frontmatter.get("entity_type") != "organization":
                continue
            counts[related_id] += 2 if _evidence_layer_value(note) == "investigative_signal" else 1
    ranked = [note_index[item] for item, _count in counts.most_common(8) if item in note_index]
    return ranked


def _upsert_analysis_entity(
    root: Path,
    project_slug: str,
    *,
    entity_type: str,
    identifier: str,
    title: str,
    metadata: dict[str, Any],
    related_ids: list[str] | None = None,
) -> Note:
    entity = upsert_entity(root, entity_type, identifier, title, project_slug, metadata=metadata)
    frontmatter = dict(entity.frontmatter)
    frontmatter["metadata"] = _merge_metadata(frontmatter.get("metadata", {}), metadata)
    frontmatter["related_ids"] = sorted(set(frontmatter.get("related_ids", [])) | set(related_ids or []))
    frontmatter["updated_at"] = utc_now()
    write_note(entity.path, frontmatter, entity.body)
    return read_note(entity.path)


def _ensure_law_entity(root: Path, project_slug: str, law_key: str) -> Note:
    spec = LAW_CATALOG[law_key]
    return _upsert_analysis_entity(
        root,
        project_slug,
        entity_type="law",
        identifier=f"law:{law_key}",
        title=spec["title"],
        metadata={
            "law_key": law_key,
            "summary": spec["summary"],
            "references": list(spec["references"]),
        },
    )


def _ensure_hypothesis_catalog_entity(root: Path, project_slug: str, hypothesis_type: str) -> Note:
    spec = HYPOTHESIS_CATALOG[hypothesis_type]
    return _upsert_analysis_entity(
        root,
        project_slug,
        entity_type="hypothesis",
        identifier=f"hypothesis-pattern:{hypothesis_type}",
        title=spec["title"],
        metadata={
            "hypothesis_type": hypothesis_type,
            "summary": spec["summary"],
            "catalog_pattern": True,
        },
        related_ids=[],
    )


def _project_hypothesis_path(root: Path, project_slug: str, hypothesis_type: str) -> Path:
    return root / "projects" / slugify(project_slug) / "hypotheses" / f"{slugify(hypothesis_type)}.md"


def _upsert_project_hypothesis(
    root: Path,
    project_slug: str,
    *,
    hypothesis_type: str,
    title: str,
    summary: str,
    trigger_notes: list[Note],
    law_entities: list[Note],
    organization_ids: list[str],
    alert_notes: list[Note],
    pattern_note: Note,
) -> Note:
    path = _project_hypothesis_path(root, project_slug, hypothesis_type)
    existing = read_note(path) if path.exists() else None
    related_ids = [pattern_note.frontmatter["id"], *organization_ids]
    related_ids.extend(note.frontmatter["id"] for note in trigger_notes)
    related_ids.extend(law.frontmatter["id"] for law in law_entities)
    related_ids.extend(note.frontmatter["id"] for note in alert_notes)
    counterevidence = _dedupe_strings(
        line
        for note in trigger_notes
        for line in _hypothesis_counterevidence(note)
    )
    missing_proof = _dedupe_strings(
        line
        for note in trigger_notes
        for line in _hypothesis_missing_proof(note)
    )
    frontmatter = {
        "id": existing.frontmatter["id"] if existing else note_id("hypothesis-case", project_slug, hypothesis_type),
        "type": "entity",
        "entity_type": "hypothesis",
        "title": title,
        "name": title,
        "status": "active",
        "source_class": "derived_workspace",
        "source_refs": [],
        "related_ids": _dedupe_strings(related_ids),
        "project_ids": [project_slug],
        "identifiers": [f"hypothesis-case:{project_slug}:{hypothesis_type}"],
        "aliases": [],
        "metadata": {
            "hypothesis_type": hypothesis_type,
            "summary": summary,
            "catalog_pattern_id": pattern_note.frontmatter["id"],
            "trigger_evidence_ids": [note.frontmatter["id"] for note in trigger_notes],
            "law_ids": [law.frontmatter["id"] for law in law_entities],
            "alert_ids": [note.frontmatter["id"] for note in alert_notes],
            "organization_ids": list(organization_ids),
            "counterevidence": counterevidence,
            "missing_proof": missing_proof,
        },
        "confidence": min(0.95, 0.62 + len(trigger_notes) * 0.04 + len(alert_notes) * 0.03),
        "updated_at": utc_now(),
    }
    body = "\n".join(
        [
            f"# {title}",
            "",
            "## Hipótese do caso",
            "",
            summary,
        ]
    )
    write_note(path, frontmatter, body)
    return read_note(path)


def _project_alert_strength(note: Note) -> float:
    metadata = dict(note.frontmatter.get("metadata", {}))
    try:
        return float(metadata.get("alert_strength", note.frontmatter.get("confidence", 0.0)) or 0.0)
    except (TypeError, ValueError):
        return float(note.frontmatter.get("confidence", 0.0) or 0.0)


def _matrix_rows_from_hypotheses(
    root: Path,
    project_slug: str,
    evidence_notes: list[Note],
    hypothesis_notes: list[Note],
    note_index: dict[str, Note],
) -> list[str]:
    matrix_rows = [
        "| Hipótese | Afirmação observável | Tipo de evidência | Fonte | Força | Papel no caso | Lacuna restante |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    evidence_by_id = {note.frontmatter.get("id", ""): note for note in evidence_notes}
    selected_rows: dict[str, dict[str, Any]] = {}
    hypothesis_priority = {
        "expense_anomaly": 4,
        "procurement_risk": 4,
        "sanction_or_control_risk": 4,
        "relationship_network_risk": 3,
        "official_signals_review": 2,
    }
    for hypothesis in hypothesis_notes:
        hypothesis_title = _note_title(hypothesis)
        missing = list(hypothesis.frontmatter.get("metadata", {}).get("missing_proof", []))
        hypothesis_type = str(hypothesis.frontmatter.get("metadata", {}).get("hypothesis_type", "")).strip()
        for evidence_id in hypothesis.frontmatter.get("metadata", {}).get("trigger_evidence_ids", [])[:4]:
            evidence = evidence_by_id.get(evidence_id) or note_index.get(evidence_id)
            if evidence is None or evidence.frontmatter.get("type") != "evidence":
                continue
            if _evidence_layer_value(evidence) not in {"investigative_signal", "case_support"}:
                continue
            ref = (evidence.frontmatter.get("source_refs") or [{}])[0]
            source_name = str(ref.get("source_name", "")).strip() or "fonte não informada"
            strength = f"{float(evidence.frontmatter.get('confidence', 0.0) or 0.0):.2f}"
            role_label = {
                "investigative_signal": "gatilho principal",
                "case_support": "suporte ao caso",
            }.get(_evidence_layer_value(evidence), "apoio")
            remaining_gap = missing[0] if missing else "Ampliar documentação primária e contestação."
            candidate = {
                "hypothesis_title": hypothesis_title,
                "claim": evidence.frontmatter.get("claim", _note_title(evidence)),
                "hypothesis_type": hypothesis_type or _evidence_layer_value(evidence),
                "source_name": source_name,
                "strength": strength,
                "role_label": role_label,
                "remaining_gap": remaining_gap,
                "sort_key": (
                    hypothesis_priority.get(hypothesis_type, 1),
                    _case_relevance_value(evidence),
                    float(evidence.frontmatter.get("confidence", 0.0) or 0.0),
                ),
            }
            existing = selected_rows.get(evidence.frontmatter.get("id", ""))
            if existing is None or candidate["sort_key"] > existing["sort_key"]:
                selected_rows[evidence.frontmatter.get("id", "")] = candidate
    ordered_rows = sorted(selected_rows.values(), key=lambda item: item["sort_key"], reverse=True)[:8]
    for item in ordered_rows:
        matrix_rows.append(
            "| "
            f"{item['hypothesis_title']} | "
            f"{item['claim']} | "
            f"{item['hypothesis_type']} | "
            f"{item['source_name']} | "
            f"{item['strength']} | "
            f"{item['role_label']} | "
            f"{item['remaining_gap']} |"
        )
    if not ordered_rows:
        matrix_rows.append("| Sem hipóteses aplicadas | Sem sinais investigativos priorizados ainda | n/a | n/a | 0.00 | n/a | Consolidar sinais menores e hipóteses do caso. |")
    return matrix_rows


def _hypothesis_counterevidence(note: Note) -> list[str]:
    plugin = str(note.frontmatter.get("plugin", "")).strip()
    hints = {
        "camara-expenses": "Conferir a regularidade normativa da despesa, a documentação fiscal e o contexto parlamentar antes de tratar o gasto como anômalo.",
        "pncp": "Conferir competição, justificativas formais, preços de referência e execução contratual antes de tratar a contratação como irregular.",
        "portal-transparencia": "Registro sancionatório ou cadastral não equivale a culpa definitiva; verificar fase, alcance e eventual baixa do apontamento.",
        "datajud": "Existência de processo ou menção judicial não equivale a condenação; confirmar classe, polo, fase e resultado.",
        "tcu": "Certidões e listas públicas exigem leitura contextual do apontamento, da situação atual e da parte efetivamente atingida.",
    }
    return [hints.get(plugin, "É necessário testar homônimos, explicações normativas e contexto administrativo antes de consolidar a hipótese.")]


def _hypothesis_missing_proof(note: Note) -> list[str]:
    plugin = str(note.frontmatter.get("plugin", "")).strip()
    hints = {
        "camara-expenses": [
            "Obter documentação primária da despesa, notas fiscais e justificativas completas.",
            "Comparar padrões históricos do parlamentar e benchmarks equivalentes.",
        ],
        "pncp": [
            "Obter edital, contrato, aditivos, medições e benchmark de preços.",
            "Identificar fornecedores, sócios e vínculos públicos correlatos.",
        ],
        "portal-transparencia": [
            "Confirmar o registro completo, vigência, fundamento e entidade efetivamente sancionada.",
            "Cruzar o apontamento com outras fontes oficiais independentes.",
        ],
        "datajud": [
            "Obter número do processo, peças públicas e fase atual para qualificar o risco.",
            "Verificar se a pessoa citada é a mesma entidade canônica do caso.",
        ],
        "tcu": [
            "Obter acórdão, certidão ou inteiro teor do apontamento.",
            "Cruzar com contratos, repasses e contrapartes ligadas ao caso.",
        ],
    }
    return hints.get(
        plugin,
        [
            "Confirmar a identidade envolvida e obter documentos primários adicionais.",
            "Buscar uma segunda fonte oficial independente antes de elevar a hipótese.",
        ],
    )


def run_hypothesis_engine(root: Path, project_slug: str) -> list[Path]:
    ensure_workspace(root)
    evidence = _load_project_evidence(root, project_slug)
    if not evidence:
        return []
    note_index = build_note_index(root)
    organization_ids = sorted(
        {
            related_id
            for note in evidence
            for related_id in note.frontmatter.get("related_ids", [])
            if related_id in note_index and note_index[related_id].frontmatter.get("entity_type") == "organization"
        }
    )
    alert_notes = _project_alert_notes(root, project_slug)
    signal_notes = [
        note
        for note in evidence
        if note.frontmatter.get("source_class") in {"official_structured", "official_document"}
        and note.frontmatter.get("evidence_role") == "investigative_signal"
    ]
    support_notes = [
        note
        for note in evidence
        if _evidence_layer_value(note) == "case_support"
        and note.frontmatter.get("source_class") in {"official_structured", "official_document"}
    ]
    hypotheses_to_write: list[dict[str, Any]] = []
    plugin_groups: dict[str, list[Note]] = {}
    for note in [*signal_notes, *support_notes]:
        plugin_groups.setdefault(str(note.frontmatter.get("plugin", "")), []).append(note)
    if signal_notes:
        hypotheses_to_write.append(
            {
                "hypothesis_type": "official_signals_review",
                "title": HYPOTHESIS_CATALOG["official_signals_review"]["title"],
                "law_keys": ["conflito-de-interesses-e-improbidade"],
                "trigger_notes": signal_notes[:6],
                "summary": "O caso já possui sinais oficiais suficientes para justificar aprofundamento, contestação estruturada e documentação primária adicional.",
            }
        )
    if plugin_groups.get("camara-expenses"):
        expense_triggers = sorted(
            plugin_groups["camara-expenses"],
            key=lambda item: (_case_relevance_value(item), float(item.frontmatter.get("confidence", 0.0) or 0.0)),
            reverse=True,
        )[:6]
        hypotheses_to_write.append(
            {
                "hypothesis_type": "expense_anomaly",
                "title": HYPOTHESIS_CATALOG["expense_anomaly"]["title"],
                "law_keys": ["uso-de-verbas-publicas"],
                "trigger_notes": expense_triggers,
                "summary": "Há sinais oficiais de concentração, recorrência ou composição de despesas que merecem benchmark, documentação primária e revisão humana específica.",
            }
        )
    procurement_notes = [
        note
        for plugin_name in ("pncp", "transferegov", "querido-diario")
        for note in plugin_groups.get(plugin_name, [])
    ]
    if procurement_notes:
        hypotheses_to_write.append(
            {
                "hypothesis_type": "procurement_risk",
                "title": HYPOTHESIS_CATALOG["procurement_risk"]["title"],
                "law_keys": ["integridade-contratacoes-publicas"],
                "trigger_notes": procurement_notes[:6],
                "summary": "Há registros oficiais de contratação, publicação ou repasse que merecem verificação de integridade, capacidade e execução.",
            }
        )
    control_notes = [
        note
        for plugin_name in ("portal-transparencia", "datajud", "tcu")
        for note in plugin_groups.get(plugin_name, [])
    ]
    if control_notes:
        hypotheses_to_write.append(
            {
                "hypothesis_type": "sanction_or_control_risk",
                "title": HYPOTHESIS_CATALOG["sanction_or_control_risk"]["title"],
                "law_keys": ["sancoes-e-controle-externo"],
                "trigger_notes": control_notes[:6],
                "summary": "Há apontamentos sancionatórios, processuais ou de controle externo que exigem contextualização e revisão cautelosa.",
            }
        )
    relationship_triggers = sorted(
        [*signal_notes, *support_notes],
        key=lambda item: (_case_relevance_value(item), float(item.frontmatter.get("confidence", 0.0) or 0.0)),
        reverse=True,
    )
    if organization_ids and (relationship_triggers or alert_notes):
        hypotheses_to_write.append(
            {
                "hypothesis_type": "relationship_network_risk",
                "title": HYPOTHESIS_CATALOG["relationship_network_risk"]["title"],
                "law_keys": ["conflito-de-interesses-e-improbidade", "integridade-contratacoes-publicas"],
                "trigger_notes": relationship_triggers[:6],
                "summary": "Há contrapartes, fornecedores ou relações cruzadas suficientemente documentadas para merecer teste de conflito de interesses e expansão da rede.",
            }
        )
    written: list[Path] = []
    active_hypothesis_types: set[str] = set()
    evidence_support_map: dict[str, set[str]] = {}
    for item in hypotheses_to_write:
        active_hypothesis_types.add(item["hypothesis_type"])
        pattern_note = _ensure_hypothesis_catalog_entity(root, project_slug, item["hypothesis_type"])
        law_entities = [_ensure_law_entity(root, project_slug, key) for key in item["law_keys"]]
        relevant_alerts = [
            note
            for note in alert_notes
            if any(
                hypothesis_type in note.frontmatter.get("metadata", {}).get("linked_hypothesis_types", [])
                for hypothesis_type in [item["hypothesis_type"]]
            )
        ][:5]
        hypothesis = _upsert_project_hypothesis(
            root,
            project_slug,
            hypothesis_type=item["hypothesis_type"],
            title=item["title"],
            summary=item["summary"],
            trigger_notes=item["trigger_notes"],
            law_entities=law_entities,
            organization_ids=organization_ids,
            alert_notes=relevant_alerts,
            pattern_note=pattern_note,
        )
        written.append(hypothesis.path)
        written.append(pattern_note.path)
        written.extend(law.path for law in law_entities)
        for note in item["trigger_notes"]:
            evidence_support_map.setdefault(note.frontmatter["id"], set()).add(hypothesis.frontmatter["id"])
    hypothesis_root = root / "projects" / slugify(project_slug) / "hypotheses"
    hypothesis_root.mkdir(parents=True, exist_ok=True)
    for path in sorted(hypothesis_root.glob("*.md")):
        note = read_note(path)
        note_type = str(note.frontmatter.get("metadata", {}).get("hypothesis_type", "")).strip()
        if note_type in active_hypothesis_types:
            continue
        updated = dict(note.frontmatter)
        updated["status"] = "inactive"
        updated["updated_at"] = utc_now()
        write_note(path, updated, note.body)
        written.append(path)
    for evidence_note in evidence:
        updated = dict(evidence_note.frontmatter)
        support_ids = sorted(evidence_support_map.get(evidence_note.frontmatter["id"], set()))
        current_support_ids = _supports_hypothesis_ids(evidence_note)
        support_types = []
        if support_ids:
            support_types = sorted(
                {
                    str(read_note(path).frontmatter.get("metadata", {}).get("hypothesis_type", "")).strip()
                    for path in hypothesis_root.glob("*.md")
                    if read_note(path).frontmatter.get("id") in support_ids
                }
            )
        if current_support_ids != support_ids or updated.get("supports_hypothesis_types", []) != support_types:
            updated["supports_hypothesis_ids"] = support_ids
            updated["supports_hypothesis_types"] = support_types
            updated["updated_at"] = utc_now()
            write_note(evidence_note.path, updated, evidence_note.body)
            written.append(evidence_note.path)
    refresh_cache(root)
    return sorted(dict.fromkeys(written))


def build_dossier(root: Path, project_slug: str) -> Path:
    ensure_workspace(root)
    project = get_project(root, project_slug)
    path = root / "projects" / project.frontmatter["project_slug"] / "dossiers" / "draft.md"
    evidence = _load_project_evidence(root, project_slug)
    note_index = build_note_index(root)
    entities = _collect_related_entities(root, project_slug, note_index=note_index)
    task_notes = [read_note(path) for path in sorted((root / "projects" / slugify(project_slug) / "tasks").glob("*.md"))]
    metrics = project_case_metrics(root, project_slug, note_index=note_index)
    priority = metrics["priority"]
    evidence_groups = _project_evidence_groups(evidence)
    hypotheses = _project_hypothesis_notes(root, project_slug, note_index=note_index)
    alerts = _project_alert_notes_filtered(root, project_slug)
    organizations = _top_project_organizations(root, project_slug, evidence, note_index)
    project_metadata = dict(project.frontmatter.get("metadata", {}))
    strong_context = list(project_metadata.get("strong_context_reasons", []))

    why_elevated_lines = [
        f"- Sinais oficiais investigativos: `{metrics['official_signal_count']}`",
        f"- Fontes oficiais independentes: `{metrics['official_signal_source_count']}`",
    ]
    if strong_context:
        why_elevated_lines.append(f"- Contexto forte determinístico: `{', '.join(strong_context)}`")
    if alerts:
        why_elevated_lines.append(f"- Alertas cross-project relevantes: `{len(alerts)}`")
    headline_signal_note = _headline_signal_note(evidence_groups)
    headline_signal = _headline_signal_text(headline_signal_note)
    headline_counterparty = _headline_organization_text(organizations)
    headline_alert = _headline_alert_text(alerts)
    headline_official_step = _headline_next_official_step(root, project_slug)
    case_importance_lines = [
        f"- O caso importa porque já há `{metrics['official_signal_count']}` sinal(is) oficial(is) com prioridade `{priority}`.",
        f"- Principal sinal concreto: {headline_signal}" if headline_signal else "- Principal sinal concreto ainda em consolidação.",
        f"- Principal contraparte: `{headline_counterparty}`" if headline_counterparty else "- Ainda não há contraparte principal bem delimitada.",
        f"- Alerta mais útil: {headline_alert}" if headline_alert else "- Ainda não há alerta cross-project forte o suficiente para abrir o dossiê.",
        f"- Próximo documento oficial a buscar: `{headline_official_step}`" if headline_official_step else "- O próximo documento oficial mais útil ainda não foi claramente delimitado.",
    ]

    top_signal_lines = [
        f"- {_note_link(path, note)}: {note.frontmatter.get('claim', _note_title(note))}"
        for note in [*evidence_groups["signals"][:3], *evidence_groups["support"][:2]]
    ] or ["- Ainda não há sinais oficiais priorizados suficientes."]

    organization_lines = [
        f"- {_note_link(path, entity)}"
        for entity in organizations[:8]
    ] or ["- Nenhuma contraparte relevante materializada ainda."]

    alert_lines = []
    for alert in alerts[:5]:
        alert_lines.append(f"- {_note_link(path, alert)} [{_alert_relevance_value(alert)}]: {_alert_explainer(alert)}")
    if not alert_lines:
        alert_lines = ["- Nenhum alerta cross-project relevante consolidado ainda."]

    case_timeline_lines = [
        f"- {note.frontmatter.get('chronology_date', 'sem data')}: {_note_link(path, note)}"
        for note in sorted(
            [*evidence_groups["signals"], *evidence_groups["support"]],
            key=lambda item: item.frontmatter.get("chronology_date", "9999-99-99"),
        )[:12]
    ] or ["- Nenhum evento investigativo consolidado ainda."]

    identity_timeline_lines = [
        f"- {note.frontmatter.get('chronology_date', 'sem data')}: {_note_link(path, note)}"
        for note in sorted(evidence_groups["identity"], key=lambda item: item.frontmatter.get("chronology_date", "9999-99-99"))[:6]
    ] or ["- A identidade pública confirmada ainda depende de consolidação adicional."]

    matrix_rows = _matrix_rows_from_hypotheses(root, project_slug, evidence, hypotheses, note_index)

    hypothesis_lines = [
        f"- {_note_link(path, entity)}: {entity.frontmatter.get('metadata', {}).get('summary', 'Hipótese do caso em revisão humana.')}"
        for entity in hypotheses
    ] or ["- Ainda não há hipótese específica do caso materializada; usar a evidência atual apenas como triagem."]

    contestation_lines = [
        f"- {task.frontmatter['title']}"
        for task in task_notes
        if "contest" in slugify(task.frontmatter["title"]) or "contestação" in task.body.lower()
    ] or [
        "- Falta concluir contestação manual sobre homônimos, explicações normativas e benchmarks alternativos."
    ]

    next_actions = [
        f"- {task.frontmatter['title']}"
        for task in task_notes
    ] or [
        "- Rodar `agent run --role skeptic` antes de divulgar qualquer resumo.",
        "- Expandir fontes oficiais de segunda onda conforme o caso exigir.",
    ]
    official_next_steps = _preferred_next_steps(root, project_slug, limit=4, official_only=True) or ["- Consolidar a próxima rodada de coleta oficial."]

    body = "\n".join(
        [
            f"# Dossiê: {project.frontmatter['title']}",
            "",
            "## Resumo executivo",
            "",
            (
                f"Este dossiê é um rascunho de triagem investigativa. O caso está em nível "
                f"`{priority}` e reúne {len(evidence)} evidências com proveniência registrada. "
                "As conclusões abaixo descrevem hipóteses e anomalias consistentes, não culpa."
            ),
            "",
            "## Por que este caso importa",
            "",
            *case_importance_lines,
            "",
            "## Por que o caso foi elevado",
            "",
            *why_elevated_lines,
            "",
            "## Sinais prioritários",
            "",
            *top_signal_lines,
            "",
            "## Contrapartes e organizações centrais",
            "",
            *organization_lines,
            "",
            "## Alertas cross-project relevantes",
            "",
            *alert_lines,
            "",
            "## Linha do tempo investigativa",
            "",
            *case_timeline_lines,
            "",
            "## Identidade e histórico confirmado",
            "",
            *identity_timeline_lines,
            "",
            "## Matriz de evidências",
            "",
            *matrix_rows,
            "",
            "## Hipóteses do caso e lacunas",
            "",
            *hypothesis_lines,
            "- Lacunas probatórias devem ser fechadas com documentos primários, cronologia complementar e revisão humana.",
            "",
            "## Próximos passos oficiais",
            "",
            *official_next_steps,
            "",
            "## Contestação",
            "",
            *contestation_lines,
            "",
            "## Próximas ações",
            "",
            *next_actions,
        ]
    )
    frontmatter = {
        "id": note_id("dossier", project.frontmatter["project_slug"]),
        "type": "dossier",
        "title": f"Dossiê {project.frontmatter['title']}",
        "status": "draft",
        "source_class": "derived_workspace",
        "source_refs": [],
        "related_ids": sorted({entity.frontmatter["id"] for entity in entities} | {alert.frontmatter["id"] for alert in alerts}),
        "project_ids": [project.frontmatter["project_slug"]],
        "confidence": 0.6,
        "updated_at": utc_now(),
        "priority": priority,
        "metadata": {
            "render_version": DOSSIER_RENDER_VERSION,
            "headline_signal": headline_signal,
            "headline_counterparty": headline_counterparty,
            "headline_alert": headline_alert,
            "next_official_step": headline_official_step,
        },
    }
    write_note(path, frontmatter, body)
    refresh_cache(root)
    return path


def validate_workspace(root: Path, publish_mode: bool = False) -> list[str]:
    if publish_mode:
        return validate_publish_safety(root)
    ensure_workspace(root)
    errors: list[str] = []
    note_index = build_note_index(root)
    required = {"id", "type", "title", "status", "source_class", "source_refs", "related_ids", "project_ids", "confidence", "updated_at"}
    for path in list_markdown_notes(root):
        try:
            note = read_note(path)
        except Exception as exc:
            errors.append(msg("validacao_falha_parse", path=path, error=exc))
            continue
        missing = sorted(required - set(note.frontmatter))
        if missing:
            errors.append(msg("validacao_frontmatter_obrigatorio", path=path, missing=missing))
        if note.frontmatter.get("type") == "evidence":
            source_class = note.frontmatter.get("source_class")
            if source_class not in SOURCE_CLASSES:
                errors.append(msg("validacao_source_class_invalida", path=path, source_class=source_class))
            evidence_role = note.frontmatter.get("evidence_role", "")
            if evidence_role and evidence_role not in EVIDENCE_ROLES:
                errors.append(msg("validacao_evidence_role_invalido", path=path, evidence_role=evidence_role))
            evidence_layer = note.frontmatter.get("evidence_layer", "")
            if evidence_layer and evidence_layer not in EVIDENCE_LAYERS:
                errors.append(f"{path}: camada de evidência inválida: {evidence_layer}")
            identity_resolution_status = note.frontmatter.get("identity_resolution_status", "")
            if identity_resolution_status and identity_resolution_status not in IDENTITY_RESOLUTION_STATUSES:
                errors.append(f"{path}: status de resolução de identidade inválido: {identity_resolution_status}")
            if not note.frontmatter.get("source_refs"):
                errors.append(msg("validacao_source_refs_ausente", path=path))
            if not note.frontmatter.get("claim"):
                errors.append(msg("validacao_claim_ausente", path=path))
        if note.frontmatter.get("type") == "target":
            canonical_id = note.frontmatter.get("canonical_id")
            if canonical_id not in note_index:
                errors.append(msg("validacao_canonical_id_inexistente", path=path, canonical_id=canonical_id))
        for related_id in note.frontmatter.get("related_ids", []):
            if related_id not in note_index:
                errors.append(msg("validacao_related_id_inexistente", path=path, related_id=related_id))
    refresh_cache(root)
    return errors
