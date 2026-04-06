from __future__ import annotations

import json
import os
import time
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Callable

from .core import (
    add_target,
    advance_project,
    build_note_index,
    create_project,
    diagnose_project,
    ensure_workspace,
    format_progress_event,
    get_project,
    portfolio_elevated_from_metrics,
    project_case_metrics,
    project_case_metrics_batch,
    project_targets,
    refresh_cache,
    refresh_project_materialized_views,
    refresh_project_materialized_views_batch,
    slugify,
    strong_context_reasons_from_metrics,
    update_project_case_metrics,
    update_project_case_metrics_batch,
    update_target,
)
from .frontmatter import read_note, write_note
from .messages import msg
from .models import (
    DEFAULT_CONTEXTUAL_PLUGINS,
    DEFAULT_PORTFOLIO_BASELINE_PLUGINS,
    DEFAULT_PORTFOLIO_DEEP_PLUGINS,
    FEDERAL_CAMARA_PLUGINS,
    Note,
    RosterMember,
    WorkerCheckpoint,
)
from .rosters import list_roster_sources


ProgressFn = Callable[[str], None]


def _utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def _emit(progress: ProgressFn | None, message: str) -> None:
    if progress is not None:
        progress(message)


def _note_id(prefix: str, *parts: str) -> str:
    value = "-".join(slugify(part) for part in parts if str(part).strip())
    return f"{prefix}-{value}".strip("-")


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


def _note_title(note: Note) -> str:
    return str(note.frontmatter.get("title") or note.frontmatter.get("name") or note.frontmatter.get("id") or note.path.stem)


def _note_link(from_path: Path, note: Note, label: str | None = None) -> str:
    relative = Path(os.path.relpath(note.path, start=from_path.parent)).as_posix()
    return f"[{label or _note_title(note)}]({relative})"


def _alert_relevance_value(frontmatter_or_note: Note | dict[str, Any] | str) -> str:
    if isinstance(frontmatter_or_note, Note):
        metadata = dict(frontmatter_or_note.frontmatter.get("metadata", {}))
        value = str(metadata.get("alert_relevance", "high_signal") or "high_signal").strip()
    elif isinstance(frontmatter_or_note, dict):
        metadata = dict(frontmatter_or_note.get("metadata", {}))
        value = str(metadata.get("alert_relevance", "high_signal") or "high_signal").strip()
    else:
        value = str(frontmatter_or_note or "high_signal").strip()
    return value if value in {"high_signal", "contextual", "trivial_shared_source"} else "high_signal"


def _alert_relevance_rank(frontmatter_or_note: Note | dict[str, Any] | str) -> int:
    return {
        "high_signal": 3,
        "contextual": 2,
        "trivial_shared_source": 0,
    }.get(_alert_relevance_value(frontmatter_or_note), 0)


def _alert_is_visible(frontmatter_or_note: Note | dict[str, Any] | str) -> bool:
    return _alert_relevance_rank(frontmatter_or_note) > 0


def _classify_alert_relevance(prefix: str, key: str, entity_note: Note | None) -> str:
    normalized_key = key.lower()
    if prefix == "shared-reference":
        if any(token in normalized_key for token in ("cdn.tse.jus.br", "consulta_cand", "dadosabertos.tse.jus.br")):
            return "trivial_shared_source"
        return "contextual"
    if entity_note is None:
        return "high_signal"
    entity_id = str(entity_note.frontmatter.get("id", "")).lower()
    entity_title = _note_title(entity_note).lower()
    if "-party-" in entity_id:
        return "contextual"
    if any(
        marker in entity_title
        for marker in (
            "câmara dos deputados",
            "assembleia legislativa",
            "tribunal",
            "prefeitura",
            "governo do estado",
        )
    ):
        return "trivial_shared_source"
    return "high_signal"


def _merge_metadata(existing: dict[str, Any], incoming: dict[str, Any]) -> dict[str, Any]:
    merged = dict(existing)
    for key, value in incoming.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _merge_metadata(merged[key], value)
        elif isinstance(value, list) and isinstance(merged.get(key), list):
            merged[key] = _dedupe_strings([*merged[key], *[str(item) for item in value]])
        elif value not in (None, "", [], {}):
            merged[key] = value
    return merged


TJ_ALIAS_BY_UF = {
    "AC": "api_publica_tjac",
    "AL": "api_publica_tjal",
    "AP": "api_publica_tjap",
    "AM": "api_publica_tjam",
    "BA": "api_publica_tjba",
    "CE": "api_publica_tjce",
    "DF": "api_publica_tjdft",
    "ES": "api_publica_tjes",
    "GO": "api_publica_tjgo",
    "MA": "api_publica_tjma",
    "MT": "api_publica_tjmt",
    "MS": "api_publica_tjms",
    "MG": "api_publica_tjmg",
    "PA": "api_publica_tjpa",
    "PB": "api_publica_tjpb",
    "PR": "api_publica_tjpr",
    "PE": "api_publica_tjpe",
    "PI": "api_publica_tjpi",
    "RJ": "api_publica_tjrj",
    "RN": "api_publica_tjrn",
    "RS": "api_publica_tjrs",
    "RO": "api_publica_tjro",
    "RR": "api_publica_tjrr",
    "SC": "api_publica_tjsc",
    "SP": "api_publica_tjsp",
    "SE": "api_publica_tjse",
    "TO": "api_publica_tjto",
}


def _portfolio_root(root: Path, slug: str) -> Path:
    return root / "portfolios" / slugify(slug)


def _portfolio_paths(root: Path, slug: str) -> dict[str, Path]:
    base = _portfolio_root(root, slug)
    return {
        "root": base,
        "portfolio": base / "portfolio.md",
        "members": base / "members",
        "runs": base / "runs",
        "alerts": base / "alerts",
        "leaderboard": base / "leaderboard.md",
        "checkpoint": base / "runs" / "checkpoint.json",
        "repair_checkpoint": base / "runs" / "repair-checkpoint.json",
    }


def _member_path(root: Path, portfolio_slug: str, project_slug: str) -> Path:
    return _portfolio_paths(root, portfolio_slug)["members"] / f"{slugify(project_slug)}.md"


def _alert_path(root: Path, portfolio_slug: str, alert_key: str) -> Path:
    return _portfolio_paths(root, portfolio_slug)["alerts"] / f"{slugify(alert_key)}.md"


def _roster_ref(member: RosterMember) -> dict[str, Any]:
    return {
        "plugin": member.source_plugin,
        "source_name": member.metadata.get("assembly_name", member.source_plugin),
        "record_id": member.source_member_id,
        "url": member.roster_url,
        "collected_at": _utc_now(),
    }


def _project_title(member: RosterMember) -> str:
    office = "Deputado Federal" if member.scope == "federal" else "Deputado Estadual"
    return f"{office} {member.parliamentary_name} ({member.uf})"


def _project_slug_for_member(member: RosterMember) -> str:
    level = "federal" if member.scope == "federal" else "estadual"
    return slugify(f"dep-{level}-{member.uf}-{member.parliamentary_name}-{member.source_member_id}")


def _target_identifier(member: RosterMember) -> str:
    return f"deputy:{member.scope}:{member.source_plugin}:{member.source_member_id}"


def _default_tribunal_aliases(uf: str) -> list[str]:
    aliases = ["api_publica_tse", "api_publica_stj"]
    if uf in TJ_ALIAS_BY_UF:
        aliases.append(TJ_ALIAS_BY_UF[uf])
    return _dedupe_strings(aliases)


def _default_project_plugins(member: RosterMember) -> list[str]:
    return _expected_project_plugins_for_scope(member.scope)


def _default_baseline_plugins(member: RosterMember) -> list[str]:
    return _expected_baseline_plugins_for_scope(member.scope)


def _expected_project_plugins_for_scope(scope: str) -> list[str]:
    plugins = list(DEFAULT_PORTFOLIO_DEEP_PLUGINS)
    if str(scope).strip().lower() == "federal":
        plugins.extend(FEDERAL_CAMARA_PLUGINS)
    return _dedupe_strings(plugins)


def _expected_baseline_plugins_for_scope(scope: str) -> list[str]:
    if str(scope).strip().lower() == "federal":
        return ["camara-profile", "camara-expenses"]
    return list(DEFAULT_PORTFOLIO_BASELINE_PLUGINS)


def _default_roster_max_age_hours() -> int:
    return 24


def _portfolio_roster_max_age_hours(portfolio: Note, override: int | None = None) -> int:
    if override is not None:
        return max(1, int(override))
    raw = portfolio.frontmatter.get("roster_max_age_hours", _default_roster_max_age_hours())
    try:
        return max(1, int(raw))
    except (TypeError, ValueError):
        return _default_roster_max_age_hours()


def _load_checkpoint(root: Path, portfolio_slug: str) -> dict[str, Any]:
    path = _portfolio_paths(root, portfolio_slug)["checkpoint"]
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def _latest_run_payload(root: Path, portfolio_slug: str, prefix: str) -> dict[str, Any]:
    run_root = _portfolio_paths(root, portfolio_slug)["runs"]
    candidates = sorted(run_root.glob(f"{prefix}-*.json"))
    if not candidates:
        return {}
    try:
        payload = json.loads(candidates[-1].read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    payload["_path"] = str(candidates[-1])
    return payload


def _latest_roster_sync_payload(root: Path, portfolio_slug: str) -> dict[str, Any]:
    return _latest_run_payload(root, portfolio_slug, "roster-sync")


def _latest_tick_event(root: Path, portfolio_slug: str, event_prefix: str) -> dict[str, Any]:
    return _latest_run_payload(root, portfolio_slug, event_prefix)


def _portfolio_last_roster_sync_at(portfolio: Note, root: Path | None = None) -> datetime | None:
    if root is not None:
        payload = _latest_successful_roster_sync(root, portfolio.frontmatter["portfolio_slug"])
        successful = _parse_timestamp(str(payload.get("completed_at", "")).strip())
        if successful is not None:
            return successful
    explicit = _parse_timestamp(str(portfolio.frontmatter.get("last_roster_sync_at", "")).strip())
    if explicit is not None:
        return explicit
    return None


def _should_sync_roster(root: Path, portfolio: Note, mode: str, roster_max_age_hours: int | None = None) -> bool:
    normalized = str(mode or "auto").strip().lower()
    if normalized == "force":
        return True
    if normalized == "skip":
        return False
    last_sync = _portfolio_last_roster_sync_at(portfolio, root)
    if last_sync is None:
        return True
    max_age = timedelta(hours=_portfolio_roster_max_age_hours(portfolio, roster_max_age_hours))
    return datetime.now(UTC) - last_sync >= max_age


def _looks_like_person_title(title: str) -> bool:
    words = [word for word in str(title or "").strip().split() if word]
    if len(words) < 2:
        return False
    lowered = " ".join(words).casefold()
    rejected = (
        "tribunal",
        "tv",
        "sess",
        "acervo",
        "foto",
        "concurso",
        "ver mais",
        "justi",
        "contas",
        "aleac",
        "galeria",
        "not",
        "memorial",
        "hist",
    )
    return not any(token in lowered for token in rejected)


def _project_run_index(root: Path, project_slug: str) -> dict[str, dict[str, Any]]:
    run_root = root / "projects" / slugify(project_slug) / "runs"
    latest: dict[str, dict[str, Any]] = {}
    for path in sorted(run_root.glob("sync-*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            continue
        plugin_name = str(payload.get("plugin", "")).strip() or path.stem.removeprefix("sync-")
        payload["_path"] = str(path)
        latest[plugin_name] = payload
    return latest


def _member_metric_counts(member: Note, project: Note | None = None) -> dict[str, int]:
    project_metadata = dict(project.frontmatter.get("metadata", {})) if project is not None else {}
    member_frontmatter = member.frontmatter
    return {
        "lead_score": int(project_metadata.get("lead_score", member_frontmatter.get("lead_score", 0)) or 0),
        "official_evidence_count": int(project_metadata.get("official_evidence_count", member_frontmatter.get("official_evidence_count", 0)) or 0),
        "official_signal_count": int(project_metadata.get("official_signal_count", member_frontmatter.get("official_signal_count", 0)) or 0),
        "official_identity_count": int(project_metadata.get("official_identity_count", member_frontmatter.get("official_identity_count", 0)) or 0),
        "official_signal_source_count": int(project_metadata.get("official_signal_source_count", member_frontmatter.get("official_signal_source_count", 0)) or 0),
        "contextual_evidence_count": int(project_metadata.get("contextual_evidence_count", member_frontmatter.get("contextual_evidence_count", 0)) or 0),
        "contextual_domain_count": int(project_metadata.get("contextual_domain_count", member_frontmatter.get("contextual_domain_count", 0)) or 0),
        "proposed_official_link_count": int(project_metadata.get("proposed_official_link_count", member_frontmatter.get("proposed_official_link_count", 0)) or 0),
        "organization_count": int(project_metadata.get("organization_count", member_frontmatter.get("organization_count", 0)) or 0),
        "hypothesis_count": int(project_metadata.get("hypothesis_count", member_frontmatter.get("hypothesis_count", 0)) or 0),
        "law_count": int(project_metadata.get("law_count", member_frontmatter.get("law_count", 0)) or 0),
        "crossref_alert_count": int(member_frontmatter.get("crossref_alert_count", 0) or 0),
        "needs_rebuild": bool(project_metadata.get("needs_rebuild", member_frontmatter.get("needs_rebuild", False))),
    }


def _normalized_scope(scope: str | None) -> str:
    value = str(scope or "all").strip().lower()
    return value if value in {"federal", "state", "all"} else "all"


def _normalized_only_status(value: str | None) -> str:
    normalized = str(value or "all").strip().lower()
    return normalized if normalized in {"active_validated", "all"} else "all"


def _member_matches_scope(member: Note, scope: str) -> bool:
    normalized = _normalized_scope(scope)
    if normalized == "all":
        return True
    return str(member.frontmatter.get("scope", "")).strip().lower() == normalized


def _member_matches_only_status(member: Note, only_status: str) -> bool:
    normalized = _normalized_only_status(only_status)
    if normalized == "all":
        return True
    return member.frontmatter.get("status") == "active_roster" and bool(member.frontmatter.get("roster_validated", False))


def _member_is_queue_eligible(member: Note, *, scope: str = "all", only_status: str = "all") -> bool:
    if not _member_matches_scope(member, scope):
        return False
    if not _member_matches_only_status(member, only_status):
        return False
    return str(member.frontmatter.get("status", "")).strip() not in {"inactive_roster", "failed_roster", "provisional_roster"}


def _active_member_rows(root: Path, portfolio_slug: str, *, scope: str = "all", only_status: str = "active_validated") -> list[tuple[Note, Note]]:
    rows: list[tuple[Note, Note]] = []
    for member in _member_notes(root, portfolio_slug):
        if not _member_is_queue_eligible(member, scope=scope, only_status=only_status):
            continue
        project_slug = str(member.frontmatter.get("project_slug", "")).strip()
        if not project_slug:
            continue
        try:
            project = get_project(root, project_slug)
        except FileNotFoundError:
            continue
        rows.append((member, project))
    return rows


def _repair_checkpoint_payload(root: Path, portfolio_slug: str) -> dict[str, Any]:
    path = _portfolio_paths(root, portfolio_slug)["repair_checkpoint"]
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def _member_body(note: Note) -> str:
    frontmatter = note.frontmatter
    root = note.path.parents[3]
    project_slug = str(frontmatter.get("project_slug", "")).strip()
    project_path = root / "projects" / project_slug / "project.md"
    project_label = f"[{project_slug}]({Path(os.path.relpath(project_path, start=note.path.parent)).as_posix()})" if project_slug and project_path.exists() else f"`{project_slug}`"
    portfolio_elevated = "Sim" if frontmatter.get("portfolio_elevated", False) else "Não"
    strong_context = frontmatter.get("strong_context_reasons", [])
    roster_url = str(frontmatter.get("roster_url", "")).strip()
    roster_ref = f"[registro oficial]({roster_url})" if roster_url else "registro oficial sem URL consolidada"
    lines = [
        f"# {frontmatter.get('title', note.path.stem)}",
        "",
        "## Situação do caso",
        "",
        f"- Projeto: {project_label}",
        f"- Fila atual: `{frontmatter.get('queue_state', '')}`",
        f"- Status do membro: `{frontmatter.get('status', '')}`",
        "",
        "## Perfil público",
        "",
        f"- Fonte de roster: `{frontmatter.get('roster_plugin', '')}`",
        f"- Referência pública: {roster_ref}",
        f"- UF: `{frontmatter.get('uf', '')}`",
        f"- Partido: `{frontmatter.get('party', '')}`",
        f"- Validado: `{frontmatter.get('roster_validated', False)}`",
        f"- Confiança do roster: `{frontmatter.get('roster_confidence', 0.0)}`",
        f"- Tipo de fonte: `{frontmatter.get('roster_source_kind', '')}`",
        "",
        "## Sinais do caso",
        "",
        f"- Pontuação de indícios: `{frontmatter.get('lead_score', 0)}`",
        f"- Caso elevado no portfolio: `{portfolio_elevated}`",
        f"- Evidências oficiais: `{frontmatter.get('official_evidence_count', 0)}`",
        f"- Sinais oficiais: `{frontmatter.get('official_signal_count', 0)}`",
        f"- Domínios contextuais: `{frontmatter.get('contextual_domain_count', 0)}`",
        f"- Links oficiais sugeridos: `{frontmatter.get('proposed_official_link_count', 0)}`",
        f"- Organizações materializadas: `{frontmatter.get('organization_count', 0)}`",
        f"- Hipóteses materializadas: `{frontmatter.get('hypothesis_count', 0)}`",
        f"- Leis/padrões materializados: `{frontmatter.get('law_count', 0)}`",
        f"- Alertas cross-project: `{frontmatter.get('crossref_alert_count', 0)}`",
        f"- Precisa de rebuild narrativo: `{'Sim' if frontmatter.get('needs_rebuild', False) else 'Não'}`",
        f"- Contexto forte: `{', '.join(strong_context) if strong_context else 'não caracterizado ainda'}`",
        "",
        "## Agendamento",
        "",
        f"- Último tick: `{frontmatter.get('last_tick_at', '')}`",
        f"- Próximo tick: `{frontmatter.get('next_tick_after', '')}`",
    ]
    return "\n".join(lines)


def _portfolio_body(note: Note) -> str:
    frontmatter = note.frontmatter
    lines = [
        f"# {frontmatter.get('title', 'Portfolio')}",
        "",
        "## Configuração",
        "",
        f"- População: `{frontmatter.get('population', '')}`",
        f"- Escopo: `{', '.join(frontmatter.get('scope', []))}`",
        f"- Estratégia de coleta: `{frontmatter.get('crawl_style', '')}`",
        f"- Plugins de baseline: `{', '.join(frontmatter.get('baseline_plugin_names', []))}`",
        f"- Plugins de aprofundamento: `{', '.join(frontmatter.get('plugin_names', []))}`",
        f"- Última sincronização de roster: `{frontmatter.get('last_roster_sync_at', '')}`",
        f"- Cadência máxima do roster: `{frontmatter.get('roster_max_age_hours', 24)}` hora(s)",
        "",
        "## Cobertura",
        "",
        f"- Membros rastreados: `{frontmatter.get('member_count', 0)}`",
        f"- Ativos validados: `{frontmatter.get('validated_active_count', 0)}`",
        f"- Fontes com falha de roster: `{frontmatter.get('roster_failure_count', 0)}`",
        f"- Estados/fontes em falha: `{frontmatter.get('failed_source_count', 0)}`",
        f"- Baseline com sinal: `{frontmatter.get('baseline_with_signals_count', 0)}`",
        f"- Deep concluído: `{frontmatter.get('deep_completed_count', 0)}`",
        f"- Organizações materializadas: `{frontmatter.get('organization_count', 0)}`",
        f"- Hipóteses materializadas: `{frontmatter.get('hypothesis_count', 0)}`",
        f"- Casos elevados: `{frontmatter.get('elevated_count', 0)}`",
        f"- Watchlist: `{frontmatter.get('watchlist_count', 0)}`",
    ]
    return "\n".join(lines)


def _leaderboard_body(
    portfolio: Note,
    coverage: dict[str, Any],
    watchlist: list[dict[str, Any]],
    ready_for_review: list[dict[str, Any]],
    deep_pending: list[dict[str, Any]],
    shallow_elevated: list[dict[str, Any]],
) -> str:
    watch_lines = [
        f"- `{item['lead_score']}` [{item['title']}]({item['member_link']}): {_project_link_text(item)}"
        for item in watchlist[:25]
    ] or ["- Nenhum caso em watchlist no momento."]
    ready_lines = [
        f"- `{item['priority']}` [{item['title']}]({item['member_link']}): {_project_link_text(item)}"
        for item in ready_for_review[:25]
    ] or ["- Nenhum caso pronto para revisão humana no momento."]
    deep_pending_lines = [
        f"- `{item['priority']}` [{item['title']}]({item['member_link']}): {_project_link_text(item)}"
        for item in deep_pending[:25]
    ] or ["- Nenhum caso com aprofundamento pendente no momento."]
    shallow_lines = [
        f"- `{item['priority']}` [{item['title']}]({item['member_link']}): {_project_link_text(item)}"
        for item in shallow_elevated[:25]
    ] or ["- Nenhum caso elevado porém raso no momento."]
    return "\n".join(
        [
            f"# Leaderboard do portfolio {portfolio.frontmatter.get('title', '')}",
            "",
            "## Cobertura do portfolio",
            "",
            f"- Ativos no roster: `{coverage['active']}`",
            f"- Ativos validados: `{coverage['validated_active']}`",
            f"- Inativos no roster: `{coverage['inactive']}`",
            f"- Fontes com falha: `{coverage['failed_sources']}`",
            f"- Baseline concluído: `{coverage['baseline_done']}`",
            f"- Baseline com sinal: `{coverage['baseline_with_signals']}`",
            f"- Deep concluído: `{coverage['deep_done']}`",
            f"- Hipóteses materializadas: `{coverage['hypotheses']}`",
            f"- Organizações materializadas: `{coverage['organizations']}`",
            f"- Alertas cross-project: `{coverage['alerts']}`",
            "",
            "## Watchlist de baseline",
            "",
            *watch_lines,
            "",
            "## Prontos para revisão humana",
            "",
            *ready_lines,
            "",
            "## Deep pendente",
            "",
            *deep_pending_lines,
            "",
            "## Elevados porém ainda rasos",
            "",
            *shallow_lines,
        ]
    )


def _project_link_text(item: dict[str, Any]) -> str:
    parts = [f"project `{item['project_slug']}`"]
    if item.get("headline_signal"):
        parts.append(f"sinal principal: {item['headline_signal']}")
    if item.get("headline_counterparty"):
        parts.append(f"contraparte: {item['headline_counterparty']}")
    if item.get("headline_alert"):
        parts.append(f"alerta forte: {item['headline_alert']}")
    if item.get("next_official_step"):
        parts.append(f"próximo passo oficial: {item['next_official_step']}")
    if len(parts) == 1:
        if item.get("official_signal_count") is not None:
            parts.append(f"{item['official_signal_count']} sinais oficiais")
        if item.get("hypothesis_count") is not None:
            parts.append(f"{item['hypothesis_count']} hipóteses")
    return ", ".join(parts)


def create_portfolio(
    root: Path,
    slug: str,
    population: str = "deputados-current",
    scope: list[str] | None = None,
) -> Path:
    ensure_workspace(root)
    portfolio_slug = slugify(slug)
    scope = [item.strip().lower() for item in (scope or ["federal", "state"]) if item.strip()]
    paths = _portfolio_paths(root, portfolio_slug)
    for directory in (paths["members"], paths["runs"], paths["alerts"]):
        directory.mkdir(parents=True, exist_ok=True)
    frontmatter = {
        "id": _note_id("portfolio", portfolio_slug),
        "type": "portfolio",
        "title": slug.replace("-", " ").title(),
        "status": "active",
        "source_class": "derived_workspace",
        "source_refs": [],
        "related_ids": [],
        "project_ids": [],
        "confidence": 1.0,
        "updated_at": _utc_now(),
        "portfolio_slug": portfolio_slug,
        "population": population,
        "scope": scope,
        "crawl_style": "shallow_then_deepen",
        "baseline_plugin_names": list(DEFAULT_PORTFOLIO_BASELINE_PLUGINS),
        "plugin_names": list(DEFAULT_PORTFOLIO_DEEP_PLUGINS),
        "contextual_plugin_names": list(DEFAULT_CONTEXTUAL_PLUGINS),
        "last_roster_sync_at": "",
        "roster_max_age_hours": _default_roster_max_age_hours(),
        "member_count": 0,
        "validated_active_count": 0,
        "failed_source_count": 0,
        "roster_failure_count": 0,
        "baseline_with_signals_count": 0,
        "deep_completed_count": 0,
        "organization_count": 0,
        "hypothesis_count": 0,
        "watchlist_count": 0,
        "elevated_count": 0,
    }
    write_note(paths["portfolio"], frontmatter, _portfolio_body(Note(paths["portfolio"], frontmatter, "")))
    if not paths["leaderboard"].exists():
        write_note(
            paths["leaderboard"],
            {
                "id": _note_id("portfolio-leaderboard", portfolio_slug),
                "type": "portfolio_leaderboard",
                "title": f"Leaderboard {frontmatter['title']}",
                "status": "derived",
                "source_class": "derived_workspace",
                "source_refs": [],
                "related_ids": [],
                "project_ids": [],
                "confidence": 1.0,
                "updated_at": _utc_now(),
            },
            "# Leaderboard\n",
        )
    refresh_cache(root)
    return paths["portfolio"]


def get_portfolio(root: Path, slug: str) -> Note:
    ensure_workspace(root)
    path = _portfolio_paths(root, slug)["portfolio"]
    if not path.exists():
        raise FileNotFoundError(msg("portfolio_inexistente", slug=slug))
    return read_note(path)


def _member_notes(root: Path, portfolio_slug: str) -> list[Note]:
    member_root = _portfolio_paths(root, portfolio_slug)["members"]
    return [read_note(path) for path in sorted(member_root.glob("*.md"))]


def _member_index(root: Path, portfolio_slug: str) -> dict[str, Note]:
    return {note.frontmatter.get("project_slug", note.path.stem): note for note in _member_notes(root, portfolio_slug)}


def _project_note_id(root: Path, project_slug: str) -> str:
    return get_project(root, project_slug).frontmatter["id"]


def _ensure_project_for_member(root: Path, portfolio_slug: str, member: RosterMember) -> tuple[str, str]:
    project_slug = _project_slug_for_member(member)
    project_title = _project_title(member)
    metadata = {
        "portfolio_slug": portfolio_slug,
        "legislature_level": "federal" if member.scope == "federal" else "state",
        "uf": member.uf,
        "party": member.party,
        "roster_plugin": member.source_plugin,
        "roster_member_id": member.source_member_id,
        "roster_url": member.roster_url,
        "assembly_name": member.metadata.get("assembly_name", ""),
        "lead_score": 0,
        "official_evidence_count": 0,
        "official_signal_count": 0,
        "official_identity_count": 0,
        "official_signal_source_count": 0,
        "contextual_evidence_count": 0,
        "contextual_domain_count": 0,
        "proposed_official_link_count": 0,
        "organization_count": 0,
        "hypothesis_count": 0,
        "law_count": 0,
        "crossref_alert_count": 0,
        "strong_context_reasons": [],
        "portfolio_elevated": False,
        "needs_rebuild": False,
    }
    try:
        project = get_project(root, project_slug)
    except FileNotFoundError:
        create_project(root, project_slug, project_title, metadata)
        project = get_project(root, project_slug)
    project_frontmatter = dict(project.frontmatter)
    project_frontmatter["title"] = project_title
    project_frontmatter["plugin_names"] = _default_project_plugins(member)
    project_frontmatter["baseline_plugin_names"] = _default_baseline_plugins(member)
    project_frontmatter["contextual_plugin_names"] = list(DEFAULT_CONTEXTUAL_PLUGINS)
    project_frontmatter["metadata"] = _merge_metadata(project_frontmatter.get("metadata", {}), metadata)
    project_frontmatter["updated_at"] = _utc_now()
    write_note(project.path, project_frontmatter, project.body)

    identifier = _target_identifier(member)
    target_metadata = {
        "office": "Deputado Federal" if member.scope == "federal" else "Deputado Estadual",
        "legislature_level": "federal" if member.scope == "federal" else "state",
        "uf": member.uf,
        "party": member.party,
        "current_mandate": True,
        "portfolio_slug": portfolio_slug,
        "roster_plugin": member.source_plugin,
        "roster_member_id": member.source_member_id,
        "roster_url": member.roster_url,
        "assembly_name": member.metadata.get("assembly_name", ""),
        "aliases": list(member.aliases),
        "tribunal_aliases": _default_tribunal_aliases(member.uf),
        **member.metadata,
    }
    if member.scope == "federal" and member.metadata.get("camara_id"):
        target_metadata["camara_id"] = member.metadata["camara_id"]
    targets = project_targets(root, project_slug)
    if not targets:
        add_target(
            root,
            project_slug,
            "person",
            identifier,
            member.parliamentary_name,
            aliases=_dedupe_strings([member.full_name, *member.aliases]),
            metadata=target_metadata,
        )
    else:
        update_target(
            root,
            project_slug,
            targets[0].frontmatter["id"],
            title=member.parliamentary_name,
            aliases=_dedupe_strings([member.full_name, *member.aliases]),
            metadata=target_metadata,
        )
    refresh_project_materialized_views(root, project_slug)
    target = project_targets(root, project_slug)[0]
    return project_slug, target.frontmatter["canonical_id"]


def _write_member_note(root: Path, portfolio_slug: str, member: RosterMember, project_slug: str, canonical_id: str, existing: Note | None = None) -> Path:
    path = _member_path(root, portfolio_slug, project_slug)
    existing_frontmatter = dict(existing.frontmatter) if existing is not None else {}
    if member.status == "failed_roster":
        status = "failed_roster"
        queue_state = "roster_failed"
    elif member.status == "provisional_roster":
        status = "provisional_roster"
        queue_state = "provisional_roster"
    elif member.status == "inactive_roster":
        status = "inactive_roster"
        queue_state = "inactive_roster"
    else:
        status = "active_roster"
        queue_state = existing_frontmatter.get("queue_state", "pending_baseline")
    frontmatter = {
        "id": existing_frontmatter.get("id", _note_id("portfolio-member", portfolio_slug, project_slug)),
        "type": "portfolio_member",
        "title": member.parliamentary_name,
        "status": status,
        "source_class": "derived_workspace",
        "source_refs": [_roster_ref(member)],
        "roster_refs": [_roster_ref(member)],
        "related_ids": _dedupe_strings([_project_note_id(root, project_slug), canonical_id]),
        "project_ids": [project_slug],
        "confidence": 0.9,
        "updated_at": _utc_now(),
        "portfolio_slug": portfolio_slug,
        "project_slug": project_slug,
        "queue_state": queue_state,
        "last_tick_at": existing_frontmatter.get("last_tick_at", ""),
        "next_tick_after": existing_frontmatter.get("next_tick_after", ""),
        "roster_plugin": member.source_plugin,
        "roster_member_id": member.source_member_id,
        "roster_url": member.roster_url,
        "roster_confidence": member.roster_confidence,
        "roster_validated": member.roster_validated,
        "roster_source_kind": member.roster_source_kind,
        "scope": member.scope,
        "uf": member.uf,
        "party": member.party,
        "lead_score": existing_frontmatter.get("lead_score", 0),
        "official_evidence_count": existing_frontmatter.get("official_evidence_count", 0),
        "official_signal_count": existing_frontmatter.get("official_signal_count", 0),
        "official_identity_count": existing_frontmatter.get("official_identity_count", 0),
        "official_signal_source_count": existing_frontmatter.get("official_signal_source_count", 0),
        "contextual_evidence_count": existing_frontmatter.get("contextual_evidence_count", 0),
        "contextual_domain_count": existing_frontmatter.get("contextual_domain_count", 0),
        "proposed_official_link_count": existing_frontmatter.get("proposed_official_link_count", 0),
        "organization_count": existing_frontmatter.get("organization_count", 0),
        "hypothesis_count": existing_frontmatter.get("hypothesis_count", 0),
        "law_count": existing_frontmatter.get("law_count", 0),
        "crossref_alert_count": existing_frontmatter.get("crossref_alert_count", 0),
        "strong_context_reasons": list(existing_frontmatter.get("strong_context_reasons", [])),
        "portfolio_elevated": bool(existing_frontmatter.get("portfolio_elevated", False)),
        "needs_rebuild": bool(existing_frontmatter.get("needs_rebuild", False)),
        "baseline_completed_at": existing_frontmatter.get("baseline_completed_at", ""),
        "deep_completed_at": existing_frontmatter.get("deep_completed_at", ""),
        "failure_count": existing_frontmatter.get("failure_count", 0),
        "metadata": _merge_metadata(existing_frontmatter.get("metadata", {}), member.metadata),
        "aliases": _dedupe_strings([member.full_name, *member.aliases]),
    }
    write_note(path, frontmatter, _member_body(Note(path, frontmatter, "")))
    return path


def _update_member_status(note: Note, *, status: str, queue_state: str, roster_error: str = "") -> None:
    frontmatter = dict(note.frontmatter)
    frontmatter["status"] = status
    frontmatter["queue_state"] = queue_state
    metadata = dict(frontmatter.get("metadata", {}))
    if roster_error:
        metadata["roster_error"] = roster_error
    elif "roster_error" in metadata:
        metadata.pop("roster_error", None)
    frontmatter["metadata"] = metadata
    frontmatter["updated_at"] = _utc_now()
    write_note(note.path, frontmatter, _member_body(Note(note.path, frontmatter, "")))


def sync_portfolio_roster(root: Path, slug: str, progress: ProgressFn | None = None) -> Path:
    portfolio = get_portfolio(root, slug)
    portfolio_slug = portfolio.frontmatter["portfolio_slug"]
    scopes = list(portfolio.frontmatter.get("scope", ["federal", "state"]))
    sources = list_roster_sources(scopes)
    existing_members = _member_index(root, portfolio_slug)
    seen_projects: set[str] = set()
    runs: list[dict[str, Any]] = []
    inactive: list[str] = []
    created_projects = 0
    updated_projects = 0
    source_failures: list[dict[str, Any]] = []
    for source in sources:
        _emit(progress, f"[roster] coletando {source.name}")
        result = source.list_current_members()
        source_seen: set[str] = set()
        existing_source_members = {
            project_slug: note
            for project_slug, note in existing_members.items()
            if note.frontmatter.get("roster_plugin", "") == source.name
        }
        source_summary = {
            "plugin": result.plugin,
            "source_url": result.source_url,
            "member_count": len(result.members),
            "validated_member_count": sum(1 for member in result.members if member.roster_validated),
            "errors": list(result.errors),
            "projects": [],
        }
        if not result.members:
            failure = {
                "plugin": result.plugin,
                "source_url": result.source_url,
                "errors": list(result.errors),
            }
            source_failures.append(failure)
            for existing_member in existing_source_members.values():
                _update_member_status(
                    existing_member,
                    status="failed_roster",
                    queue_state="roster_failed",
                    roster_error="; ".join(result.errors) or "roster source failed validation",
                )
            runs.append({**result.to_dict(), **source_summary, "failed": True})
            continue
        for member in result.members:
            _emit(
                progress,
                f"[roster] sincronizando {member.parliamentary_name} ({member.scope}/{member.uf})",
            )
            project_slug, canonical_id = _ensure_project_for_member(root, portfolio_slug, member)
            existing_member = existing_members.get(project_slug)
            if existing_member is None:
                created_projects += 1
            else:
                updated_projects += 1
            _write_member_note(root, portfolio_slug, member, project_slug, canonical_id, existing=existing_member)
            seen_projects.add(project_slug)
            source_seen.add(project_slug)
            source_summary["projects"].append(project_slug)
        for project_slug, member_note in existing_source_members.items():
            if project_slug in source_seen:
                continue
            _update_member_status(member_note, status="inactive_roster", queue_state="inactive_roster")
            inactive.append(project_slug)
        runs.append({**result.to_dict(), **source_summary})
    current_members = _member_notes(root, portfolio_slug)
    portfolio_frontmatter = dict(portfolio.frontmatter)
    portfolio_frontmatter["member_count"] = len(current_members)
    portfolio_frontmatter["failed_source_count"] = len(source_failures)
    portfolio_frontmatter["roster_failure_count"] = len(source_failures)
    portfolio_frontmatter["validated_active_count"] = sum(
        1
        for note in current_members
        if note.frontmatter.get("status") == "active_roster" and note.frontmatter.get("roster_validated", False)
    )
    if created_projects > 0 or updated_projects > 0 or any(run.get("member_count", 0) > 0 for run in runs):
        portfolio_frontmatter["last_roster_sync_at"] = _utc_now()
    if "roster_max_age_hours" not in portfolio_frontmatter:
        portfolio_frontmatter["roster_max_age_hours"] = _default_roster_max_age_hours()
    portfolio_frontmatter["updated_at"] = _utc_now()
    write_note(portfolio.path, portfolio_frontmatter, _portfolio_body(Note(portfolio.path, portfolio_frontmatter, "")))
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    run_path = _portfolio_paths(root, portfolio_slug)["runs"] / f"roster-sync-{timestamp}.json"
    run_payload = {
        "portfolio": portfolio_slug,
        "completed_at": _utc_now(),
        "sources": runs,
        "created_projects": created_projects,
        "updated_projects": updated_projects,
        "inactive_projects": inactive,
        "source_failures": source_failures,
    }
    run_path.write_text(json.dumps(run_payload, indent=2, ensure_ascii=False), encoding="utf-8")
    _emit(
        progress,
        f"[roster] concluído: {created_projects} projeto(s) novo(s), {updated_projects} atualizado(s), {len(inactive)} inativado(s)",
    )
    refresh_cache(root)
    return run_path


def _write_repair_event(root: Path, portfolio_slug: str, repair_id: str, event_name: str, payload: dict[str, Any]) -> Path:
    path = _portfolio_paths(root, portfolio_slug)["runs"] / f"{event_name}-{repair_id}.json"
    _write_json(path, payload)
    return path


def _write_repair_progress(root: Path, portfolio_slug: str, repair_id: str, batch_number: int, payload: dict[str, Any]) -> Path:
    path = _portfolio_paths(root, portfolio_slug)["runs"] / f"repair-progress-{repair_id}-{batch_number:04d}.json"
    _write_json(path, payload)
    return path


def _write_repair_checkpoint(root: Path, portfolio_slug: str, payload: dict[str, Any]) -> Path:
    path = _portfolio_paths(root, portfolio_slug)["repair_checkpoint"]
    _write_json(path, payload)
    return path


def _member_scope_label(member: Note) -> str:
    return str(member.frontmatter.get("scope", "")).strip().lower() or "unknown"


def _repair_member_note(root: Path, portfolio_slug: str, member: Note) -> tuple[Note, dict[str, int], str | None]:
    current_member = read_note(member.path)
    frontmatter = dict(current_member.frontmatter)
    project_slug = str(frontmatter.get("project_slug", "")).strip() or None
    counts = {
        "repaired_projects": 0,
        "repaired_members": 0,
        "requeued_members": 0,
        "normalized_non_runnable": 0,
        "baseline_reruns_required": 0,
        "provisional_members": 0,
    }
    title = str(frontmatter.get("title", "")).strip()
    scope = _member_scope_label(current_member)
    if scope == "state" and frontmatter.get("status") == "active_roster":
        metadata = dict(frontmatter.get("metadata", {}))
        if not frontmatter.get("roster_validated", False):
            frontmatter["status"] = "failed_roster"
            frontmatter["queue_state"] = "roster_failed"
            metadata["roster_error"] = metadata.get("roster_error", "registro legado sem validação suficiente para fila ativa.")
            counts["normalized_non_runnable"] += 1
        elif not _looks_like_person_title(title):
            frontmatter["status"] = "provisional_roster"
            frontmatter["queue_state"] = "provisional_roster"
            metadata["roster_error"] = metadata.get("roster_error", "registro legado ambíguo; mantido para auditoria, fora da fila ativa.")
            counts["normalized_non_runnable"] += 1
            counts["provisional_members"] += 1
        frontmatter["metadata"] = metadata

    project_repaired = _repair_project_from_member(root, portfolio_slug, Note(current_member.path, frontmatter, current_member.body))
    if project_repaired:
        counts["repaired_projects"] += 1

    updated = dict(frontmatter)
    if project_slug and updated.get("status") == "active_roster":
        try:
            project = get_project(root, project_slug)
        except FileNotFoundError:
            project = None
        if project is not None:
            baseline_plugins = set(project.frontmatter.get("baseline_plugin_names", []))
            if scope == "federal" and {"camara-profile", "camara-expenses"}.issubset(baseline_plugins):
                run_index = _project_run_index(root, project_slug)
                if "camara-profile" not in run_index or "camara-expenses" not in run_index:
                    if updated.get("queue_state") != "pending_baseline" or updated.get("baseline_completed_at") or updated.get("next_tick_after"):
                        counts["requeued_members"] += 1
                    updated["queue_state"] = "pending_baseline"
                    updated["baseline_completed_at"] = ""
                    updated["next_tick_after"] = ""
                    counts["baseline_reruns_required"] += 1
    desired_queue = str(updated.get("queue_state", "")).strip()
    if updated.get("status") == "failed_roster":
        desired_queue = "roster_failed"
    elif updated.get("status") == "inactive_roster":
        desired_queue = "inactive_roster"
    elif updated.get("status") == "provisional_roster":
        desired_queue = "provisional_roster"
    elif not project_slug:
        desired_queue = "pending_seed"
    elif not desired_queue:
        desired_queue = "pending_baseline"
    updated["queue_state"] = desired_queue
    if updated != current_member.frontmatter:
        updated["updated_at"] = _utc_now()
        write_note(current_member.path, updated, _member_body(Note(current_member.path, updated, "")))
        counts["repaired_members"] += 1
        current_member = read_note(current_member.path)
    return current_member, counts, project_slug


def _merge_repair_counts(target: dict[str, int], delta: dict[str, int]) -> None:
    for key, value in delta.items():
        target[key] = target.get(key, 0) + int(value or 0)


def _repair_metrics_for_batch(root: Path, portfolio_slug: str, project_slugs: set[str], progress: ProgressFn | None = None) -> dict[str, dict[str, Any]]:
    if not project_slugs:
        return {}
    ordered_projects = sorted(project_slugs)
    _emit(progress, f"[repair] recalculando métricas para {len(ordered_projects)} projeto(s) tocado(s)")
    metrics_by_project = update_project_case_metrics_batch(root, ordered_projects, refresh_cache_enabled=False)
    refresh_project_materialized_views_batch(root, ordered_projects, refresh_cache_enabled=False)
    for project_slug, metrics in metrics_by_project.items():
        member = read_note(_member_path(root, portfolio_slug, project_slug))
        desired_queue = _desired_queue_state_from_metrics(root, member, metrics)
        _update_member_metrics(root, portfolio_slug, project_slug, queue_state=desired_queue, metrics=metrics)
    refresh_cache(root)
    return metrics_by_project


def repair_portfolio(
    root: Path,
    slug: str,
    *,
    scope: str = "federal",
    batch_size: int = 25,
    resume: bool = True,
    progress: ProgressFn | None = None,
) -> Path:
    portfolio = get_portfolio(root, slug)
    portfolio_slug = portfolio.frontmatter["portfolio_slug"]
    normalized_scope = _normalized_scope(scope)
    effective_scope = normalized_scope if normalized_scope != "all" else "all"
    batch_size = max(1, int(batch_size))
    started_at = datetime.now(UTC)
    checkpoint = _repair_checkpoint_payload(root, portfolio_slug) if resume else {}
    repair_id = str(checkpoint.get("repair_id", "")).strip() if checkpoint.get("scope") == effective_scope else ""
    if not repair_id:
        repair_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
        checkpoint = {}
    members = [member for member in _member_notes(root, portfolio_slug) if _member_matches_scope(member, effective_scope)]
    processed_member_ids = {
        str(item).strip()
        for item in checkpoint.get("processed_member_ids", [])
        if str(item).strip()
    }
    pending_members = [member for member in members if member.frontmatter.get("id", "") not in processed_member_ids]
    total_members = len(members)
    completed_before = len(processed_member_ids)
    summary = {
        "repaired_projects": 0,
        "repaired_members": 0,
        "requeued_members": 0,
        "normalized_non_runnable": 0,
        "baseline_reruns_required": 0,
        "provisional_members": 0,
    }
    _emit(progress, f"[repair] iniciando reparo do portfolio `{portfolio_slug}` scope={effective_scope} batch_size={batch_size} pendentes={len(pending_members)}")
    _write_repair_event(
        root,
        portfolio_slug,
        repair_id,
        "repair-start",
        {
            "portfolio": portfolio_slug,
            "repair_id": repair_id,
            "scope": effective_scope,
            "batch_size": batch_size,
            "resume": bool(resume),
            "started_at": _utc_now(),
            "total_members": total_members,
            "completed_before": completed_before,
        },
    )
    last_touched_project = ""
    batch_number = int(checkpoint.get("batch_number", 0) or 0)
    try:
        for offset in range(0, len(pending_members), batch_size):
            batch_members = pending_members[offset : offset + batch_size]
            batch_number += 1
            batch_started = datetime.now(UTC)
            touched_projects: set[str] = set()
            batch_member_ids: list[str] = []
            for index, member in enumerate(batch_members, start=1):
                current_position = completed_before + len(processed_member_ids - set(checkpoint.get("processed_member_ids", []))) + len(batch_member_ids) + 1
                _emit(progress, f"[repair] lote {batch_number} membro {index}/{len(batch_members)} -> {member.frontmatter.get('title', member.path.stem)} ({member.frontmatter.get('scope', '')}/{member.frontmatter.get('uf', '')})")
                repaired_member, delta, touched_project = _repair_member_note(root, portfolio_slug, member)
                _merge_repair_counts(summary, delta)
                batch_member_ids.append(repaired_member.frontmatter.get("id", ""))
                processed_member_ids.add(repaired_member.frontmatter.get("id", ""))
                if touched_project:
                    touched_projects.add(touched_project)
                    last_touched_project = touched_project
            metrics_by_project = _repair_metrics_for_batch(root, portfolio_slug, touched_projects, progress=progress)
            elapsed_seconds = max(1.0, (datetime.now(UTC) - started_at).total_seconds())
            processed_total = len(processed_member_ids)
            remaining = max(0, total_members - processed_total)
            eta_seconds = int((elapsed_seconds / max(1, processed_total)) * remaining) if processed_total else 0
            checkpoint_payload = {
                "portfolio": portfolio_slug,
                "repair_id": repair_id,
                "scope": effective_scope,
                "batch_size": batch_size,
                "status": "running",
                "updated_at": _utc_now(),
                "processed_member_ids": sorted(processed_member_ids),
                "processed_count": processed_total,
                "total_members": total_members,
                "remaining_members": remaining,
                "batch_number": batch_number,
                "last_member": batch_members[-1].frontmatter.get("title", "") if batch_members else "",
                "last_touched_project": last_touched_project,
                "touched_projects": sorted(touched_projects),
                "summary": dict(summary),
            }
            _write_repair_checkpoint(root, portfolio_slug, checkpoint_payload)
            progress_payload = {
                "portfolio": portfolio_slug,
                "repair_id": repair_id,
                "scope": effective_scope,
                "batch_number": batch_number,
                "processed_count": processed_total,
                "total_members": total_members,
                "remaining_members": remaining,
                "elapsed_seconds": int((datetime.now(UTC) - started_at).total_seconds()),
                "eta_seconds": eta_seconds,
                "batch_elapsed_seconds": int((datetime.now(UTC) - batch_started).total_seconds()),
                "batch_member_ids": batch_member_ids,
                "touched_projects": sorted(touched_projects),
                "metrics_projects": sorted(metrics_by_project.keys()),
                "summary": dict(summary),
                "last_touched_project": last_touched_project,
            }
            _write_repair_progress(root, portfolio_slug, repair_id, batch_number, progress_payload)
            _emit(progress, f"[repair] lote {batch_number} concluído: processados={processed_total}/{total_members}, tocados={len(touched_projects)}, eta~{eta_seconds}s")
        current_members = _member_notes(root, portfolio_slug)
        portfolio_frontmatter = dict(portfolio.frontmatter)
        portfolio_frontmatter["member_count"] = len(current_members)
        portfolio_frontmatter["validated_active_count"] = sum(
            1
            for note in current_members
            if note.frontmatter.get("status") == "active_roster" and note.frontmatter.get("roster_validated", False)
        )
        if "roster_max_age_hours" not in portfolio_frontmatter:
            portfolio_frontmatter["roster_max_age_hours"] = _default_roster_max_age_hours()
        portfolio_frontmatter["updated_at"] = _utc_now()
        write_note(portfolio.path, portfolio_frontmatter, _portfolio_body(Note(portfolio.path, portfolio_frontmatter, "")))
        complete_payload = {
            "portfolio": portfolio_slug,
            "repair_id": repair_id,
            "completed_at": _utc_now(),
            "scope": effective_scope,
            "batch_size": batch_size,
            "processed_count": len(processed_member_ids),
            "total_members": total_members,
            "member_count": len(current_members),
            "summary": dict(summary),
            "last_touched_project": last_touched_project,
        }
        _write_repair_checkpoint(
            root,
            portfolio_slug,
            {
                **complete_payload,
                "status": "completed",
                "processed_member_ids": sorted(processed_member_ids),
                "remaining_members": max(0, total_members - len(processed_member_ids)),
                "batch_number": batch_number,
            },
        )
        run_path = _write_repair_event(root, portfolio_slug, repair_id, "repair-complete", complete_payload)
        _emit(progress, f"[repair] concluído: {summary}")
        refresh_cache(root)
        return run_path
    except Exception as exc:
        failure_payload = {
            "portfolio": portfolio_slug,
            "repair_id": repair_id,
            "failed_at": _utc_now(),
            "scope": effective_scope,
            "batch_size": batch_size,
            "processed_count": len(processed_member_ids),
            "total_members": total_members,
            "summary": dict(summary),
            "error": str(exc),
            "last_touched_project": last_touched_project,
            "batch_number": batch_number,
        }
        _write_repair_checkpoint(
            root,
            portfolio_slug,
            {
                **failure_payload,
                "status": "failed",
                "processed_member_ids": sorted(processed_member_ids),
                "remaining_members": max(0, total_members - len(processed_member_ids)),
            },
        )
        _emit(progress, f"[repair] falha: {exc}")
        return _write_repair_event(root, portfolio_slug, repair_id, "repair-failure", failure_payload)


def _load_alert_notes(root: Path, portfolio_slug: str) -> list[Note]:
    alert_root = _portfolio_paths(root, portfolio_slug)["alerts"]
    return [read_note(path) for path in sorted(alert_root.glob("*.md"))]


def _load_member_project(root: Path, member: Note) -> Note:
    return get_project(root, member.frontmatter["project_slug"])


def _update_member_metrics(
    root: Path,
    portfolio_slug: str,
    project_slug: str,
    *,
    queue_state: str | None = None,
    metrics: dict[str, Any] | None = None,
) -> Note:
    resolved_metrics = metrics or update_project_case_metrics(root, project_slug)
    member_path = _member_path(root, portfolio_slug, project_slug)
    member = read_note(member_path)
    frontmatter = dict(member.frontmatter)
    crossref_alert_count = int(resolved_metrics.get("crossref_alert_count", frontmatter.get("crossref_alert_count", 0)) or 0)
    strong_context_reasons = strong_context_reasons_from_metrics(resolved_metrics, crossref_alert_count=crossref_alert_count)
    portfolio_elevated = portfolio_elevated_from_metrics(resolved_metrics, crossref_alert_count=crossref_alert_count)
    frontmatter["lead_score"] = resolved_metrics["lead_score"]
    frontmatter["official_evidence_count"] = resolved_metrics["official_evidence_count"]
    frontmatter["official_signal_count"] = resolved_metrics["official_signal_count"]
    frontmatter["official_identity_count"] = resolved_metrics["official_identity_count"]
    frontmatter["official_signal_source_count"] = resolved_metrics["official_signal_source_count"]
    frontmatter["contextual_evidence_count"] = resolved_metrics["contextual_evidence_count"]
    frontmatter["contextual_domain_count"] = resolved_metrics["contextual_domain_count"]
    frontmatter["proposed_official_link_count"] = resolved_metrics["proposed_official_link_count"]
    frontmatter["organization_count"] = resolved_metrics["organization_count"]
    frontmatter["hypothesis_count"] = resolved_metrics["hypothesis_count"]
    frontmatter["law_count"] = resolved_metrics["law_count"]
    frontmatter["crossref_alert_count"] = crossref_alert_count
    frontmatter["strong_context_reasons"] = strong_context_reasons
    frontmatter["portfolio_elevated"] = portfolio_elevated
    frontmatter["needs_rebuild"] = bool(resolved_metrics.get("needs_rebuild", False))
    if queue_state:
        frontmatter["queue_state"] = queue_state
    frontmatter["updated_at"] = _utc_now()
    write_note(member.path, frontmatter, _member_body(Note(member.path, frontmatter, "")))
    project = get_project(root, project_slug)
    project_frontmatter = dict(project.frontmatter)
    project_metadata = _merge_metadata(
        project_frontmatter.get("metadata", {}),
        {
            "crossref_alert_count": crossref_alert_count,
            "strong_context_reasons": strong_context_reasons,
            "portfolio_elevated": portfolio_elevated,
            "needs_rebuild": bool(resolved_metrics.get("needs_rebuild", False)),
        },
    )
    if project_metadata != project_frontmatter.get("metadata", {}):
        project_frontmatter["metadata"] = project_metadata
        project_frontmatter["updated_at"] = _utc_now()
        write_note(project.path, project_frontmatter, project.body)
    return read_note(member.path)


def _desired_queue_state(root: Path, member: Note) -> str:
    status = str(member.frontmatter.get("status", "")).strip()
    if status == "inactive_roster":
        return "inactive_roster"
    if status == "failed_roster":
        return "roster_failed"
    if status == "provisional_roster":
        return "provisional_roster"
    project_slug = str(member.frontmatter.get("project_slug", "")).strip()
    if not project_slug:
        return "pending_seed"
    try:
        get_project(root, project_slug)
    except FileNotFoundError:
        return "pending_seed"
    current_state = str(member.frontmatter.get("queue_state", "")).strip()
    if current_state == "retry_backoff" and not _is_due(member):
        return "retry_backoff"
    if _baseline_needs_rerun(root, member):
        return "pending_baseline"
    if not member.frontmatter.get("baseline_completed_at"):
        return "pending_baseline"
    if member.frontmatter.get("needs_rebuild", False):
        return "pending_deep"
    if not member.frontmatter.get("deep_completed_at") and _project_should_deepen(root, project_slug):
        return "pending_deep"
    stale_state = _staleness_state(root, project_slug, member)
    if stale_state:
        return stale_state
    return "idle"


def _project_should_deepen_from_metrics(metrics: dict[str, Any]) -> bool:
    if metrics["official_signal_count"] >= 1:
        return True
    return (
        metrics["contextual_domain_count"] >= 2
        and metrics["proposed_official_link_count"] >= 1
        and metrics["official_identity_count"] >= 1
    )


def _is_elevated_from_metrics(metrics: dict[str, Any]) -> bool:
    return portfolio_elevated_from_metrics(metrics, crossref_alert_count=int(metrics.get("crossref_alert_count", 0) or 0))


def _staleness_state_from_metrics(member: Note, metrics: dict[str, Any]) -> str:
    if not _is_due(member):
        return ""
    if _is_elevated_from_metrics(metrics):
        return "stale_high_priority"
    return "stale_other"


def _desired_queue_state_from_metrics(root: Path, member: Note, metrics: dict[str, Any]) -> str:
    status = str(member.frontmatter.get("status", "")).strip()
    if status == "inactive_roster":
        return "inactive_roster"
    if status == "failed_roster":
        return "roster_failed"
    if status == "provisional_roster":
        return "provisional_roster"
    project_slug = str(member.frontmatter.get("project_slug", "")).strip()
    if not project_slug:
        return "pending_seed"
    current_state = str(member.frontmatter.get("queue_state", "")).strip()
    if current_state == "retry_backoff" and not _is_due(member):
        return "retry_backoff"
    if _baseline_needs_rerun(root, member):
        return "pending_baseline"
    if not member.frontmatter.get("baseline_completed_at"):
        return "pending_baseline"
    if bool(metrics.get("needs_rebuild", member.frontmatter.get("needs_rebuild", False))):
        return "pending_deep"
    if not member.frontmatter.get("deep_completed_at") and _project_should_deepen_from_metrics(metrics):
        return "pending_deep"
    stale_state = _staleness_state_from_metrics(member, metrics)
    if stale_state:
        return stale_state
    return "idle"


def _baseline_needs_rerun(root: Path, member: Note) -> bool:
    if member.frontmatter.get("status") != "active_roster":
        return False
    project_slug = str(member.frontmatter.get("project_slug", "")).strip()
    if not project_slug:
        return False
    try:
        project = get_project(root, project_slug)
    except FileNotFoundError:
        return False
    if str(member.frontmatter.get("scope", "")).strip().lower() != "federal":
        return False
    baseline_plugins = set(project.frontmatter.get("baseline_plugin_names", []))
    if not {"camara-profile", "camara-expenses"}.issubset(baseline_plugins):
        return False
    run_index = _project_run_index(root, project_slug)
    return "camara-profile" not in run_index or "camara-expenses" not in run_index


def _repair_project_from_member(root: Path, portfolio_slug: str, member: Note) -> bool:
    project_slug = str(member.frontmatter.get("project_slug", "")).strip()
    if not project_slug:
        return False
    try:
        project = get_project(root, project_slug)
    except FileNotFoundError:
        return False
    frontmatter = dict(project.frontmatter)
    member_scope = str(member.frontmatter.get("scope", "")).strip().lower()
    metadata = _merge_metadata(
        frontmatter.get("metadata", {}),
        {
            "portfolio_slug": portfolio_slug,
            "legislature_level": member_scope,
            "uf": member.frontmatter.get("uf", ""),
            "party": member.frontmatter.get("party", ""),
            "roster_plugin": member.frontmatter.get("roster_plugin", ""),
            "roster_member_id": member.frontmatter.get("roster_member_id", ""),
            "roster_url": member.frontmatter.get("roster_url", ""),
            "assembly_name": member.frontmatter.get("metadata", {}).get("assembly_name", ""),
        },
    )
    expected_plugins = _expected_project_plugins_for_scope(member_scope)
    expected_baseline = _expected_baseline_plugins_for_scope(member_scope)
    changed = False
    if frontmatter.get("plugin_names", []) != expected_plugins:
        frontmatter["plugin_names"] = expected_plugins
        changed = True
    if frontmatter.get("baseline_plugin_names", []) != expected_baseline:
        frontmatter["baseline_plugin_names"] = expected_baseline
        changed = True
    if frontmatter.get("contextual_plugin_names", []) != list(DEFAULT_CONTEXTUAL_PLUGINS):
        frontmatter["contextual_plugin_names"] = list(DEFAULT_CONTEXTUAL_PLUGINS)
        changed = True
    if metadata != frontmatter.get("metadata", {}):
        frontmatter["metadata"] = metadata
        changed = True
    if changed:
        frontmatter["updated_at"] = _utc_now()
        write_note(project.path, frontmatter, project.body)
    targets = project_targets(root, project_slug)
    if targets:
        target = targets[0]
        target_metadata = {
            "office": member.frontmatter.get("metadata", {}).get("office", ""),
            "legislature_level": member_scope,
            "uf": member.frontmatter.get("uf", ""),
            "party": member.frontmatter.get("party", ""),
            "current_mandate": member.frontmatter.get("status") == "active_roster",
            "portfolio_slug": portfolio_slug,
            "roster_plugin": member.frontmatter.get("roster_plugin", ""),
            "roster_member_id": member.frontmatter.get("roster_member_id", ""),
            "roster_url": member.frontmatter.get("roster_url", ""),
            "assembly_name": member.frontmatter.get("metadata", {}).get("assembly_name", ""),
            "aliases": list(member.frontmatter.get("aliases", [])),
            "tribunal_aliases": _default_tribunal_aliases(str(member.frontmatter.get("uf", "")).strip().upper()),
            **member.frontmatter.get("metadata", {}),
        }
        if target_metadata.get("legislature_level") == "federal":
            camara_id = target_metadata.get("camara_id") or member.frontmatter.get("metadata", {}).get("camara_id") or member.frontmatter.get("roster_member_id", "")
            if camara_id:
                target_metadata["camara_id"] = str(camara_id)
        desired_title = member.frontmatter.get("title", "") or target.frontmatter.get("title", "")
        desired_aliases = _dedupe_strings(list(member.frontmatter.get("aliases", [])))
        current_aliases = _dedupe_strings(list(target.frontmatter.get("aliases", [])))
        if (
            target.frontmatter.get("title", "") != desired_title
            or current_aliases != desired_aliases
            or _merge_metadata(target.frontmatter.get("metadata", {}), target_metadata) != target.frontmatter.get("metadata", {})
        ):
            target_frontmatter = dict(target.frontmatter)
            target_frontmatter["title"] = desired_title
            target_frontmatter["aliases"] = desired_aliases
            target_frontmatter["metadata"] = _merge_metadata(target.frontmatter.get("metadata", {}), target_metadata)
            target_frontmatter["updated_at"] = _utc_now()
            write_note(target.path, target_frontmatter, target.body)
            canonical_path = root / target.frontmatter["canonical_path"]
            if canonical_path.exists():
                canonical = read_note(canonical_path)
                canonical_frontmatter = dict(canonical.frontmatter)
                canonical_frontmatter["title"] = desired_title
                canonical_frontmatter["name"] = desired_title
                canonical_frontmatter["aliases"] = _dedupe_strings([*canonical.frontmatter.get("aliases", []), *desired_aliases])
                canonical_frontmatter["metadata"] = _merge_metadata(canonical.frontmatter.get("metadata", {}), target_metadata)
                canonical_frontmatter["updated_at"] = _utc_now()
                write_note(canonical.path, canonical_frontmatter, canonical.body)
            changed = True
    return changed


def _repair_portfolio_state(root: Path, portfolio_slug: str, progress: ProgressFn | None = None) -> dict[str, int]:
    repaired_projects = 0
    repaired_members = 0
    requeued_members = 0
    normalized_non_runnable = 0
    baseline_reruns_required = 0
    for member in _member_notes(root, portfolio_slug):
        current_member = read_note(member.path)
        frontmatter = dict(current_member.frontmatter)
        project_slug = str(frontmatter.get("project_slug", "")).strip()
        title = str(frontmatter.get("title", "")).strip()
        if str(frontmatter.get("scope", "")).strip().lower() == "state" and frontmatter.get("status") == "active_roster":
            if not frontmatter.get("roster_validated", False) or not _looks_like_person_title(title):
                frontmatter["status"] = "failed_roster"
                frontmatter["queue_state"] = "roster_failed"
                metadata = dict(frontmatter.get("metadata", {}))
                metadata["roster_error"] = metadata.get("roster_error", "registro legado normalizado por repair; membro não confiável para fila ativa.")
                frontmatter["metadata"] = metadata
                frontmatter["updated_at"] = _utc_now()
                write_note(current_member.path, frontmatter, _member_body(Note(current_member.path, frontmatter, "")))
                current_member = read_note(current_member.path)
                normalized_non_runnable += 1
        project_repaired = _repair_project_from_member(root, portfolio_slug, current_member)
        if project_repaired:
            repaired_projects += 1
        if project_slug:
            try:
                update_project_case_metrics(root, project_slug)
                refresh_project_materialized_views(root, project_slug)
            except FileNotFoundError:
                pass
        refreshed_member = read_note(member.path)
        updated = dict(refreshed_member.frontmatter)
        if project_slug and updated.get("status") == "active_roster":
            try:
                project = get_project(root, project_slug)
            except FileNotFoundError:
                project = None
            if project is not None:
                baseline_plugins = set(project.frontmatter.get("baseline_plugin_names", []))
                if str(updated.get("scope", "")).strip().lower() == "federal" and {
                    "camara-profile",
                    "camara-expenses",
                }.issubset(baseline_plugins):
                    run_index = _project_run_index(root, project_slug)
                    if "camara-profile" not in run_index or "camara-expenses" not in run_index:
                        updated["queue_state"] = "pending_baseline"
                        updated["baseline_completed_at"] = ""
                        updated["next_tick_after"] = ""
                        baseline_reruns_required += 1
        desired_state = _desired_queue_state(root, Note(refreshed_member.path, updated, refreshed_member.body))
        if updated.get("queue_state") != desired_state:
            updated["queue_state"] = desired_state
            requeued_members += 1
        if project_slug:
            try:
                metrics = project_case_metrics(root, project_slug)
            except FileNotFoundError:
                metrics = None
            if metrics is not None:
                updated["lead_score"] = metrics["lead_score"]
                updated["official_evidence_count"] = metrics["official_evidence_count"]
                updated["official_signal_count"] = metrics["official_signal_count"]
                updated["official_identity_count"] = metrics["official_identity_count"]
                updated["contextual_evidence_count"] = metrics["contextual_evidence_count"]
                updated["contextual_domain_count"] = metrics["contextual_domain_count"]
                updated["proposed_official_link_count"] = metrics["proposed_official_link_count"]
                updated["organization_count"] = metrics["organization_count"]
                updated["hypothesis_count"] = metrics["hypothesis_count"]
                updated["law_count"] = metrics["law_count"]
        if updated != refreshed_member.frontmatter:
            updated["updated_at"] = _utc_now()
            write_note(refreshed_member.path, updated, _member_body(Note(refreshed_member.path, updated, "")))
            repaired_members += 1
    if repaired_projects or repaired_members or requeued_members:
        _emit(
            progress,
            f"[repair] projetos ajustados={repaired_projects}, membros atualizados={repaired_members}, refileirados={requeued_members}",
        )
    refresh_cache(root)
    return {
        "repaired_projects": repaired_projects,
        "repaired_members": repaired_members,
        "requeued_members": requeued_members,
        "normalized_non_runnable": normalized_non_runnable,
        "baseline_reruns_required": baseline_reruns_required,
    }


def _project_should_deepen(root: Path, project_slug: str) -> bool:
    metrics = project_case_metrics(root, project_slug)
    return _project_should_deepen_from_metrics(metrics)


def _is_elevated(root: Path, project_slug: str) -> bool:
    metrics = project_case_metrics(root, project_slug)
    return _is_elevated_from_metrics(metrics)


def _parse_timestamp(value: str) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _is_due(member: Note, *, now: datetime | None = None) -> bool:
    now = now or datetime.now(UTC)
    next_tick = _parse_timestamp(str(member.frontmatter.get("next_tick_after", "")))
    return next_tick is None or next_tick <= now


def _seed_missing_projects(root: Path, portfolio_slug: str) -> list[str]:
    repaired: list[str] = []
    for member in _member_notes(root, portfolio_slug):
        if member.frontmatter.get("status") in {"inactive_roster", "failed_roster", "provisional_roster"}:
            continue
        project_slug = member.frontmatter.get("project_slug", "")
        try:
            get_project(root, project_slug)
        except FileNotFoundError:
            frontmatter = dict(member.frontmatter)
            frontmatter["queue_state"] = "pending_seed"
            frontmatter["updated_at"] = _utc_now()
            write_note(member.path, frontmatter, _member_body(Note(member.path, frontmatter, "")))
            repaired.append(project_slug)
    return repaired


def _staleness_state(root: Path, project_slug: str, member: Note) -> str:
    if not _is_due(member):
        return ""
    if _is_elevated(root, project_slug):
        return "stale_high_priority"
    return "stale_other"


def _queue_buckets(root: Path, portfolio_slug: str, *, scope: str = "all", only_status: str = "all") -> dict[str, list[str]]:
    ranked_buckets: dict[str, list[tuple[tuple[Any, ...], str]]] = {
        "baseline_pending": [],
        "deep_pending": [],
        "stale_high_priority": [],
        "stale_other": [],
        "retries_due": [],
    }
    for member in _member_notes(root, portfolio_slug):
        if not _member_is_queue_eligible(member, scope=scope, only_status=only_status):
            continue
        project_slug = member.frontmatter.get("project_slug", "")
        queue_state = str(member.frontmatter.get("queue_state", "")).strip() or _desired_queue_state(root, member)
        if queue_state != str(member.frontmatter.get("queue_state", "")).strip():
            updated = dict(member.frontmatter)
            updated["queue_state"] = queue_state
            updated["updated_at"] = _utc_now()
            write_note(member.path, updated, _member_body(Note(member.path, updated, "")))
        rank = (
            not bool(member.frontmatter.get("needs_rebuild", False)),
            not bool(member.frontmatter.get("portfolio_elevated", False)),
            -int(member.frontmatter.get("lead_score", 0) or 0),
            _note_title(member),
        )
        if queue_state == "retry_backoff" and _is_due(member):
            ranked_buckets["retries_due"].append((rank, project_slug))
            continue
        if not member.frontmatter.get("baseline_completed_at"):
            ranked_buckets["baseline_pending"].append((rank, project_slug))
            continue
        if queue_state == "pending_deep" or (
            not member.frontmatter.get("deep_completed_at")
            and _project_should_deepen(root, project_slug)
        ):
            ranked_buckets["deep_pending"].append((rank, project_slug))
            continue
        stale_state = _staleness_state(root, project_slug, member)
        if stale_state:
            ranked_buckets[stale_state].append((rank, project_slug))
    return {
        bucket_name: [project_slug for _rank, project_slug in sorted(items)]
        for bucket_name, items in ranked_buckets.items()
    }


def _update_member_schedule(root: Path, portfolio_slug: str, project_slug: str, *, queue_state: str, next_tick_after: datetime, baseline_done: bool = False, deep_done: bool = False, failure_count: int | None = None) -> None:
    member = read_note(_member_path(root, portfolio_slug, project_slug))
    frontmatter = dict(member.frontmatter)
    frontmatter["queue_state"] = queue_state
    frontmatter["last_tick_at"] = _utc_now()
    frontmatter["next_tick_after"] = next_tick_after.replace(microsecond=0).isoformat()
    if baseline_done:
        frontmatter["baseline_completed_at"] = _utc_now()
    if deep_done:
        frontmatter["deep_completed_at"] = _utc_now()
    if failure_count is not None:
        frontmatter["failure_count"] = failure_count
    frontmatter["updated_at"] = _utc_now()
    write_note(member.path, frontmatter, _member_body(Note(member.path, frontmatter, "")))


def _run_project_batch(
    root: Path,
    portfolio_slug: str,
    project_slugs: list[str],
    *,
    mode: str,
    provider_name: str | None,
    stage_name: str,
    progress: ProgressFn | None = None,
    on_progress: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    processed: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    total_projects = len(project_slugs)
    for index, project_slug in enumerate(project_slugs, start=1):
        try:
            project = get_project(root, project_slug)
            if on_progress is not None:
                on_progress(
                    {
                        "event": "project_start",
                        "name": stage_name,
                        "project_slug": project_slug,
                        "project_title": project.frontmatter.get("title", project_slug),
                        "mode": mode,
                        "index": index,
                        "total_projects": total_projects,
                    }
                )
            _emit(
                progress,
                f"[{stage_name}] {index}/{total_projects} {project.frontmatter.get('title', project_slug)} -> mode={mode}",
            )
            def _forward_advance_progress(event: dict[str, Any]) -> None:
                if on_progress is not None:
                    merged = dict(event)
                    merged.setdefault("name", stage_name)
                    merged.setdefault("project_slug", project_slug)
                    merged.setdefault("project_title", project.frontmatter.get("title", project_slug))
                    merged.setdefault("mode", mode)
                    on_progress(merged)
                message = format_progress_event(event)
                if message:
                    _emit(progress, f"[{stage_name}] {index}/{total_projects} {project.frontmatter.get('title', project_slug)} -> {message}")

            summary_path = advance_project(root, project_slug, provider_name, mode=mode, progress=_forward_advance_progress)
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
            metrics = summary.get("project_metrics", project_case_metrics(root, project_slug))
            next_state = "pending_deep" if mode == "baseline" and _project_should_deepen(root, project_slug) else "idle"
            next_window = timedelta(days=1 if _is_elevated(root, project_slug) else 7)
            _update_member_schedule(
                root,
                portfolio_slug,
                project_slug,
                queue_state=next_state,
                next_tick_after=datetime.now(UTC) + next_window,
                baseline_done=mode == "baseline",
                deep_done=mode == "deep",
                failure_count=0,
            )
            member = _update_member_metrics(root, portfolio_slug, project_slug)
            desired_state = _desired_queue_state(root, member)
            if desired_state != member.frontmatter.get("queue_state", ""):
                member = _update_member_metrics(root, portfolio_slug, project_slug, queue_state=desired_state)
            processed.append(
                {
                    "project_slug": project_slug,
                    "summary_path": str(summary_path),
                    "lead_score": metrics.get("lead_score", 0),
                    "priority": metrics.get("priority", ""),
                    "member_path": str(member.path),
                }
            )
            if on_progress is not None:
                on_progress(
                    {
                        "event": "project_finish",
                        "name": stage_name,
                        "project_slug": project_slug,
                        "project_title": project.frontmatter.get("title", project_slug),
                        "mode": mode,
                        "processed": list(processed),
                        "failures": list(failures),
                    }
                )
            _emit(
                progress,
                f"[{stage_name}] concluído {index}/{total_projects} {project.frontmatter.get('title', project_slug)}",
            )
        except Exception as exc:
            member = read_note(_member_path(root, portfolio_slug, project_slug))
            failure_count = int(member.frontmatter.get("failure_count", 0)) + 1
            backoff_minutes = min(5 * (2 ** min(failure_count - 1, 5)), 300)
            _update_member_schedule(
                root,
                portfolio_slug,
                project_slug,
                queue_state="retry_backoff",
                next_tick_after=datetime.now(UTC) + timedelta(minutes=backoff_minutes),
                failure_count=failure_count,
            )
            failures.append({"project_slug": project_slug, "error": str(exc), "stage": stage_name})
            if on_progress is not None:
                on_progress(
                    {
                        "event": "project_error",
                        "name": stage_name,
                        "project_slug": project_slug,
                        "project_title": project_slug,
                        "mode": mode,
                        "error": str(exc),
                        "processed": list(processed),
                        "failures": list(failures),
                    }
                )
            _emit(progress, f"[{stage_name}] erro {index}/{total_projects} em {project_slug}: {exc}")
    return {"name": stage_name, "processed": processed, "failures": failures}


def _build_crossref_alerts(root: Path, portfolio_slug: str) -> list[Path]:
    members = _member_notes(root, portfolio_slug)
    shared_entities: dict[str, list[str]] = {}
    shared_refs: dict[str, list[str]] = {}
    shared_entity_notes: dict[str, dict[str, list[Note]]] = {}
    shared_ref_notes: dict[str, dict[str, list[Note]]] = {}
    note_index = build_note_index(root)
    for member in members:
        if member.frontmatter.get("status") in {"inactive_roster", "failed_roster", "provisional_roster"}:
            continue
        project_slug = member.frontmatter["project_slug"]
        project = get_project(root, project_slug)
        evidence_root = root / "projects" / slugify(project_slug) / "evidence"
        for evidence_path in evidence_root.glob("*.md"):
            evidence = read_note(evidence_path)
            for related_id in evidence.frontmatter.get("related_ids", []):
                if related_id.startswith("organization-"):
                    shared_entities.setdefault(related_id, []).append(project_slug)
                    shared_entity_notes.setdefault(related_id, {}).setdefault(project_slug, []).append(evidence)
            if evidence.frontmatter.get("source_class") in {"official_structured", "official_document"}:
                for ref in evidence.frontmatter.get("source_refs", []):
                    key = ref.get("url") or f"{ref.get('plugin', '')}:{ref.get('record_id', '')}"
                    if key:
                        shared_refs.setdefault(str(key), []).append(project_slug)
                        shared_ref_notes.setdefault(str(key), {}).setdefault(project_slug, []).append(evidence)
        for run_path in sorted((root / "projects" / slugify(project_slug) / "runs").glob("sync-*.json")):
            payload = json.loads(run_path.read_text(encoding="utf-8"))
            for link in payload.get("proposed_links", []):
                domain = str(link.get("domain", "")).strip()
                url = str(link.get("url", "")).strip()
                if not url or not domain:
                    continue
                if any(domain.endswith(suffix) for suffix in (".gov.br", ".jus.br", ".leg.br", ".mp.br")):
                    shared_refs.setdefault(url, []).append(project_slug)
        project.frontmatter
    alert_paths: list[Path] = []
    current_alerts: set[str] = set()
    for prefix, index in (("shared-entity", shared_entities), ("shared-reference", shared_refs)):
        for key, projects in index.items():
            unique_projects = sorted(set(projects))
            if len(unique_projects) < 2:
                continue
            alert_key = f"{prefix}-{key}"
            current_alerts.add(slugify(alert_key))
            path = _alert_path(root, portfolio_slug, alert_key)
            related_ids = []
            project_ids = []
            project_lines: list[str] = []
            linked_hypothesis_types: set[str] = {"relationship_network_risk"}
            entity_note = note_index.get(key)
            shared_label = _note_title(entity_note) if entity_note is not None else key
            for project_slug in unique_projects:
                project_note = get_project(root, project_slug)
                project_ids.append(project_slug)
                related_ids.append(project_note.frontmatter["id"])
                member_note = read_note(_member_path(root, portfolio_slug, project_slug))
                related_ids.append(member_note.frontmatter["id"])
                evidence_notes = (
                    shared_entity_notes.get(key, {}).get(project_slug, [])
                    if prefix == "shared-entity"
                    else shared_ref_notes.get(key, {}).get(project_slug, [])
                )
                evidence_links = []
                for evidence in evidence_notes[:3]:
                    evidence_links.append(_note_link(path, evidence))
                    plugin_name = str(evidence.frontmatter.get("plugin", "")).strip()
                    if plugin_name == "camara-expenses":
                        linked_hypothesis_types.update({"expense_anomaly", "relationship_network_risk"})
                    elif plugin_name in {"pncp", "transferegov", "querido-diario"}:
                        linked_hypothesis_types.update({"procurement_risk", "relationship_network_risk"})
                    elif plugin_name in {"portal-transparencia", "datajud", "tcu"}:
                        linked_hypothesis_types.add("sanction_or_control_risk")
                explanation = (
                    "evidências oficiais que mencionam a mesma entidade"
                    if prefix == "shared-entity"
                    else "referências oficiais coincidentes ou links oficiais recorrentes"
                )
                if evidence_links:
                    explanation += f": {', '.join(evidence_links)}"
                project_lines.append(f"- {_note_link(path, project_note)}: {explanation}")
            if prefix == "shared-entity":
                explainer = (
                    f"A entidade {shared_label} aparece em múltiplos projetos e pode indicar contraparte, fornecedor ou elo relacional recorrente."
                )
            else:
                explainer = (
                    f"A referência oficial compartilhada {shared_label} conecta múltiplos projetos e merece verificação de contexto, escopo e causalidade."
                )
            alert_relevance = _classify_alert_relevance(prefix, key, entity_note)
            alert_strength = min(0.97, 0.58 + len(unique_projects) * 0.08 + len(linked_hypothesis_types) * 0.03)
            if entity_note is not None:
                related_ids.append(entity_note.frontmatter["id"])
            frontmatter = {
                "id": _note_id("portfolio-alert", portfolio_slug, slugify(alert_key)),
                "type": "portfolio_alert",
                "title": f"Alerta cross-project: {prefix}",
                "status": "active",
                "source_class": "derived_workspace",
                "source_refs": [],
                "related_ids": _dedupe_strings(related_ids),
                "project_ids": unique_projects,
                "confidence": alert_strength,
                "updated_at": _utc_now(),
                "alert_type": prefix,
                "shared_key": key,
                "metadata": {
                    "shared_label": shared_label,
                    "explainer": explainer,
                    "alert_strength": alert_strength,
                    "alert_relevance": alert_relevance,
                    "linked_hypothesis_types": sorted(linked_hypothesis_types),
                },
            }
            body = "\n".join(
                [
                    f"# {frontmatter['title']}",
                    "",
                    "## Elo compartilhado",
                    "",
                    f"- Chave compartilhada: `{key}`",
                    f"- Referência principal: {_note_link(path, entity_note, shared_label) if entity_note is not None else f'`{shared_label}`'}",
                    f"- Força do alerta: `{alert_strength:.2f}`",
                    "",
                    "## Por que este alerta importa",
                    "",
                    explainer,
                    "",
                    "## Relevância analítica",
                    "",
                    f"- Classe: `{alert_relevance}`",
                    "",
                    "## Projetos conectados",
                    "",
                    *(project_lines or ["- Nenhum projeto adicional conectado."]),
                    "",
                    "## Hipóteses potencialmente reforçadas",
                    "",
                    *[f"- `{item}`" for item in sorted(linked_hypothesis_types)],
                ]
            )
            write_note(path, frontmatter, body)
            alert_paths.append(path)
    for path in _portfolio_paths(root, portfolio_slug)["alerts"].glob("*.md"):
        if path.stem in current_alerts:
            continue
        note = read_note(path)
        frontmatter = dict(note.frontmatter)
        if frontmatter.get("status") == "inactive":
            continue
        frontmatter["status"] = "inactive"
        frontmatter["updated_at"] = _utc_now()
        write_note(path, frontmatter, note.body)
    counts: dict[str, int] = {}
    for path in alert_paths:
        note = read_note(path)
        if not _alert_is_visible(note):
            continue
        for project_slug in note.frontmatter.get("project_ids", []):
            counts[project_slug] = counts.get(project_slug, 0) + 1
    for member in _member_notes(root, portfolio_slug):
        frontmatter = dict(member.frontmatter)
        project_slug = str(frontmatter.get("project_slug", "")).strip()
        crossref_alert_count = counts.get(project_slug, 0)
        frontmatter["crossref_alert_count"] = crossref_alert_count
        frontmatter["updated_at"] = _utc_now()
        write_note(member.path, frontmatter, _member_body(Note(member.path, frontmatter, "")))
        if project_slug:
            _update_member_metrics(root, portfolio_slug, project_slug, metrics=project_case_metrics(root, project_slug))
    refresh_cache(root)
    return sorted(alert_paths)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def _write_tick_event(root: Path, portfolio_slug: str, tick_id: str, event_name: str, payload: dict[str, Any]) -> Path:
    path = _portfolio_paths(root, portfolio_slug)["runs"] / f"{event_name}-{tick_id}.json"
    _write_json(path, payload)
    return path


def _write_tick_stage_progress(root: Path, portfolio_slug: str, tick_id: str, stage: dict[str, Any]) -> Path:
    safe_name = slugify(stage.get("name", "stage"))
    path = _portfolio_paths(root, portfolio_slug)["runs"] / f"tick-stage-progress-{tick_id}-{safe_name}.json"
    _write_json(path, stage)
    return path


def _empty_current_worker_state() -> dict[str, str]:
    return {
        "current_stage": "",
        "current_project_slug": "",
        "current_project_title": "",
        "current_mode": "",
        "current_plugin": "",
        "current_plugin_started_at": "",
    }


def _write_checkpoint(
    root: Path,
    portfolio_slug: str,
    processed: list[str],
    failures: list[dict[str, Any]],
    *,
    current_state: dict[str, str] | None = None,
) -> Path:
    current = dict(_empty_current_worker_state())
    if current_state:
        current.update({key: str(value or "") for key, value in current_state.items() if key in current})
    checkpoint = WorkerCheckpoint(
        last_tick_at=_utc_now(),
        next_tick_after=(datetime.now(UTC) + timedelta(minutes=5)).replace(microsecond=0).isoformat(),
        portfolio_slug=portfolio_slug,
        processed_projects=processed,
        failures=failures,
        current_stage=current["current_stage"],
        current_project_slug=current["current_project_slug"],
        current_project_title=current["current_project_title"],
        current_mode=current["current_mode"],
        current_plugin=current["current_plugin"],
        current_plugin_started_at=current["current_plugin_started_at"],
    )
    checkpoint_path = _portfolio_paths(root, portfolio_slug)["checkpoint"]
    _write_json(checkpoint_path, checkpoint.to_dict())
    return checkpoint_path


def _latest_successful_roster_sync(root: Path, portfolio_slug: str) -> dict[str, Any]:
    run_root = _portfolio_paths(root, portfolio_slug)["runs"]
    for path in sorted(run_root.glob("roster-sync-*.json"), reverse=True):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            continue
        if not (
            int(payload.get("created_projects", 0)) > 0
            or int(payload.get("updated_projects", 0)) > 0
            or any(int(source.get("member_count", 0)) > 0 for source in payload.get("sources", []))
        ):
            continue
        payload["_path"] = str(path)
        return payload
    return {}


def _stage_quotas(max_projects: int, buckets: dict[str, list[str]]) -> dict[str, int]:
    if max_projects <= 0:
        return {
            "baseline_pending": 0,
            "deep_pending": 0,
            "stale_high_priority": 0,
            "stale_other": 0,
        }
    backlogs = {
        "baseline_pending": len(buckets["baseline_pending"]),
        "deep_pending": len(buckets["deep_pending"]),
        "stale_high_priority": len(buckets["stale_high_priority"]),
        "stale_other": len(buckets["stale_other"]) + len(buckets["retries_due"]),
    }
    ratios = {
        "baseline_pending": 0.45,
        "deep_pending": 0.35,
        "stale_high_priority": 0.05,
        "stale_other": 0.15,
    }
    quotas = {key: 0 for key in backlogs}
    remaining = max_projects
    for stage_name in ("baseline_pending", "deep_pending", "stale_high_priority", "stale_other"):
        backlog = backlogs[stage_name]
        if backlog <= 0 or remaining <= 0:
            continue
        quota = min(backlog, int(max_projects * ratios[stage_name]))
        if stage_name == "deep_pending" and backlog > 0 and quota == 0:
            quota = 1
        quotas[stage_name] = min(backlog, quota)
        remaining -= quotas[stage_name]
    if backlogs["deep_pending"] > 0 and quotas["deep_pending"] == 0 and remaining > 0:
        quotas["deep_pending"] = 1
        remaining -= 1
    for stage_name in ("deep_pending", "stale_high_priority", "baseline_pending", "stale_other"):
        backlog = backlogs[stage_name]
        while remaining > 0 and quotas[stage_name] < backlog:
            quotas[stage_name] += 1
            remaining -= 1
    return quotas


def build_portfolio_leaderboard(root: Path, slug: str) -> Path:
    portfolio = get_portfolio(root, slug)
    portfolio_slug = portfolio.frontmatter["portfolio_slug"]
    members = _member_notes(root, portfolio_slug)
    coverage = {
        "active": sum(
            1
            for note in members
            if note.frontmatter.get("status") == "active_roster" and note.frontmatter.get("roster_validated", False)
        ),
        "validated_active": sum(
            1
            for note in members
            if note.frontmatter.get("status") == "active_roster" and note.frontmatter.get("roster_validated", False)
        ),
        "inactive": sum(1 for note in members if note.frontmatter.get("status") == "inactive_roster"),
        "failed_sources": len({note.frontmatter.get("roster_plugin", "") for note in members if note.frontmatter.get("status") == "failed_roster"}),
        "baseline_done": sum(1 for note in members if note.frontmatter.get("baseline_completed_at")),
        "baseline_with_signals": sum(
            1
            for note in members
            if note.frontmatter.get("status") == "active_roster"
            and note.frontmatter.get("roster_validated", False)
            and int(note.frontmatter.get("official_signal_count", 0)) > 0
        ),
        "deep_done": sum(1 for note in members if note.frontmatter.get("deep_completed_at")),
        "hypotheses": sum(
            int(note.frontmatter.get("hypothesis_count", 0))
            for note in members
            if note.frontmatter.get("status") == "active_roster" and note.frontmatter.get("roster_validated", False)
        ),
        "organizations": sum(
            int(note.frontmatter.get("organization_count", 0))
            for note in members
            if note.frontmatter.get("status") == "active_roster" and note.frontmatter.get("roster_validated", False)
        ),
        "alerts": len(
            [
                note
                for note in _load_alert_notes(root, portfolio_slug)
                if note.frontmatter.get("status") == "active" and _alert_is_visible(note)
            ]
        ),
    }
    rows: list[dict[str, Any]] = []
    for member, project in _active_member_rows(root, portfolio_slug):
        project_slug = member.frontmatter.get("project_slug", "")
        metrics = _member_metric_counts(member, project)
        project_metadata = dict(project.frontmatter.get("metadata", {}))
        priority = str(project_metadata.get("priority_snapshot", "pista") or "pista")
        elevated = portfolio_elevated_from_metrics(
            {
                "priority": priority,
                "official_signal_count": metrics["official_signal_count"],
                "official_signal_source_count": metrics["official_signal_source_count"],
                "hypothesis_count": metrics["hypothesis_count"],
                "contextual_domain_count": metrics["contextual_domain_count"],
                "proposed_official_link_count": metrics["proposed_official_link_count"],
                "official_identity_count": metrics["official_identity_count"],
                "lead_score": metrics["lead_score"],
            },
            crossref_alert_count=metrics["crossref_alert_count"],
        )
        member_link = Path(os.path.relpath(member.path, start=_portfolio_paths(root, portfolio_slug)["leaderboard"].parent)).as_posix()
        rows.append(
            {
                "title": member.frontmatter.get("title", project_slug),
                "project_slug": project_slug,
                "member_link": member_link,
                "lead_score": metrics["lead_score"],
                "official_evidence_count": metrics["official_evidence_count"],
                "official_signal_count": metrics["official_signal_count"],
                "hypothesis_count": metrics["hypothesis_count"],
                "crossref_alert_count": int(member.frontmatter.get("crossref_alert_count", 0)),
                "priority": priority,
                "portfolio_elevated": elevated,
                "needs_rebuild": bool(metrics["needs_rebuild"]),
                "queue_state": str(member.frontmatter.get("queue_state", "")).strip(),
                "deep_completed_at": str(member.frontmatter.get("deep_completed_at", "")).strip(),
                "headline_signal": str(project_metadata.get("headline_signal", "")).strip(),
                "headline_counterparty": str(project_metadata.get("headline_counterparty", "")).strip(),
                "headline_alert": str(project_metadata.get("headline_alert", "")).strip(),
                "next_official_step": str(project_metadata.get("next_official_step", "")).strip(),
                "updated_at": member.frontmatter.get("updated_at", ""),
            }
        )
    elevated = sorted(
        [row for row in rows if row["portfolio_elevated"]],
        key=lambda item: (item["priority"] != "alta_prioridade_investigativa", -item["official_signal_count"], -item["crossref_alert_count"], item["updated_at"]),
    )
    watchlist = sorted(
        [row for row in rows if row not in elevated and row["lead_score"] > 0],
        key=lambda item: (-item["lead_score"], -item["crossref_alert_count"], item["updated_at"]),
    )
    ready_for_review = [
        row
        for row in elevated
        if row["deep_completed_at"] and not row["needs_rebuild"]
    ]
    deep_pending = [
        row
        for row in elevated
        if row["queue_state"] == "pending_deep" or row["needs_rebuild"]
    ]
    shallow_elevated = [
        row
        for row in elevated
        if row not in ready_for_review and row not in deep_pending
    ]
    path = _portfolio_paths(root, portfolio_slug)["leaderboard"]
    frontmatter = {
        "id": _note_id("portfolio-leaderboard", portfolio_slug),
        "type": "portfolio_leaderboard",
        "title": f"Leaderboard {portfolio.frontmatter.get('title', '')}",
        "status": "derived",
        "source_class": "derived_workspace",
        "source_refs": [],
        "related_ids": [note.frontmatter["id"] for note in members],
        "project_ids": [note.frontmatter["project_slug"] for note in members],
        "confidence": 1.0,
        "updated_at": _utc_now(),
    }
    write_note(path, frontmatter, _leaderboard_body(portfolio, coverage, watchlist, ready_for_review, deep_pending, shallow_elevated))
    portfolio_frontmatter = dict(portfolio.frontmatter)
    portfolio_frontmatter["member_count"] = len(members)
    portfolio_frontmatter["validated_active_count"] = coverage["validated_active"]
    portfolio_frontmatter["failed_source_count"] = coverage["failed_sources"]
    portfolio_frontmatter["roster_failure_count"] = coverage["failed_sources"]
    portfolio_frontmatter["baseline_with_signals_count"] = coverage["baseline_with_signals"]
    portfolio_frontmatter["deep_completed_count"] = coverage["deep_done"]
    portfolio_frontmatter["organization_count"] = coverage["organizations"]
    portfolio_frontmatter["hypothesis_count"] = coverage["hypotheses"]
    portfolio_frontmatter["watchlist_count"] = len(watchlist)
    portfolio_frontmatter["elevated_count"] = len(elevated)
    portfolio_frontmatter["updated_at"] = _utc_now()
    write_note(portfolio.path, portfolio_frontmatter, _portfolio_body(Note(portfolio.path, portfolio_frontmatter, "")))
    refresh_cache(root)
    return path


def diagnose_portfolio(root: Path, slug: str, *, limit: int = 25) -> dict[str, Any]:
    portfolio = get_portfolio(root, slug)
    portfolio_slug = portfolio.frontmatter["portfolio_slug"]
    members = _member_notes(root, portfolio_slug)
    latest_tick_failure = _latest_tick_event(root, portfolio_slug, "tick-failure")
    latest_tick_complete = _latest_tick_event(root, portfolio_slug, "tick-complete")
    checkpoint = _load_checkpoint(root, portfolio_slug)
    state_totals: dict[str, dict[str, int]] = {}
    stuck_cases: list[dict[str, Any]] = []
    for member in members:
        uf = str(member.frontmatter.get("uf", "??")).strip() or "??"
        bucket = state_totals.setdefault(
            uf,
            {
                "active_validated": 0,
                "failed_roster": 0,
                "inactive_roster": 0,
                "provisional_roster": 0,
                "pending_baseline": 0,
                "pending_deep": 0,
                "deep_completed": 0,
                "zero_successful_plugins": 0,
            },
        )
        status = str(member.frontmatter.get("status", "")).strip()
        if status == "active_roster" and member.frontmatter.get("roster_validated", False):
            bucket["active_validated"] += 1
        elif status == "failed_roster":
            bucket["failed_roster"] += 1
        elif status == "inactive_roster":
            bucket["inactive_roster"] += 1
        elif status == "provisional_roster":
            bucket["provisional_roster"] += 1
        queue_state = _desired_queue_state(root, member)
        if queue_state == "pending_baseline":
            bucket["pending_baseline"] += 1
        elif queue_state == "pending_deep":
            bucket["pending_deep"] += 1
        if member.frontmatter.get("deep_completed_at"):
            bucket["deep_completed"] += 1
        project_slug = str(member.frontmatter.get("project_slug", "")).strip()
        if not project_slug:
            continue
        run_index = _project_run_index(root, project_slug)
        project = None
        metrics = _member_metric_counts(member)
        plugin_runs: list[dict[str, Any]] = list(run_index.values())
        successful_plugin_count = sum(1 for payload in plugin_runs if len(payload.get("records", [])) > 0)
        if successful_plugin_count == 0:
            bucket["zero_successful_plugins"] += 1
        try:
            project = get_project(root, project_slug)
            metrics = _member_metric_counts(member, project)
        except FileNotFoundError:
            pass
        baseline_plugins = list(project.frontmatter.get("baseline_plugin_names", [])) if project is not None else []
        required_baseline_not_run = [
            plugin_name
            for plugin_name in ("camara-profile", "camara-expenses")
            if plugin_name in baseline_plugins and plugin_name not in run_index
        ]
        zero_record_plugins = sorted(
            plugin_name
            for plugin_name, payload in run_index.items()
            if not payload.get("error") and len(payload.get("records", [])) == 0
        )
        failing_plugins = sorted(
            plugin_name
            for plugin_name, payload in run_index.items()
            if str(payload.get("error", "")).strip()
        )
        deep_ready = metrics["official_signal_count"] >= 1 or (
            metrics["contextual_domain_count"] >= 2
            and metrics["proposed_official_link_count"] >= 1
            and metrics["official_identity_count"] >= 1
        )
        stuck_reason = ""
        if not member.frontmatter.get("last_tick_at"):
            stuck_reason = msg("razao_nunca_processado")
        elif queue_state == "pending_baseline" and not _is_due(member):
            stuck_reason = msg("razao_aguardando_cadencia")
        elif latest_tick_failure and (
            not latest_tick_complete
            or str(latest_tick_failure.get("failed_at", "")) > str(latest_tick_complete.get("completed_at", ""))
        ):
            stuck_reason = msg("razao_falha_tick_previa")
        elif required_baseline_not_run:
            stuck_reason = msg("razao_reparo_sem_rerun")
        elif successful_plugin_count == 0:
            stuck_reason = msg("razao_zero_plugins_sucesso")
        elif bool(member.frontmatter.get("needs_rebuild", False)):
            stuck_reason = "caso elevado precisa de rebuild narrativo"
        elif not deep_ready and queue_state in {"pending_deep", "idle", "stale_high_priority", "stale_other"}:
            stuck_reason = msg("razao_sem_sinais_promoviveis")
        elif queue_state == "pending_baseline":
            stuck_reason = "baseline still pending"
        elif queue_state == "pending_deep":
            stuck_reason = "deep still pending"
        if queue_state in {"pending_baseline", "pending_deep", "retry_backoff"}:
            stuck_cases.append(
                {
                    "project_slug": project_slug,
                    "title": member.frontmatter.get("title", project_slug),
                    "uf": uf,
                    "queue_state": queue_state,
                    "status": status,
                    "lead_score": metrics["lead_score"],
                    "official_signal_count": metrics["official_signal_count"],
                    "organization_count": metrics["organization_count"],
                    "hypothesis_count": metrics["hypothesis_count"],
                    "successful_plugin_count": successful_plugin_count,
                    "deep_ready": deep_ready,
                    "stuck_reason": stuck_reason,
                    "required_baseline_not_run": required_baseline_not_run,
                    "failing_plugins": failing_plugins[:5],
                    "zero_record_plugins": zero_record_plugins[:5],
                    "checkpoint_processed": project_slug in checkpoint.get("processed_projects", []),
                }
            )
    stuck_cases.sort(
        key=lambda item: (
            item["queue_state"] != "pending_deep",
            -item["lead_score"],
            item["successful_plugin_count"],
            item["title"],
        )
    )
    return {
        "portfolio": portfolio_slug,
        "title": portfolio.frontmatter.get("title", portfolio_slug),
        "updated_at": portfolio.frontmatter.get("updated_at", ""),
        "member_count": len(members),
        "last_roster_sync_at": portfolio.frontmatter.get("last_roster_sync_at", ""),
        "roster_max_age_hours": _portfolio_roster_max_age_hours(portfolio),
        "queue_totals_by_state": dict(sorted(state_totals.items())),
        "stuck_cases": stuck_cases[: max(1, limit)],
    }


def portfolio_status(root: Path, slug: str) -> dict[str, Any]:
    portfolio = get_portfolio(root, slug)
    portfolio_slug = portfolio.frontmatter["portfolio_slug"]
    repair_checkpoint = _repair_checkpoint_payload(root, portfolio_slug)
    tick_checkpoint = _load_checkpoint(root, portfolio_slug)
    latest_tick_start = _latest_tick_event(root, portfolio_slug, "tick-start")
    latest_tick_complete = _latest_tick_event(root, portfolio_slug, "tick-complete")
    latest_tick_failure = _latest_tick_event(root, portfolio_slug, "tick-failure")
    latest_repair_complete = _latest_run_payload(root, portfolio_slug, "repair-complete")
    latest_repair_failure = _latest_run_payload(root, portfolio_slug, "repair-failure")
    latest_repair_start = _latest_run_payload(root, portfolio_slug, "repair-start")
    members = _member_notes(root, portfolio_slug)
    processed_repair = int(repair_checkpoint.get("processed_count", 0) or 0)
    total_repair = int(repair_checkpoint.get("total_members", len(members)) or len(members))
    return {
        "portfolio": portfolio_slug,
        "title": portfolio.frontmatter.get("title", portfolio_slug),
        "member_count": len(members),
        "last_roster_sync_at": portfolio.frontmatter.get("last_roster_sync_at", ""),
        "repair": {
            "repair_id": repair_checkpoint.get("repair_id", "") or latest_repair_start.get("repair_id", ""),
            "status": repair_checkpoint.get("status", "idle"),
            "scope": repair_checkpoint.get("scope", ""),
            "batch_number": int(repair_checkpoint.get("batch_number", 0) or 0),
            "processed_count": processed_repair,
            "remaining_members": int(repair_checkpoint.get("remaining_members", max(0, total_repair - processed_repair)) or 0),
            "total_members": total_repair,
            "last_member": repair_checkpoint.get("last_member", ""),
            "last_touched_project": repair_checkpoint.get("last_touched_project", ""),
            "last_started_at": latest_repair_start.get("started_at", ""),
            "last_completed_at": latest_repair_complete.get("completed_at", ""),
            "last_failed_at": latest_repair_failure.get("failed_at", ""),
            "last_error": latest_repair_failure.get("error", ""),
        },
        "tick": {
            "status": "failed" if latest_tick_failure and (
                not latest_tick_complete or str(latest_tick_failure.get("failed_at", "")) > str(latest_tick_complete.get("completed_at", ""))
            ) else ("completed" if latest_tick_complete else ("running" if latest_tick_start else "idle")),
            "last_started_at": latest_tick_start.get("started_at", ""),
            "last_completed_at": latest_tick_complete.get("completed_at", ""),
            "last_failed_at": latest_tick_failure.get("failed_at", ""),
            "last_error": latest_tick_failure.get("error", ""),
            "processed_projects": len(tick_checkpoint.get("processed_projects", [])),
            "failure_count": len(tick_checkpoint.get("failures", [])),
            "next_tick_after": tick_checkpoint.get("next_tick_after", ""),
            "current_stage": tick_checkpoint.get("current_stage", ""),
            "current_project_slug": tick_checkpoint.get("current_project_slug", ""),
            "current_project_title": tick_checkpoint.get("current_project_title", ""),
            "current_mode": tick_checkpoint.get("current_mode", ""),
            "current_plugin": tick_checkpoint.get("current_plugin", ""),
            "current_plugin_started_at": tick_checkpoint.get("current_plugin_started_at", ""),
        },
    }


def portfolio_tick(
    root: Path,
    slug: str,
    *,
    max_projects: int = 200,
    provider_name: str | None = None,
    max_concurrent: int = 4,
    sync_roster_mode: str = "auto",
    roster_max_age_hours: int | None = None,
    scope: str = "all",
    only_status: str = "all",
    progress: ProgressFn | None = None,
) -> Path:
    del max_concurrent
    portfolio = get_portfolio(root, slug)
    portfolio_slug = portfolio.frontmatter["portfolio_slug"]
    normalized_scope = _normalized_scope(scope)
    normalized_only_status = _normalized_only_status(only_status)
    started_at = _utc_now()
    stages: list[dict[str, Any]] = []
    tick_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    processed_projects: list[str] = []
    failure_items: list[dict[str, Any]] = []
    current_state = _empty_current_worker_state()
    _emit(progress, f"[tick] iniciando portfolio `{portfolio_slug}`")
    _write_tick_event(
        root,
        portfolio_slug,
        tick_id,
        "tick-start",
        {
            "portfolio": portfolio_slug,
            "started_at": started_at,
            "max_projects": max_projects,
            "provider": provider_name or os.environ.get("INVESTIGADOR_AGENT_PROVIDER", "mock"),
            "sync_roster_mode": sync_roster_mode,
            "scope": normalized_scope,
            "only_status": normalized_only_status,
        },
    )
    failure_message = ""
    status = "failed"

    def _persist_checkpoint() -> Path:
        return _write_checkpoint(root, portfolio_slug, processed_projects, failure_items, current_state=current_state)

    try:
        portfolio = get_portfolio(root, portfolio_slug)
        if roster_max_age_hours is not None and int(portfolio.frontmatter.get("roster_max_age_hours", 0) or 0) != max(1, int(roster_max_age_hours)):
            updated_portfolio = dict(portfolio.frontmatter)
            updated_portfolio["roster_max_age_hours"] = max(1, int(roster_max_age_hours))
            updated_portfolio["updated_at"] = _utc_now()
            write_note(portfolio.path, updated_portfolio, _portfolio_body(Note(portfolio.path, updated_portfolio, "")))
            portfolio = read_note(portfolio.path)
        normalized_sync_mode = str(sync_roster_mode or "auto").strip().lower()
        should_sync = _should_sync_roster(root, portfolio, normalized_sync_mode, roster_max_age_hours)
        current_state.update(_empty_current_worker_state())
        current_state["current_stage"] = "sync_roster"
        if should_sync:
            roster_path = sync_portfolio_roster(root, portfolio_slug, progress=progress)
            roster_payload = json.loads(roster_path.read_text(encoding="utf-8"))
            stage = {
                "name": "sync_roster",
                "run_path": str(roster_path),
                "created_projects": roster_payload["created_projects"],
                "updated_projects": roster_payload["updated_projects"],
                "source_failures": roster_payload.get("source_failures", []),
                "mode": normalized_sync_mode,
                "skipped": False,
            }
        else:
            roster_payload = _latest_successful_roster_sync(root, portfolio_slug)
            stage = {
                "name": "sync_roster",
                "run_path": str(roster_payload.get("_path", "")),
                "created_projects": 0,
                "updated_projects": 0,
                "source_failures": [],
                "mode": normalized_sync_mode,
                "skipped": True,
                "reason": "recent successful roster sync still within cadence window",
            }
            _emit(progress, f"[sync_roster] pulando roster para `{portfolio_slug}`; último sync ainda está fresco")
        stages.append(stage)
        _write_tick_stage_progress(root, portfolio_slug, tick_id, stage)
        _persist_checkpoint()

        current_state.update(_empty_current_worker_state())
        current_state["current_stage"] = "seed_missing_projects"
        repaired = _seed_missing_projects(root, portfolio_slug)
        if repaired:
            _emit(progress, f"[seed_missing_projects] reparando {len(repaired)} projeto(s)")
        stage = {"name": "seed_missing_projects", "projects": repaired}
        stages.append(stage)
        _write_tick_stage_progress(root, portfolio_slug, tick_id, stage)
        _persist_checkpoint()

        buckets = _queue_buckets(root, portfolio_slug, scope=normalized_scope, only_status=normalized_only_status)
        quotas = _stage_quotas(max_projects, buckets)
        _emit(
            progress,
            f"[tick] filas scope={normalized_scope} only_status={normalized_only_status}: baseline={len(buckets['baseline_pending'])} deep={len(buckets['deep_pending'])} stale={len(buckets['stale_other']) + len(buckets['stale_high_priority'])} retry={len(buckets['retries_due'])}",
        )
        for stage_name, bucket_name, mode in (
            ("baseline_pending", "baseline_pending", "baseline"),
            ("deep_pending", "deep_pending", "deep"),
            ("stale_high_priority", "stale_high_priority", "deep"),
            ("stale_other", "stale_other", "baseline"),
        ):
            current_state.update(_empty_current_worker_state())
            current_state["current_stage"] = stage_name
            if stage_name == "stale_other":
                selected = list(dict.fromkeys([*buckets["stale_other"], *buckets["retries_due"]]))[: quotas[stage_name]]
            else:
                selected = list(buckets[bucket_name])[: quotas[stage_name]]
            if selected:
                _emit(progress, f"[{stage_name}] {len(selected)} projeto(s) na fila")
            def _persist_partial(stage_snapshot: dict[str, Any]) -> None:
                event_name = str(stage_snapshot.get("event", "")).strip()
                if event_name == "project_start":
                    current_state["current_stage"] = stage_name
                    current_state["current_project_slug"] = str(stage_snapshot.get("project_slug", "")).strip()
                    current_state["current_project_title"] = str(stage_snapshot.get("project_title", "")).strip()
                    current_state["current_mode"] = str(stage_snapshot.get("mode", "")).strip()
                    current_state["current_plugin"] = ""
                    current_state["current_plugin_started_at"] = ""
                    _persist_checkpoint()
                    return
                if event_name == "plugin_start":
                    current_state["current_stage"] = stage_name
                    current_state["current_project_slug"] = str(stage_snapshot.get("project_slug", "")).strip()
                    current_state["current_project_title"] = str(stage_snapshot.get("project_title", "")).strip()
                    current_state["current_mode"] = str(stage_snapshot.get("mode", "")).strip()
                    current_state["current_plugin"] = str(stage_snapshot.get("plugin", "")).strip()
                    current_state["current_plugin_started_at"] = str(stage_snapshot.get("at", "")).strip()
                    _persist_checkpoint()
                    return
                if event_name in {"plugin_finish", "plugin_skip"}:
                    current_state["current_plugin"] = ""
                    current_state["current_plugin_started_at"] = ""
                    _persist_checkpoint()
                    return
                if event_name in {"project_finish", "project_error"}:
                    combined_processed = list(stage_snapshot.get("processed", []))
                    combined_failures = list(stage_snapshot.get("failures", []))
                    _write_tick_stage_progress(
                        root,
                        portfolio_slug,
                        tick_id,
                        {
                            "name": stage_name,
                            "processed": combined_processed,
                            "failures": combined_failures,
                        },
                    )
                    current_state["current_plugin"] = ""
                    current_state["current_plugin_started_at"] = ""
                    current_state["current_project_slug"] = ""
                    current_state["current_project_title"] = ""
                    current_state["current_mode"] = ""
                    processed_projects[:] = [item["project_slug"] for item in combined_processed]
                    failure_items[:] = list(combined_failures)
                    _persist_checkpoint()
                    return
            stage = _run_project_batch(
                root,
                portfolio_slug,
                selected,
                mode=mode,
                provider_name=provider_name,
                stage_name=stage_name,
                progress=progress,
                on_progress=_persist_partial,
            )
            stages.append(stage)
            _write_tick_stage_progress(root, portfolio_slug, tick_id, stage)
            processed_projects = [
                item["project_slug"]
                for stage_item in stages
                for item in stage_item.get("processed", [])
            ]
            failure_items = [
                item
                for stage_item in stages
                for item in stage_item.get("failures", [])
            ]
            current_state["current_project_slug"] = ""
            current_state["current_project_title"] = ""
            current_state["current_mode"] = ""
            current_state["current_plugin"] = ""
            current_state["current_plugin_started_at"] = ""
            _persist_checkpoint()

        current_state.update(_empty_current_worker_state())
        current_state["current_stage"] = "crossref"
        touched_project_slugs = sorted(dict.fromkeys(processed_projects))
        alert_paths = _build_crossref_alerts(root, portfolio_slug) if touched_project_slugs else []
        _emit(progress, f"[crossref] {len(alert_paths)} alerta(s) ativo(s)")
        stage = {
            "name": "crossref",
            "alert_paths": [str(path) for path in alert_paths],
            "touched_projects": touched_project_slugs,
        }
        stages.append(stage)
        _write_tick_stage_progress(root, portfolio_slug, tick_id, stage)
        _persist_checkpoint()

        current_state.update(_empty_current_worker_state())
        current_state["current_stage"] = "leaderboard"
        leaderboard_path = build_portfolio_leaderboard(root, portfolio_slug)
        _emit(progress, f"[leaderboard] atualizado em {leaderboard_path}")
        stage = {"name": "leaderboard", "path": str(leaderboard_path)}
        stages.append(stage)
        _write_tick_stage_progress(root, portfolio_slug, tick_id, stage)
        _persist_checkpoint()

        status = "completed"
        current_state.update(_empty_current_worker_state())
        _write_tick_event(
            root,
            portfolio_slug,
            tick_id,
            "tick-complete",
            {
                "portfolio": portfolio_slug,
                "completed_at": _utc_now(),
                "stage_order": [stage["name"] for stage in stages],
            },
        )
    except KeyboardInterrupt:
        failure_message = "interrupted"
        status = "failed"
        _emit(progress, f"[tick] interrompido no portfolio `{portfolio_slug}`")
        _write_tick_event(
            root,
            portfolio_slug,
            tick_id,
            "tick-failure",
            {
                "portfolio": portfolio_slug,
                "failed_at": _utc_now(),
                "error": failure_message,
                "stage_order": [stage["name"] for stage in stages],
            },
        )
    except Exception as exc:
        failure_message = str(exc)
        status = "failed"
        _emit(progress, f"[tick] falha no portfolio `{portfolio_slug}`: {failure_message}")
        _write_tick_event(
            root,
            portfolio_slug,
            tick_id,
            "tick-failure",
            {
                "portfolio": portfolio_slug,
                "failed_at": _utc_now(),
                "error": failure_message,
                "stage_order": [stage["name"] for stage in stages],
            },
        )
        try:
            leaderboard_path = build_portfolio_leaderboard(root, portfolio_slug)
            stages.append({"name": "leaderboard", "path": str(leaderboard_path), "partial": True})
        except Exception:
            pass

    processed = [
        item["project_slug"]
        for stage in stages
        for item in stage.get("processed", [])
    ]
    failures = [
        item
        for stage in stages
        for item in stage.get("failures", [])
    ]
    if status != "completed":
        processed = processed_projects or processed
        failures = failure_items or failures
    if status == "completed":
        current_state.update(_empty_current_worker_state())
    checkpoint_path = _write_checkpoint(root, portfolio_slug, processed, failures, current_state=current_state)

    run_path = _portfolio_paths(root, portfolio_slug)["runs"] / f"tick-{tick_id}.json"
    run_payload = {
        "portfolio": portfolio_slug,
        "started_at": started_at,
        "completed_at": _utc_now(),
        "max_projects": max_projects,
        "provider": provider_name or os.environ.get("INVESTIGADOR_AGENT_PROVIDER", "mock"),
        "sync_roster_mode": sync_roster_mode,
        "roster_max_age_hours": _portfolio_roster_max_age_hours(get_portfolio(root, portfolio_slug), roster_max_age_hours),
        "scope": normalized_scope,
        "only_status": normalized_only_status,
        "stage_order": [stage["name"] for stage in stages],
        "stages": stages,
        "processed_projects": processed,
        "failure_count": len(failures) + (1 if failure_message else 0),
        "status": status,
        "checkpoint_path": str(checkpoint_path),
        "error": failure_message,
    }
    _write_json(run_path, run_payload)
    if status == "completed":
        _emit(progress, f"[tick] concluído portfolio `{portfolio_slug}`")
    else:
        _emit(progress, f"[tick] finalizado com falha em `{portfolio_slug}`")
    refresh_cache(root)
    return run_path


@contextmanager
def _portfolio_lock(root: Path, portfolio_slug: str):
    lock_path = root / ".investigador" / "locks" / f"portfolio-{slugify(portfolio_slug)}.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError as exc:
        raise RuntimeError(f"Portfolio worker for '{portfolio_slug}' is already running.") from exc
    try:
        os.write(fd, json.dumps({"portfolio": portfolio_slug, "pid": os.getpid(), "started_at": _utc_now()}).encode("utf-8"))
        os.close(fd)
        yield lock_path
    finally:
        if lock_path.exists():
            lock_path.unlink()


def _is_recoverable_portfolio_failure(message: str) -> bool:
    text = str(message or "").strip().lower()
    if not text:
        return False
    transient_markers = (
        "timed out",
        "timeout",
        "tempor",
        "certificate",
        "network",
        "connection",
        "429",
        "503",
        "name or service not known",
        "temporary failure",
        "urlopen error",
        "remote end closed",
        "ssl",
    )
    return any(marker in text for marker in transient_markers)


def run_portfolio(
    root: Path,
    slug: str,
    *,
    loop: bool = False,
    max_projects: int = 200,
    sleep_seconds: int = 300,
    max_concurrent: int = 4,
    provider_name: str | None = None,
    sync_roster_mode: str = "auto",
    roster_max_age_hours: int | None = None,
    scope: str = "all",
    only_status: str = "all",
    progress: ProgressFn | None = None,
) -> Path:
    portfolio = get_portfolio(root, slug)
    portfolio_slug = portfolio.frontmatter["portfolio_slug"]
    last_path: Path | None = None
    consecutive_failures = 0
    with _portfolio_lock(root, portfolio_slug):
        _emit(progress, f"[run] worker iniciado para `{portfolio_slug}`")
        try:
            while True:
                last_path = portfolio_tick(
                    root,
                    portfolio_slug,
                    max_projects=max_projects,
                    provider_name=provider_name,
                    max_concurrent=max_concurrent,
                    sync_roster_mode=sync_roster_mode,
                    roster_max_age_hours=roster_max_age_hours,
                    scope=scope,
                    only_status=only_status,
                    progress=progress,
                )
                try:
                    tick_payload = json.loads(last_path.read_text(encoding="utf-8"))
                except Exception:
                    tick_payload = {}
                if tick_payload.get("status") == "failed":
                    consecutive_failures += 1
                    _emit(progress, f"[run] tick falhou em `{portfolio_slug}`: {tick_payload.get('error', 'erro não identificado')}")
                else:
                    consecutive_failures = 0
                if not loop:
                    break
                if tick_payload.get("status") == "failed" and not _is_recoverable_portfolio_failure(tick_payload.get("error", "")):
                    _emit(progress, f"[run] falha não recuperável em `{portfolio_slug}`; encerrando loop para evitar repetição cega")
                    break
                delay = max(1, sleep_seconds)
                if consecutive_failures:
                    delay = min(max(30, sleep_seconds) * (2 ** min(consecutive_failures - 1, 4)), 3600)
                    _emit(progress, f"[run] retry do worker em {delay} segundo(s) após falha consecutiva #{consecutive_failures}")
                else:
                    _emit(progress, f"[run] dormindo por {delay} segundo(s)")
                time.sleep(delay)
        except KeyboardInterrupt:
            _emit(progress, f"[run] worker interrompido para `{portfolio_slug}`")
    if last_path is None:
        raise RuntimeError("Portfolio worker finished without producing a tick run.")
    _emit(progress, f"[run] worker finalizado para `{portfolio_slug}`")
    return last_path
