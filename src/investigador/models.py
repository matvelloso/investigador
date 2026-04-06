from __future__ import annotations

from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any


SOURCE_CLASSES = {
    "official_structured",
    "official_document",
    "contextual_web",
}

EVIDENCE_ROLES = {
    "identity_baseline",
    "investigative_signal",
    "contextual_lead",
}

EVIDENCE_LAYERS = {
    "identity_baseline",
    "contextual_lead",
    "investigative_signal",
    "case_support",
}

IDENTITY_RESOLUTION_STATUSES = {
    "",
    "confirmed_identity_match",
    "possible_identity_match",
    "rejected_homonym",
}

AGENT_ROLES = {
    "orchestrator",
    "entity_resolver",
    "collector_analyst",
    "skeptic",
    "dossier_writer",
}

ENTITY_TYPES = {
    "person": "people",
    "organization": "organizations",
    "law": "laws",
    "hypothesis": "hypotheses",
}

PRIORITY_LEVELS = (
    "pista",
    "anomalia_consistente",
    "alta_prioridade_investigativa",
)

DEFAULT_WAVE_ONE_PLUGINS = [
    "tse",
    "cnpj-qsa",
    "pncp",
    "datajud",
    "dou",
]

DEFAULT_WAVE_TWO_PLUGINS = [
    "portal-transparencia",
    "transferegov",
    "tcu",
    "querido-diario",
]

DEFAULT_CONTEXTUAL_PLUGINS = [
    "web-search",
]

BROAD_FACT_PLUGINS = [
    "tse",
    "cnpj-qsa",
    "dou",
]

DEFAULT_PORTFOLIO_BASELINE_PLUGINS = [
    "tse",
    "dou",
    "datajud",
]

DEFAULT_PORTFOLIO_DEEP_PLUGINS = [
    *DEFAULT_WAVE_ONE_PLUGINS,
    *DEFAULT_WAVE_TWO_PLUGINS,
]

FEDERAL_CAMARA_PLUGINS = [
    "camara-profile",
    "camara-expenses",
    "camara-organs",
]

UF_CODES = (
    "AC",
    "AL",
    "AP",
    "AM",
    "BA",
    "CE",
    "DF",
    "ES",
    "GO",
    "MA",
    "MT",
    "MS",
    "MG",
    "PA",
    "PB",
    "PR",
    "PE",
    "PI",
    "RJ",
    "RN",
    "RS",
    "RO",
    "RR",
    "SC",
    "SP",
    "SE",
    "TO",
)


@dataclass(slots=True)
class Note:
    path: Path
    frontmatter: dict[str, Any]
    body: str
    storage_format: str = "footer"

    def copy(self) -> "Note":
        return Note(self.path, dict(self.frontmatter), self.body, self.storage_format)


@dataclass(slots=True)
class SourceReference:
    plugin: str
    source_name: str
    record_id: str
    url: str
    collected_at: str
    query: str = ""
    domain: str = ""
    publisher: str = ""
    published_at: str = ""
    retrieved_from: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {key: value for key, value in asdict(self).items() if value not in ("", None, [], {})}


@dataclass(slots=True)
class ProposedEntity:
    entity_type: str
    identifier: str
    name: str
    relation: str
    confidence: float = 0.6
    notes: str = ""
    source_refs: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class EvidenceRecord:
    plugin: str
    source_name: str
    source_class: str
    record_id: str
    url: str
    title: str
    claim: str
    excerpt: str
    related_ids: list[str]
    chronology_date: str
    confidence: float
    evidence_role: str = ""
    evidence_layer: str = ""
    identity_resolution_status: str = ""
    case_relevance: int = 0
    supports_hypothesis_ids: list[str] = field(default_factory=list)
    supports_hypothesis_types: list[str] = field(default_factory=list)
    proposed_entities: list[ProposedEntity] = field(default_factory=list)
    metadata_updates: dict[str, Any] = field(default_factory=dict)
    source_metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["proposed_entities"] = [item.to_dict() for item in self.proposed_entities]
        return data


@dataclass(slots=True)
class EvidenceBundle:
    plugin: str
    records: list[EvidenceRecord]
    next_queries: list[str] = field(default_factory=list)
    proposed_links: list[dict[str, Any]] = field(default_factory=list)
    artifacts: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "plugin": self.plugin,
            "records": [record.to_dict() for record in self.records],
            "next_queries": list(self.next_queries),
            "proposed_links": list(self.proposed_links),
            "artifacts": list(self.artifacts),
        }


@dataclass(slots=True)
class ProposedChange:
    action: str
    note_type: str
    payload: dict[str, Any]
    rationale: str
    confidence: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class AgentRunResult:
    provider: str
    role: str
    content: str
    mode: str
    proposed_changes: list[ProposedChange] = field(default_factory=list)
    raw_payload: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "provider": self.provider,
            "role": self.role,
            "content": self.content,
            "mode": self.mode,
            "proposed_changes": [change.to_dict() for change in self.proposed_changes],
            "raw_payload": dict(self.raw_payload),
        }


@dataclass(slots=True)
class RosterMember:
    scope: str
    uf: str
    source_plugin: str
    source_member_id: str
    full_name: str
    parliamentary_name: str
    party: str
    status: str
    roster_url: str
    roster_confidence: float = 0.0
    roster_validated: bool = True
    roster_source_kind: str = "official_html"
    aliases: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class RosterResult:
    plugin: str
    members: list[RosterMember]
    source_url: str
    artifacts: list[dict[str, Any]] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "plugin": self.plugin,
            "members": [member.to_dict() for member in self.members],
            "source_url": self.source_url,
            "artifacts": list(self.artifacts),
            "errors": list(self.errors),
        }


@dataclass(slots=True)
class WorkerCheckpoint:
    last_tick_at: str
    next_tick_after: str
    portfolio_slug: str
    active_lock_path: str = ""
    processed_projects: list[str] = field(default_factory=list)
    failures: list[dict[str, Any]] = field(default_factory=list)
    current_stage: str = ""
    current_project_slug: str = ""
    current_project_title: str = ""
    current_mode: str = ""
    current_plugin: str = ""
    current_plugin_started_at: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
