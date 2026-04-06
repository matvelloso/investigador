from __future__ import annotations

import csv
import hashlib
import html
import io
import json
import os
import re
import unicodedata
import xml.etree.ElementTree as ET
import zipfile
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable
from urllib import parse

from .http import HttpResponse, fetch_bytes, fetch_json, fetch_text
from .messages import msg
from .models import EvidenceBundle, EvidenceRecord, Note, ProposedEntity, SourceReference


def _utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def _slug_fragment(text: str) -> str:
    digest = hashlib.sha1(text.encode("utf-8")).hexdigest()
    return digest[:10]


def _digits(value: str | None) -> str:
    return "".join(char for char in str(value or "") if char.isdigit())


def _normalize_text(text: str | None) -> str:
    value = unicodedata.normalize("NFKD", str(text or ""))
    value = "".join(char for char in value if not unicodedata.combining(char))
    return re.sub(r"\s+", " ", value).strip().lower()


def _dedupe(values: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        if not value:
            continue
        if value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return ordered


def _coerce_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value if str(item).strip()]
    return [str(value)]


def _looks_like_cnpj(value: str | None) -> bool:
    return len(_digits(value)) == 14


def _looks_like_cpf(value: str | None) -> bool:
    return len(_digits(value)) == 11


def _safe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    text = str(value).strip().replace(".", "").replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return None


def _pick_value(mapping: dict[str, Any], *names: str) -> str:
    lowered = {str(key).strip().lower(): value for key, value in mapping.items()}
    for name in names:
        if name in mapping:
            return str(mapping[name] or "")
        lowered_name = name.lower()
        if lowered_name in lowered:
            return str(lowered[lowered_name] or "")
    return ""


def _as_list_rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        for key in ("items", "data", "resultados", "results", "rows", "content"):
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
        if "hits" in payload and isinstance(payload["hits"], dict):
            hits = payload["hits"].get("hits")
            if isinstance(hits, list):
                rows: list[dict[str, Any]] = []
                for item in hits:
                    if isinstance(item, dict):
                        rows.append(item.get("_source", item))
                return rows
    return []


def _text_snippet(text: str, terms: list[str], radius: int = 200) -> str:
    collapsed = re.sub(r"\s+", " ", text)
    normalized = _normalize_text(collapsed)
    for term in terms:
        needle = _normalize_text(term)
        if not needle:
            continue
        index = normalized.find(needle)
        if index == -1:
            continue
        start = max(index - radius // 2, 0)
        end = min(index + len(needle) + radius // 2, len(collapsed))
        return collapsed[start:end].strip()
    return collapsed[:radius].strip()


def _decode_text(raw: bytes) -> str:
    for encoding in ("utf-8-sig", "utf-8", "latin-1", "cp1252"):
        try:
            return raw.decode(encoding)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="replace")


def _iter_zip_rows(payload: bytes) -> Iterable[tuple[str, dict[str, str]]]:
    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        for member in archive.namelist():
            if not member.lower().endswith((".csv", ".txt")):
                continue
            raw = archive.read(member)
            text = _decode_text(raw)
            lines = text.splitlines()
            if not lines:
                continue
            delimiter = ";" if lines[0].count(";") >= lines[0].count(",") else ","
            reader = csv.DictReader(io.StringIO(text), delimiter=delimiter)
            for row in reader:
                yield member, {str(key).strip().lstrip("\ufeff"): str(value or "").strip() for key, value in row.items()}


def _iter_zip_xml(payload: bytes) -> Iterable[tuple[str, str, str]]:
    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        for member in archive.namelist():
            if not member.lower().endswith(".xml"):
                continue
            raw = archive.read(member)
            try:
                root = ET.fromstring(raw)
            except ET.ParseError:
                continue
            text = " ".join(piece.strip() for piece in root.itertext() if piece and piece.strip())
            title = ""
            for element in root.iter():
                tag = element.tag.split("}", 1)[-1].lower()
                if tag in {"identifica", "titulo", "title"} and element.text:
                    title = element.text.strip()
                    break
            yield member, title or member, text


def _strip_tags(text: str) -> str:
    without_tags = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", html.unescape(without_tags)).strip()


def _decode_search_url(url: str) -> str:
    if not url:
        return ""
    if url.startswith("//"):
        url = "https:" + url
    parsed = parse.urlsplit(url)
    if "duckduckgo.com" in parsed.netloc or "html.duckduckgo.com" in parsed.netloc:
        uddg = parse.parse_qs(parsed.query).get("uddg")
        if uddg:
            return parse.unquote(uddg[0])
    return html.unescape(url)


@dataclass(slots=True)
class PluginContext:
    root: Path
    project: Note
    targets: list[Note]
    entities: dict[str, Note]


@dataclass(slots=True)
class TargetProfile:
    target: Note
    entity: Note
    metadata: dict[str, Any]
    aliases: list[str]

    @property
    def canonical_id(self) -> str:
        return self.target.frontmatter["canonical_id"]

    @property
    def entity_type(self) -> str:
        return self.target.frontmatter.get("entity_type", self.entity.frontmatter.get("entity_type", ""))

    @property
    def identifier(self) -> str:
        return self.target.frontmatter.get("identifier", "")

    @property
    def title(self) -> str:
        return self.target.frontmatter.get("title", self.entity.frontmatter.get("title", self.identifier))

    @property
    def search_terms(self) -> list[str]:
        values = _dedupe([self.title, self.identifier, *self.aliases])
        return [value for value in values if value and not value.isdigit()]

    @property
    def cnpjs(self) -> list[str]:
        candidates = [self.identifier, *self.aliases]
        candidates.extend(_coerce_list(self.metadata.get("cnpjs")))
        explicit = self.metadata.get("cnpj")
        if explicit:
            candidates.append(str(explicit))
        return _dedupe([_digits(item) for item in candidates if _looks_like_cnpj(item)])

    @property
    def cpfs(self) -> list[str]:
        candidates = [self.identifier, *self.aliases]
        candidates.extend(_coerce_list(self.metadata.get("cpfs")))
        explicit = self.metadata.get("cpf")
        if explicit:
            candidates.append(str(explicit))
        return _dedupe([_digits(item) for item in candidates if _looks_like_cpf(item)])

    @property
    def territory_ids(self) -> list[str]:
        candidates = _coerce_list(self.metadata.get("territory_ids"))
        municipality_ibge = self.metadata.get("municipality_ibge") or self.metadata.get("ibge")
        if municipality_ibge:
            candidates.append(str(municipality_ibge))
        return _dedupe([_digits(item) for item in candidates if _digits(item)])

    @property
    def election_year(self) -> int:
        return self.election_years[0]

    @property
    def election_years(self) -> list[int]:
        explicit_years = _coerce_list(self.metadata.get("election_years"))
        explicit = self.metadata.get("election_year") or self.metadata.get("year")
        values: list[int] = []
        for raw in [*explicit_years, explicit]:
            if raw in (None, ""):
                continue
            try:
                year = int(raw)
            except (TypeError, ValueError):
                continue
            if year not in values:
                values.append(year)
        if values:
            return values
        current_year = datetime.now(UTC).year
        if current_year % 2:
            current_year -= 1
        return [year for year in range(current_year, max(current_year - 8, 1994), -2)]

    @property
    def broad_fact_hint(self) -> str:
        value = self.metadata.get("election_year") or self.metadata.get("year")
        if value not in (None, ""):
            return f"ano eleitoral {value}"
        if self.entity_type == "person":
            return "cargo, partido, UF e identificador eleitoral"
        if self.entity_type == "organization":
            return "situação cadastral, quadro societário e jurisdição"
        return "identificadores públicos e cronologia básica"

    @property
    def tribunal_aliases(self) -> list[str]:
        aliases = _coerce_list(self.metadata.get("tribunal_aliases"))
        aliases.extend(_coerce_list(self.metadata.get("tribunal_alias")))
        aliases.extend(_coerce_list(self.metadata.get("datajud_aliases")))
        return _dedupe(aliases)


class SourcePlugin:
    name = "base"
    source_name = "Base Source"
    source_class = "official_structured"
    evidence_role = "identity_baseline"

    def discover(self, context: PluginContext) -> list[str]:
        return [target.frontmatter["id"] for target in context.targets]

    def collect(self, context: PluginContext) -> EvidenceBundle:
        raise NotImplementedError

    def normalize(self, context: PluginContext, bundle: EvidenceBundle) -> EvidenceBundle:
        default_role = self.evidence_role
        if not default_role and self.source_class == "contextual_web":
            default_role = "contextual_lead"
        for record in bundle.records:
            if not record.evidence_role:
                record.evidence_role = default_role
            if not record.evidence_layer:
                if record.evidence_role == "investigative_signal":
                    record.evidence_layer = "investigative_signal"
                elif record.evidence_role == "contextual_lead":
                    record.evidence_layer = "contextual_lead"
                else:
                    record.evidence_layer = "identity_baseline"
        return bundle

    def run(self, context: PluginContext) -> EvidenceBundle:
        return self.normalize(context, self.collect(context))

    def _profiles(self, context: PluginContext) -> list[TargetProfile]:
        project_metadata = context.project.frontmatter.get("metadata", {})
        profiles: list[TargetProfile] = []
        for target in context.targets:
            entity = context.entities[target.frontmatter["canonical_id"]]
            metadata = dict(project_metadata)
            metadata.update(entity.frontmatter.get("metadata", {}))
            metadata.update(target.frontmatter.get("metadata", {}))
            aliases = _dedupe(
                [
                    *target.frontmatter.get("aliases", []),
                    *entity.frontmatter.get("aliases", []),
                    entity.frontmatter.get("title", ""),
                ]
            )
            profiles.append(TargetProfile(target=target, entity=entity, metadata=metadata, aliases=aliases))
        return profiles

    def _reference(self, record_id: str, url: str, **extras: str) -> dict[str, str]:
        return SourceReference(
            plugin=self.name,
            source_name=self.source_name,
            record_id=record_id,
            url=url,
            collected_at=_utc_now(),
            **extras,
        ).to_dict()

    def _artifact_json(self, name: str, source_url: str, payload: Any) -> dict[str, Any]:
        return {
            "name": name,
            "filename": f"{name}.json",
            "content_type": "application/json",
            "source_url": source_url,
            "json": payload,
        }

    def _artifact_text(self, name: str, source_url: str, payload: str) -> dict[str, Any]:
        return {
            "name": name,
            "filename": f"{name}.txt",
            "content_type": "text/plain",
            "source_url": source_url,
            "text": payload,
        }

    def _cache_bytes(self, context: PluginContext, url: str, *, headers: dict[str, str] | None = None) -> tuple[bytes, str]:
        suffix = Path(url.split("?", 1)[0]).suffix or ".bin"
        cache_root = context.root / ".investigador" / "cache" / self.name
        cache_root.mkdir(parents=True, exist_ok=True)
        cache_path = cache_root / f"{_slug_fragment(url)}{suffix}"
        if cache_path.exists():
            return cache_path.read_bytes(), str(cache_path.relative_to(context.root))
        response = fetch_bytes(url, headers=headers)
        cache_path.write_bytes(response.content)
        return response.content, str(cache_path.relative_to(context.root))


class SyntheticPublicDataPlugin(SourcePlugin):
    def _build_record(self, target: Note, claim_suffix: str, excerpt: str, proposed: list[ProposedEntity] | None = None) -> EvidenceRecord:
        entity_id = target.frontmatter["canonical_id"]
        identifier = target.frontmatter["identifier"]
        record_id = f"{self.name}-{_slug_fragment(entity_id + identifier)}"
        title = f"{self.source_name}: {target.frontmatter['title']}"
        claim = f"{target.frontmatter['title']} {claim_suffix}"
        url = f"https://example.invalid/{self.name}/{record_id}"
        return EvidenceRecord(
            plugin=self.name,
            source_name=self.source_name,
            source_class=self.source_class,
            record_id=record_id,
            url=url,
            title=title,
            claim=claim,
            excerpt=excerpt,
            related_ids=[entity_id],
            chronology_date=_utc_now().split("T", 1)[0],
            confidence=0.72,
            proposed_entities=proposed or [],
        )


class MockPlugin(SyntheticPublicDataPlugin):
    name = "mock"
    source_name = "Investigador Mock Source"
    source_class = "official_structured"
    evidence_role = "investigative_signal"

    def collect(self, context: PluginContext) -> EvidenceBundle:
        records: list[EvidenceRecord] = []
        for target in context.targets:
            canonical = context.entities[target.frontmatter["canonical_id"]]
            organization_name = f"Fornecedor ligado a {canonical.frontmatter['title']}"
            proposed = ProposedEntity(
                entity_type="organization",
                identifier=f"org:{_slug_fragment(organization_name)}",
                name=organization_name,
                relation="fornecedor recorrente em revisão inicial",
                confidence=0.63,
                source_refs=[self._reference("mock-org", "https://example.invalid/mock-org")],
            )
            records.append(
                self._build_record(
                    target,
                    "aparece em uma coleta sintética para validar o pipeline investigativo",
                    "Registro sintético de teste com proveniência suficiente para o MVP.",
                    [proposed],
                )
            )
        return EvidenceBundle(plugin=self.name, records=records, next_queries=["Executar fontes oficiais prioritárias."])


class TSEPlugin(SourcePlugin):
    name = "tse"
    source_name = "Tribunal Superior Eleitoral"
    source_class = "official_structured"
    evidence_role = "identity_baseline"

    def collect(self, context: PluginContext) -> EvidenceBundle:
        records: list[EvidenceRecord] = []
        artifacts: list[dict[str, Any]] = []
        next_queries: list[str] = []
        seen_record_ids: set[str] = set()
        search_url = os.environ.get(
            "INVESTIGADOR_TSE_CKAN_SEARCH_URL",
            "https://dadosabertos.tse.jus.br/api/3/action/package_search",
        )
        for profile in self._profiles(context):
            cycle_matches = 0
            searched_years: list[int] = []
            for year in profile.election_years:
                searched_years.append(year)
                packages: list[dict[str, Any]] = []
                for query_text in self._search_queries(profile, year):
                    search_payload, search_response = fetch_json(search_url, query={"q": query_text, "rows": 20})
                    artifacts.append(
                        self._artifact_json(
                            f"tse-package-search-{profile.canonical_id}-{year}-{_slug_fragment(query_text)}",
                            search_response.url,
                            search_payload,
                        )
                    )
                    found_packages = search_payload.get("result", {}).get("results", []) if isinstance(search_payload, dict) else []
                    for candidate in found_packages:
                        if candidate not in packages:
                            packages.append(candidate)
                package = self._pick_tse_package(packages, year)
                if not package:
                    continue
                resource = self._pick_tse_resource(package.get("resources", []), year)
                if not resource:
                    continue
                content, cache_path = self._cache_bytes(context, resource["url"])
                classified_matches = self._search_tse_rows(content, profile)
                confirmed = [
                    match for match in classified_matches if match.get("identity_resolution_status") == "confirmed_identity_match"
                ]
                possible = [
                    match for match in classified_matches if match.get("identity_resolution_status") == "possible_identity_match"
                ]
                rejected = [
                    match for match in classified_matches if match.get("identity_resolution_status") == "rejected_homonym"
                ]
                artifacts.append(
                    self._artifact_json(
                        f"tse-resource-{_slug_fragment(resource.get('url', ''))}",
                        resource.get("url", ""),
                        {
                            "cache_path": cache_path,
                            "confirmed_matches": confirmed[:5],
                            "possible_matches": possible[:5],
                            "rejected_matches": rejected[:5],
                            "package": package.get("name", ""),
                        },
                    )
                )
                if not confirmed:
                    continue
                cycle_matches += len(confirmed[:3])
                for match in confirmed[:3]:
                    row = match["row"]
                    candidate_name = _pick_value(row, "NM_CANDIDATO", "nm_candidato") or profile.title
                    ballot_name = _pick_value(row, "NM_URNA_CANDIDATO", "nm_urna_candidato")
                    cargo = _pick_value(row, "DS_CARGO", "descricao_cargo") or "cargo não identificado"
                    partido = _pick_value(row, "SG_PARTIDO", "sg_partido")
                    uf = _pick_value(row, "SG_UF", "sg_ue")
                    situation = _pick_value(row, "DS_SIT_TOT_TURNO", "DS_DETALHE_SITUACAO_CAND")
                    sq_candidato = _pick_value(row, "SQ_CANDIDATO", "NR_CANDIDATO") or match["row_index"]
                    claim = f"{candidate_name} consta na base de candidatos do TSE de {year} para {cargo}"
                    if partido:
                        claim += f" pelo partido {partido}"
                    if uf:
                        claim += f" em {uf}"
                    excerpt = f"Situação: {situation or 'não informada'}. Identificador TSE: {sq_candidato}."
                    proposed_entities: list[ProposedEntity] = []
                    if partido:
                        proposed_entities.append(
                            ProposedEntity(
                                entity_type="organization",
                                identifier=f"party:{partido}",
                                name=partido,
                                relation="partido informado em candidatura do TSE",
                                confidence=0.82,
                                source_refs=[self._reference(str(sq_candidato), resource["url"])],
                            )
                        )
                    metadata_updates = {
                        "broad_fact_status": "seeded_from_tse",
                        "election_year": year,
                        "tse_dataset": package.get("name") or f"candidatos-{year}",
                        "tse_candidate_id": str(sq_candidato),
                    }
                    if cargo and cargo != "cargo não identificado":
                        metadata_updates["office"] = cargo
                    if partido:
                        metadata_updates["party"] = partido
                    if uf:
                        metadata_updates["uf"] = uf
                    if ballot_name:
                        metadata_updates["ballot_name"] = ballot_name
                    record_id = f"{profile.canonical_id}-{sq_candidato}-{year}"
                    if record_id in seen_record_ids:
                        continue
                    seen_record_ids.add(record_id)
                    records.append(
                        EvidenceRecord(
                            plugin=self.name,
                            source_name=self.source_name,
                            source_class=self.source_class,
                            record_id=record_id,
                            url=resource["url"],
                            title=f"TSE candidatura: {candidate_name}",
                            claim=claim,
                            excerpt=excerpt,
                            related_ids=[profile.canonical_id],
                            chronology_date=f"{year}-01-01",
                            confidence=0.86,
                            evidence_layer="identity_baseline",
                            identity_resolution_status=str(match.get("identity_resolution_status", "confirmed_identity_match")),
                            case_relevance=32,
                            proposed_entities=proposed_entities,
                            metadata_updates=metadata_updates,
                        )
                    )
            if cycle_matches == 0:
                years_text = ", ".join(str(year) for year in searched_years[:5])
                next_queries.append(
                    f"TSE: sem correspondência confirmada para {profile.title} nos ciclos {years_text}; informe `metadata.election_year`, `metadata.tse_dataset`, CPF público, nome civil completo ou identificador do candidato."
                )
        return EvidenceBundle(plugin=self.name, records=records, next_queries=_dedupe(next_queries), artifacts=artifacts)

    def _search_query(self, profile: TargetProfile, year: int) -> str:
        explicit = str(profile.metadata.get("tse_dataset", "")).strip()
        if explicit:
            return explicit
        return f"consulta_cand_{year}"

    def _search_queries(self, profile: TargetProfile, year: int) -> list[str]:
        explicit = str(profile.metadata.get("tse_dataset", "")).strip()
        if explicit:
            return [explicit]
        return _dedupe(
            [
                self._search_query(profile, year),
                f"consulta_cand {year}",
                f"candidatos {year}",
            ]
        )

    def _pick_tse_package(self, packages: list[dict[str, Any]], year: int) -> dict[str, Any] | None:
        ranked = sorted(
            packages,
            key=lambda item: self._tse_package_score(item, year),
            reverse=True,
        )
        if not ranked:
            return None
        best = ranked[0]
        if self._tse_package_score(best, year) <= 0:
            return None
        return best

    def _tse_package_score(self, package: dict[str, Any], year: int) -> int:
        package_text = _normalize_text(
            " ".join(
                str(package.get(key, ""))
                for key in ("name", "title", "notes")
            )
        )
        resource_text = _normalize_text(
            " ".join(
                " ".join(str(resource.get(key, "")) for key in ("name", "description", "format"))
                for resource in package.get("resources", [])
            )
        )
        score = 0
        if f"consulta_cand_{year}" in package_text or "consulta_cand" in package_text:
            score += 50
        if f"/consulta_cand/" in resource_text or f"consulta_cand_{year}" in resource_text:
            score += 50
        if f"candidatos-{year}" in package_text or f"candidatos {year}" in package_text:
            score += 30
        if f"consulta_cand_{year}" in resource_text or f"consulta cand {year}" in resource_text:
            score += 25
        if "consulta_cand" in resource_text:
            score += 12
        if "candid" in package_text:
            score += 10
        if str(year) in package_text or str(year) in resource_text:
            score += 8
        if self._pick_tse_resource(package.get("resources", []), year) is not None:
            score += 5
        for penalty in (
            "prestacao_contas",
            "prestacao de contas",
            "transferencia-do-eleitorado",
            "transferencia do eleitorado",
            "eleitorado",
            "votacao",
            "receitas",
            "despesas",
        ):
            if penalty in package_text or penalty in resource_text:
                score -= 30
        return score

    def _pick_tse_resource(self, resources: list[dict[str, Any]], year: int) -> dict[str, Any] | None:
        ranked = sorted(
            resources,
            key=lambda item: (
                "consulta_cand" not in _normalize_text(item.get("name") or item.get("description") or item.get("url") or ""),
                str(year) not in _normalize_text(item.get("name") or item.get("description") or item.get("url") or ""),
                "candid" not in _normalize_text(item.get("name") or item.get("description") or item.get("url") or ""),
                "prestacao" in _normalize_text(item.get("name") or item.get("description") or item.get("url") or ""),
                item.get("format") not in {"ZIP", "CSV", "TXT"},
            ),
        )
        for resource in ranked:
            url = resource.get("url")
            if url and resource.get("format", "").upper() in {"ZIP", "CSV", "TXT"}:
                return resource
        return None

    def _name_tokens(self, text: str | None) -> set[str]:
        return {token for token in _normalize_text(text).split() if len(token) >= 3}

    def _name_overlap(self, left: str | None, right: str | None) -> float:
        left_tokens = self._name_tokens(left)
        right_tokens = self._name_tokens(right)
        if not left_tokens or not right_tokens:
            return 0.0
        return len(left_tokens & right_tokens) / max(len(left_tokens), len(right_tokens))

    def _ballot_terms(self, profile: TargetProfile) -> list[str]:
        candidates = [
            profile.title,
            *profile.aliases,
            str(profile.metadata.get("ballot_name", "")).strip(),
            str(profile.metadata.get("civil_name", "")).strip(),
        ]
        terms: list[str] = []
        for candidate in candidates:
            normalized = _normalize_text(candidate)
            if not normalized or len(self._name_tokens(normalized)) < 2:
                continue
            terms.append(normalized)
        return _dedupe(terms)

    def _classify_tse_match(self, row: dict[str, str], profile: TargetProfile) -> tuple[str, int]:
        candidate_name = _pick_value(row, "NM_CANDIDATO", "NM_SOCIAL_CANDIDATO", "nm_candidato")
        candidate_ballot = _pick_value(row, "NM_URNA_CANDIDATO", "nm_urna_candidato")
        candidate_cpf = _digits(_pick_value(row, "NR_CPF_CANDIDATO", "CPF_CANDIDATO"))
        candidate_sq = _digits(_pick_value(row, "SQ_CANDIDATO", "NR_CANDIDATO"))
        candidate_uf = _normalize_text(_pick_value(row, "SG_UF", "SG_UE"))
        candidate_party = _normalize_text(_pick_value(row, "SG_PARTIDO"))
        candidate_office = _normalize_text(_pick_value(row, "DS_CARGO", "descricao_cargo"))
        cpf_set = set(profile.cpfs)
        numeric_id = _digits(profile.identifier)
        profile_uf = _normalize_text(profile.metadata.get("uf"))
        profile_party = _normalize_text(profile.metadata.get("party"))
        profile_office = _normalize_text(profile.metadata.get("office"))
        strong_terms = self._ballot_terms(profile)
        best_name_overlap = max((self._name_overlap(candidate_name, term) for term in strong_terms), default=0.0)
        best_ballot_overlap = max((self._name_overlap(candidate_ballot, term) for term in strong_terms), default=0.0)
        exact_name = any(_normalize_text(candidate_name) == term for term in strong_terms)
        exact_ballot = any(_normalize_text(candidate_ballot) == term for term in strong_terms)
        uf_match = bool(profile_uf and candidate_uf and profile_uf == candidate_uf)
        party_match = bool(profile_party and candidate_party and profile_party == candidate_party)
        office_match = bool(
            profile_office
            and candidate_office
            and (profile_office in candidate_office or candidate_office in profile_office)
        )
        matched_facets = sum([uf_match, party_match, office_match])
        if cpf_set and candidate_cpf in cpf_set:
            return "confirmed_identity_match", 100
        if numeric_id and candidate_sq and numeric_id == candidate_sq:
            return "confirmed_identity_match", 96
        if exact_name and uf_match and office_match:
            return "confirmed_identity_match", 88
        if best_name_overlap >= 0.85 and uf_match and office_match:
            return "confirmed_identity_match", 83
        if (exact_ballot or best_ballot_overlap >= 0.75) and uf_match and office_match and party_match:
            return "confirmed_identity_match", 80
        if exact_name and matched_facets >= 1:
            return "possible_identity_match", 68
        if best_name_overlap >= 0.65 and matched_facets >= 2:
            return "possible_identity_match", 64
        if best_ballot_overlap >= 0.7 and matched_facets >= 2:
            return "possible_identity_match", 60
        if (
            best_name_overlap > 0
            or best_ballot_overlap > 0
            or any(term in _normalize_text(candidate_name) for term in strong_terms if len(term.split()) == 1)
        ):
            return "rejected_homonym", 25
        return "", 0

    def _search_tse_rows(self, payload: bytes, profile: TargetProfile) -> list[dict[str, Any]]:
        matches: list[dict[str, Any]] = []
        cpf_set = set(profile.cpfs)
        numeric_id = _digits(profile.identifier)
        for member, row in _iter_zip_rows(payload):
            candidate_cpf = _digits(_pick_value(row, "NR_CPF_CANDIDATO", "CPF_CANDIDATO"))
            candidate_sq = _digits(_pick_value(row, "SQ_CANDIDATO", "NR_CANDIDATO"))
            resolution_status, match_score = self._classify_tse_match(row, profile)
            if not resolution_status:
                continue
            matches.append(
                {
                    "member": member,
                    "row": row,
                    "row_index": candidate_sq or candidate_cpf or len(matches) + 1,
                    "score": match_score,
                    "identity_resolution_status": resolution_status,
                }
            )
        resolution_rank = {
            "confirmed_identity_match": 3,
            "possible_identity_match": 2,
            "rejected_homonym": 1,
        }
        return sorted(
            matches,
            key=lambda item: (
                resolution_rank.get(str(item.get("identity_resolution_status", "")), 0),
                item["score"],
                str(item["row_index"]),
            ),
            reverse=True,
        )


class CNPJQSAPlugin(SourcePlugin):
    name = "cnpj-qsa"
    source_name = "Cadastro CNPJ / QSA"
    source_class = "contextual_web"
    evidence_role = "identity_baseline"

    def collect(self, context: PluginContext) -> EvidenceBundle:
        records: list[EvidenceRecord] = []
        artifacts: list[dict[str, Any]] = []
        next_queries: list[str] = []
        base_url = os.environ.get("INVESTIGADOR_CNPJ_BASE_URL", "https://brasilapi.com.br/api/cnpj/v1")
        use_official_class = "gov.br" in base_url or "receita" in base_url.lower() or os.environ.get("INVESTIGADOR_CNPJ_SOURCE_CLASS") == "official_structured"
        source_class = "official_structured" if use_official_class else self.source_class
        source_name = "Receita Federal / Base CNPJ" if use_official_class else "BrasilAPI / CNPJ"
        for profile in self._profiles(context):
            if not profile.cnpjs:
                next_queries.append(f"CNPJ/QSA: informe um CNPJ para {profile.title} em `metadata.cnpj` ou no identificador do alvo.")
                continue
            for cnpj in profile.cnpjs:
                payload, response = fetch_json(f"{base_url.rstrip('/')}/{cnpj}")
                artifacts.append(self._artifact_json(f"cnpj-{cnpj}", response.url, payload))
                company_name = payload.get("razao_social") or payload.get("nome_fantasia") or profile.title
                status = payload.get("descricao_situacao_cadastral") or payload.get("descricao_situacao") or "situação não informada"
                city = payload.get("municipio") or payload.get("descricao_municipio")
                uf = payload.get("uf")
                claim = f"{company_name} consta no cadastro CNPJ/QSA com situação {status}"
                if city or uf:
                    claim += f" em {city or ''}/{uf or ''}".strip("/")
                excerpt = f"CNPJ {cnpj}. Natureza jurídica: {payload.get('natureza_juridica', 'não informada')}."
                proposed_entities: list[ProposedEntity] = []
                for member in payload.get("qsa", [])[:8]:
                    partner_name = member.get("nome_socio") or member.get("nome")
                    if not partner_name:
                        continue
                    partner_identifier = _digits(member.get("cnpj_cpf_do_socio") or member.get("cnpj_cpf_socio"))
                    entity_type = "person"
                    ident_type = str(member.get("identificador_de_socio", "")).strip().lower()
                    if ident_type in {"2", "juridica", "pj"} or _looks_like_cnpj(partner_identifier):
                        entity_type = "organization"
                    proposed_entities.append(
                        ProposedEntity(
                            entity_type=entity_type,
                            identifier=partner_identifier or f"{entity_type}:{_slug_fragment(partner_name)}",
                            name=partner_name,
                            relation=member.get("qualificacao_socio") or "integrante do QSA",
                            confidence=0.8,
                            source_refs=[self._reference(cnpj, response.url)],
                        )
                    )
                records.append(
                    EvidenceRecord(
                        plugin=self.name,
                        source_name=source_name,
                        source_class=source_class,
                        record_id=cnpj,
                        url=response.url,
                        title=f"CNPJ/QSA: {company_name}",
                        claim=claim,
                        excerpt=excerpt,
                        related_ids=[profile.canonical_id],
                        chronology_date=payload.get("data_inicio_atividade") or _utc_now().split("T", 1)[0],
                        confidence=0.84 if use_official_class else 0.68,
                        proposed_entities=proposed_entities,
                        metadata_updates={
                            "broad_fact_status": "seeded_from_cnpj_qsa",
                            "cnpj": cnpj,
                            "registration_status": status,
                            "registered_city": city or "",
                            "uf": uf or "",
                            "legal_nature": payload.get("natureza_juridica") or "",
                        },
                    )
                )
        return EvidenceBundle(plugin=self.name, records=records, next_queries=_dedupe(next_queries), artifacts=artifacts)


class PNCPPlugin(SourcePlugin):
    name = "pncp"
    source_name = "Portal Nacional de Contratações Públicas"
    source_class = "official_structured"
    evidence_role = "investigative_signal"

    def collect(self, context: PluginContext) -> EvidenceBundle:
        records: list[EvidenceRecord] = []
        artifacts: list[dict[str, Any]] = []
        next_queries: list[str] = []
        for profile in self._profiles(context):
            cnpjs = profile.cnpjs
            if not cnpjs:
                next_queries.append(f"PNCP: informe o CNPJ do órgão ou fornecedor em `metadata.cnpj` para {profile.title}.")
                continue
            years = _coerce_list(profile.metadata.get("pncp_years")) or [str(profile.metadata.get("pncp_year") or datetime.now(UTC).year)]
            for cnpj in cnpjs:
                for year in years:
                    records.extend(self._collect_pca_records(context, profile, cnpj, str(year), artifacts))
                records.extend(self._collect_publication_records(profile, cnpj, artifacts, next_queries))
        return EvidenceBundle(plugin=self.name, records=records, next_queries=_dedupe(next_queries), artifacts=artifacts)

    def _collect_pca_records(self, context: PluginContext, profile: TargetProfile, cnpj: str, year: str, artifacts: list[dict[str, Any]]) -> list[EvidenceRecord]:
        base_candidates = [
            os.environ.get("INVESTIGADOR_PNCP_PCA_URL"),
            f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/pca/{year}",
            f"https://pncp.gov.br/api/pncp/v1/orgaos/{cnpj}/pca/{year}/json",
        ]
        for candidate in [value for value in base_candidates if value]:
            try:
                payload, response = fetch_json(candidate)
            except Exception:
                continue
            rows = _as_list_rows(payload)
            if not rows and isinstance(payload, dict):
                rows = _as_list_rows(payload.get("items"))
            artifacts.append(self._artifact_json(f"pncp-pca-{cnpj}-{year}", response.url, rows[:10] if rows else payload))
            if not rows:
                return []
            results: list[EvidenceRecord] = []
            for row in rows[:5]:
                item_name = _pick_value(row, "nomeFuturaContratacao", "nome_futura_contratacao", "descricaoItem")
                unit = _pick_value(row, "unidadeResponsavel", "unidade_responsavel")
                claim = f"O órgão {cnpj} publicou item no Plano de Contratações Anual {year}: {item_name or 'item sem descrição'}"
                excerpt = f"Unidade responsável: {unit or 'não informada'}."
                record_id = _digits(_pick_value(row, "idItem", "id_item", "identificadorFuturaContratacao")) or _slug_fragment(item_name or json.dumps(row, ensure_ascii=False))
                results.append(
                    EvidenceRecord(
                        plugin=self.name,
                        source_name=self.source_name,
                        source_class=self.source_class,
                        record_id=f"pca-{cnpj}-{year}-{record_id}",
                        url=response.url,
                        title=f"PNCP PCA {year}: {item_name or profile.title}",
                        claim=claim,
                        excerpt=excerpt,
                        related_ids=[profile.canonical_id],
                        chronology_date=f"{year}-01-01",
                        confidence=0.8,
                    )
                )
            return results
        return []

    def _collect_publication_records(
        self,
        profile: TargetProfile,
        cnpj: str,
        artifacts: list[dict[str, Any]],
        next_queries: list[str],
    ) -> list[EvidenceRecord]:
        base_url = os.environ.get("INVESTIGADOR_PNCP_CONSULTA_URL", "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao")
        date_from = str(profile.metadata.get("date_from") or (date.today() - timedelta(days=180)).isoformat())
        date_to = str(profile.metadata.get("date_to") or date.today().isoformat())
        modalidades = _coerce_list(profile.metadata.get("modalidades_contratacao")) or _coerce_list(profile.metadata.get("codigo_modalidade_contratacao"))
        if not modalidades:
            next_queries.append(
                f"PNCP: para buscar publicações de contratação de {profile.title}, informe `metadata.modalidades_contratacao` com os códigos aplicáveis."
            )
            return []
        rows: list[EvidenceRecord] = []
        for modalidade in modalidades[:5]:
            params = {
                "dataInicial": date_from,
                "dataFinal": date_to,
                "codigoModalidadeContratacao": modalidade,
                "pagina": 1,
                "tamanhoPagina": 5,
            }
            if profile.metadata.get("uf"):
                params["uf"] = profile.metadata["uf"]
            try:
                payload, response = fetch_json(base_url, query=params)
            except Exception:
                continue
            data = _as_list_rows(payload)
            artifacts.append(self._artifact_json(f"pncp-publicacao-{cnpj}-{modalidade}", response.url, payload))
            for row in data:
                orgao = _digits(_pick_value(row, "cnpjOrgao", "orgaoEntidadeCnpj"))
                fornecedor = _digits(_pick_value(row, "cnpjFornecedor", "niFornecedor"))
                if cnpj not in {orgao, fornecedor}:
                    continue
                objeto = _pick_value(row, "objetoCompra", "objeto", "descricao")
                valor = _pick_value(row, "valorTotalEstimado", "valorTotalHomologado", "valor")
                record_id = _pick_value(row, "numeroControlePNCP", "numeroCompra")
                rows.append(
                    EvidenceRecord(
                        plugin=self.name,
                        source_name=self.source_name,
                        source_class=self.source_class,
                        record_id=record_id or _slug_fragment(json.dumps(row, ensure_ascii=False)),
                        url=response.url,
                        title=f"PNCP contratação: {objeto or profile.title}",
                        claim=f"O CNPJ {cnpj} aparece em contratação publicada no PNCP: {objeto or 'objeto não informado'}",
                        excerpt=f"Valor informado: {valor or 'não disponível'}. Modalidade {modalidade}.",
                        related_ids=[profile.canonical_id],
                        chronology_date=_pick_value(row, "dataPublicacaoPncp", "dataPublicacao", "dataInclusao") or date.today().isoformat(),
                        confidence=0.82,
                    )
                )
        return rows


class DataJudPlugin(SourcePlugin):
    name = "datajud"
    source_name = "CNJ DataJud"
    source_class = "official_structured"
    evidence_role = "investigative_signal"

    def collect(self, context: PluginContext) -> EvidenceBundle:
        records: list[EvidenceRecord] = []
        artifacts: list[dict[str, Any]] = []
        next_queries: list[str] = []
        api_key = self._resolve_api_key(artifacts)
        if not api_key:
            next_queries.append("DataJud: defina `INVESTIGADOR_DATAJUD_API_KEY` para consultas estruturadas ou permita a descoberta automática da chave pública.")
            return EvidenceBundle(plugin=self.name, records=records, next_queries=next_queries, artifacts=artifacts)
        aliases = _dedupe(
            _coerce_list(os.environ.get("INVESTIGADOR_DATAJUD_ALIASES", ""))
            + _coerce_list(context.project.frontmatter.get("metadata", {}).get("tribunal_aliases"))
        ) or ["api_publica_tse", "api_publica_stj"]
        base_url = os.environ.get("INVESTIGADOR_DATAJUD_BASE_URL", "https://api-publica.datajud.cnj.jus.br")
        for profile in self._profiles(context):
            before_count = len(records)
            search_aliases = profile.tribunal_aliases or aliases
            for alias in search_aliases[:4]:
                payload = {
                    "size": 5,
                    "query": {
                        "query_string": {
                            "query": self._query_string(profile),
                            "default_operator": "AND",
                        }
                    },
                }
                try:
                    response_payload, response = fetch_json(
                        f"{base_url.rstrip('/')}/{alias}/_search",
                        headers={"Authorization": f"APIKey {api_key}", "Content-Type": "application/json"},
                        method="POST",
                        data=json.dumps(payload).encode("utf-8"),
                    )
                except Exception:
                    continue
                artifacts.append(self._artifact_json(f"datajud-{alias}-{_slug_fragment(profile.title)}", response.url, response_payload))
                for row in _as_list_rows(response_payload):
                    process_number = _pick_value(row, "numeroProcesso", "numero_processo")
                    class_name = _pick_value(row, "classe.nome", "classeProcessual", "classe")
                    court = _pick_value(row, "tribunal", "nomeTribunal", "orgaoJulgador.nome")
                    excerpt = _pick_value(row, "formato", "texto", "assuntos") or _dict_excerpt(row)
                    records.append(
                        EvidenceRecord(
                            plugin=self.name,
                            source_name=self.source_name,
                            source_class=self.source_class,
                            record_id=f"{alias}-{process_number or _slug_fragment(excerpt)}",
                            url=response.url,
                            title=f"DataJud: {process_number or profile.title}",
                            claim=f"Consulta DataJud encontrou referência possivelmente relacionada a {profile.title} no índice {alias}",
                            excerpt=f"Processo: {process_number or 'não identificado'}. Classe: {class_name or 'não informada'}. Tribunal/órgão: {court or alias}. {excerpt[:220]}",
                            related_ids=[profile.canonical_id],
                            chronology_date=_pick_value(row, "dataAjuizamento", "dataHoraUltimaAtualizacao") or date.today().isoformat(),
                            confidence=0.74,
                        )
                    )
            if len(records) == before_count:
                next_queries.append(
                    f"DataJud: sem resultados claros para {profile.title}; refine `metadata.tribunal_aliases` e confirme {profile.broad_fact_hint} antes de aprofundar hipóteses."
                )
        return EvidenceBundle(plugin=self.name, records=records, next_queries=_dedupe(next_queries), artifacts=artifacts)

    def _query_string(self, profile: TargetProfile) -> str:
        if profile.cpfs:
            return profile.cpfs[0]
        if profile.identifier.isdigit():
            return profile.identifier
        return " AND ".join(f'"{term}"' for term in profile.search_terms[:3]) or f'"{profile.title}"'

    def _resolve_api_key(self, artifacts: list[dict[str, Any]]) -> str:
        explicit = os.environ.get("INVESTIGADOR_DATAJUD_API_KEY") or os.environ.get("DATAJUD_API_KEY")
        if explicit:
            return explicit
        docs_url = os.environ.get("INVESTIGADOR_DATAJUD_ACCESS_DOC_URL", "https://datajud-wiki.cnj.jus.br/api-publica/acesso/")
        try:
            text, response = fetch_text(docs_url)
        except Exception:
            return ""
        artifacts.append(self._artifact_text("datajud-access-doc", response.url, text[:2000]))
        match = re.search(r"APIKey\s+([A-Za-z0-9._-]+)", text)
        return match.group(1) if match else ""


class DOUPlugin(SourcePlugin):
    name = "dou"
    source_name = "Diário Oficial da União / INLABS"
    source_class = "official_document"
    evidence_role = "identity_baseline"

    def collect(self, context: PluginContext) -> EvidenceBundle:
        records: list[EvidenceRecord] = []
        artifacts: list[dict[str, Any]] = []
        next_queries: list[str] = []
        base_url = os.environ.get("INVESTIGADOR_DOU_BASE_URL", "https://inlabs.in.gov.br/index.php")
        cookie = os.environ.get("INVESTIGADOR_DOU_COOKIE", os.environ.get("INLABS_COOKIE", ""))
        sections = _coerce_list(context.project.frontmatter.get("metadata", {}).get("dou_sections")) or ["DO1", "DO2", "DO3"]
        start_date, end_date = _resolve_date_window(context.project.frontmatter.get("metadata", {}))
        for current in _date_range(start_date, end_date):
            for section in sections:
                params = {"p": current.isoformat(), "dl": f"{current.isoformat()}-{section}.zip"}
                headers = {"Origem": "736372697074"}
                if cookie:
                    headers["Cookie"] = cookie
                try:
                    response = fetch_bytes(base_url, query=params, headers=headers, timeout=45)
                except Exception:
                    continue
                artifacts.append(self._artifact_json(f"dou-download-{current.isoformat()}-{section}", response.url, {"status": response.status, "size": len(response.content)}))
                if not response.content:
                    continue
                if not zipfile.is_zipfile(io.BytesIO(response.content)):
                    preview = response.text()[:500]
                    artifacts.append(
                        self._artifact_text(
                            f"dou-nonzip-{current.isoformat()}-{section}",
                            response.url,
                            preview,
                        )
                    )
                    next_queries.append(
                        "DOU: a resposta do INLABS não veio em ZIP válido; confirme `INVESTIGADOR_DOU_COOKIE`, a janela de datas e possíveis páginas de login/bloqueio."
                    )
                    continue
                for profile in self._profiles(context):
                    try:
                        records.extend(self._search_dou_zip(response.content, response.url, profile))
                    except zipfile.BadZipFile:
                        next_queries.append(
                            "DOU: o arquivo retornado pelo INLABS estava corrompido ou não era ZIP válido; revisar autenticação e parâmetros."
                        )
                        break
        if not records:
            next_queries.append("DOU: se o portal exigir autenticação/cookie INLABS, configure `INVESTIGADOR_DOU_COOKIE` e refine `metadata.date_from/date_to`.")
        return EvidenceBundle(plugin=self.name, records=records, next_queries=_dedupe(next_queries), artifacts=artifacts)

    def _search_dou_zip(self, payload: bytes, source_url: str, profile: TargetProfile) -> list[EvidenceRecord]:
        terms = [_normalize_text(term) for term in profile.search_terms if len(_normalize_text(term)) >= 4]
        matches: list[EvidenceRecord] = []
        for member, title, text in _iter_zip_xml(payload):
            normalized = _normalize_text(text)
            if not any(term in normalized for term in terms):
                continue
            snippet = _text_snippet(text, profile.search_terms)
            matches.append(
                EvidenceRecord(
                    plugin=self.name,
                    source_name=self.source_name,
                    source_class=self.source_class,
                    record_id=f"{profile.canonical_id}-{_slug_fragment(member)}",
                    url=source_url,
                    title=f"DOU: {title}",
                    claim=f"O nome {profile.title} aparece em ato oficial do DOU/INLABS.",
                    excerpt=snippet,
                    related_ids=[profile.canonical_id],
                    chronology_date=date.today().isoformat(),
                    confidence=0.78,
                )
            )
            if len(matches) >= 4:
                break
        return matches


class PortalTransparenciaPlugin(SourcePlugin):
    name = "portal-transparencia"
    source_name = "Portal da Transparência"
    source_class = "official_structured"
    evidence_role = "investigative_signal"

    def collect(self, context: PluginContext) -> EvidenceBundle:
        records: list[EvidenceRecord] = []
        artifacts: list[dict[str, Any]] = []
        next_queries: list[str] = []
        api_key = (
            os.environ.get("INVESTIGADOR_PORTAL_API_KEY")
            or os.environ.get("PORTAL_TRANSPARENCIA_API_KEY")
            or os.environ.get("TRANSPARENCIA_API_KEY")
        )
        schema_url = os.environ.get("INVESTIGADOR_PORTAL_OPENAPI_URL", "https://api.portaldatransparencia.gov.br/v3/api-docs")
        schema, response = fetch_json(schema_url)
        artifacts.append(self._artifact_json("portal-transparencia-openapi", response.url, {"paths": list(schema.get("paths", {}).keys())[:40]}))
        if not api_key:
            next_queries.append("Portal da Transparência: defina `INVESTIGADOR_PORTAL_API_KEY` ou `TRANSPARENCIA_API_KEY` para consultas ao endpoint oficial.")
            return EvidenceBundle(plugin=self.name, records=records, next_queries=next_queries, artifacts=artifacts)
        operations = self._discover_sanction_operations(schema)
        for profile in self._profiles(context):
            for operation in operations:
                params = self._build_portal_params(operation["parameters"], profile)
                if params is None:
                    continue
                try:
                    payload, api_response = fetch_json(
                        f"https://api.portaldatransparencia.gov.br{operation['path']}",
                        query=params,
                        headers={"chave-api-dados": api_key},
                    )
                except Exception:
                    continue
                rows = _as_list_rows(payload)
                artifacts.append(self._artifact_json(f"portal-{operation['label']}-{_slug_fragment(profile.title)}", api_response.url, rows[:10] if rows else payload))
                for row in rows[:5]:
                    sanctioned = _pick_value(row, "nomeSancionado", "nome", "razaoSocial")
                    sanction = _pick_value(row, "descricaoSancao", "tipoSancao", "situacao")
                    organ = _pick_value(row, "orgaoSancionador", "nomeOrgao")
                    start = _pick_value(row, "dataInicialSancao", "dataInicio", "dataPublicacao")
                    records.append(
                        EvidenceRecord(
                            plugin=self.name,
                            source_name=self.source_name,
                            source_class=self.source_class,
                            record_id=f"{operation['label']}-{_slug_fragment(json.dumps(row, ensure_ascii=False))}",
                            url=api_response.url,
                            title=f"Portal da Transparência: {operation['label'].upper()}",
                            claim=f"{profile.title} aparece em registro do Portal da Transparência relacionado a {operation['label'].upper()}",
                            excerpt=f"Sancionado: {sanctioned or profile.title}. Tipo/descrição: {sanction or 'não informada'}. Órgão: {organ or 'não informado'}.",
                            related_ids=[profile.canonical_id],
                            chronology_date=start or date.today().isoformat(),
                            confidence=0.83,
                        )
                    )
        return EvidenceBundle(plugin=self.name, records=records, next_queries=_dedupe(next_queries), artifacts=artifacts)

    def _discover_sanction_operations(self, schema: dict[str, Any]) -> list[dict[str, Any]]:
        operations: list[dict[str, Any]] = []
        for path, methods in schema.get("paths", {}).items():
            if not isinstance(methods, dict):
                continue
            lowered_path = path.lower()
            if not any(keyword in lowered_path for keyword in ("ceis", "cnep", "cepim")):
                continue
            operation = methods.get("get")
            if not isinstance(operation, dict):
                continue
            operations.append(
                {
                    "path": path,
                    "label": lowered_path.rsplit("/", 1)[-1],
                    "parameters": operation.get("parameters", []),
                }
            )
        return operations

    def _build_portal_params(self, parameters: list[dict[str, Any]], profile: TargetProfile) -> dict[str, Any] | None:
        cpf = profile.cpfs[0] if profile.cpfs else ""
        cnpj = profile.cnpjs[0] if profile.cnpjs else ""
        start_date, end_date = _resolve_date_window(profile.metadata)
        params: dict[str, Any] = {}
        for parameter in parameters:
            name = str(parameter.get("name", ""))
            lowered = name.lower()
            if "pagina" == lowered or lowered.startswith("pagina"):
                params[name] = 1
            elif "tamanhopagina" in lowered or "size" == lowered:
                params[name] = 10
            elif "cpf" in lowered and cpf:
                params[name] = cpf
            elif ("cnpj" in lowered or "codigo" in lowered or "documento" in lowered) and cnpj:
                params[name] = cnpj
            elif "data" in lowered and ("inicial" in lowered or lowered.endswith("de")):
                params[name] = start_date.isoformat()
            elif "data" in lowered and ("final" in lowered or lowered.endswith("ate")):
                params[name] = end_date.isoformat()
            elif parameter.get("required"):
                return None
        if not cpf and not cnpj:
            return None
        return params


class TransfereGovPlugin(SourcePlugin):
    name = "transferegov"
    source_name = "TransfereGov - Transferências Especiais"
    source_class = "official_structured"
    evidence_role = "investigative_signal"

    def collect(self, context: PluginContext) -> EvidenceBundle:
        records: list[EvidenceRecord] = []
        artifacts: list[dict[str, Any]] = []
        next_queries: list[str] = []
        bases = _dedupe(
            [
                os.environ.get("INVESTIGADOR_TRANSFEREGOV_BASE_URL", ""),
                "https://api.transferegov.gestao.gov.br/transferenciasespeciais",
                "https://docs.api.transferegov.gestao.gov.br/transferenciasespeciais",
            ]
        )
        relations = _coerce_list(os.environ.get("INVESTIGADOR_TRANSFEREGOV_RELATIONS")) or [
            "executor_especial",
            "relatorio_gestao_novo_especial",
        ]
        for profile in self._profiles(context):
            if not (profile.cnpjs or profile.territory_ids or profile.search_terms):
                next_queries.append(f"TransfereGov: informe CNPJ, código IBGE ou nome do beneficiário para {profile.title}.")
                continue
            for relation in relations:
                rowset = self._query_transferegov_relation(bases, relation, profile, artifacts)
                for row, source_url in rowset[:5]:
                    summary = _dict_excerpt(row, preferred=("nome_parlamentar", "nome_beneficiario", "nome_executor", "objeto"))
                    value = _pick_value(row, "valor_empenhado", "valor_pago", "valor_global", "valor_total")
                    year = _pick_value(row, "ano_emenda", "ano", "exercicio")
                    records.append(
                        EvidenceRecord(
                            plugin=self.name,
                            source_name=self.source_name,
                            source_class=self.source_class,
                            record_id=f"{relation}-{_slug_fragment(json.dumps(row, ensure_ascii=False))}",
                            url=source_url,
                            title=f"TransfereGov: {relation}",
                            claim=f"{profile.title} aparece em dado aberto de transferências especiais do TransfereGov ({relation}).",
                            excerpt=f"{summary}. Valor: {value or 'não informado'}. Ano: {year or 'não informado'}.",
                            related_ids=[profile.canonical_id],
                            chronology_date=_pick_value(row, "ano_emenda", "ano", "data_assinatura") or date.today().isoformat(),
                            confidence=0.76,
                        )
                    )
        return EvidenceBundle(plugin=self.name, records=records, next_queries=_dedupe(next_queries), artifacts=artifacts)

    def _query_transferegov_relation(
        self,
        bases: list[str],
        relation: str,
        profile: TargetProfile,
        artifacts: list[dict[str, Any]],
    ) -> list[tuple[dict[str, Any], str]]:
        for base in bases:
            if not base:
                continue
            sample_url = f"{base.rstrip('/')}/{relation}"
            try:
                sample_payload, sample_response = fetch_json(sample_url, query={"limit": 1})
            except Exception:
                continue
            sample_rows = _as_list_rows(sample_payload)
            artifacts.append(self._artifact_json(f"transferegov-sample-{relation}", sample_response.url, sample_rows[:1] if sample_rows else sample_payload))
            keys = sample_rows[0].keys() if sample_rows else []
            params = self._build_transferegov_filters(keys, profile)
            if not params:
                continue
            params["limit"] = 5
            try:
                payload, response = fetch_json(sample_url, query=params)
            except Exception:
                continue
            rows = _as_list_rows(payload)
            artifacts.append(self._artifact_json(f"transferegov-{relation}-{_slug_fragment(profile.title)}", response.url, rows[:10] if rows else payload))
            return [(row, response.url) for row in rows]
        return []

    def _build_transferegov_filters(self, keys: Iterable[str], profile: TargetProfile) -> dict[str, str] | None:
        key_list = list(keys)
        for cnpj in profile.cnpjs:
            for key in key_list:
                lowered = key.lower()
                if "cnpj" in lowered:
                    return {key: f"eq.{cnpj}"}
        for territory in profile.territory_ids:
            for key in key_list:
                if "ibge" in key.lower():
                    return {key: f"eq.{territory}"}
        for term in profile.search_terms:
            for key in key_list:
                lowered = key.lower()
                if "nome" in lowered or "objeto" in lowered:
                    return {key: f"ilike.*{term}*"}
        return None


class TCUPlugin(SourcePlugin):
    name = "tcu"
    source_name = "Tribunal de Contas da União"
    source_class = "official_structured"
    evidence_role = "investigative_signal"

    def collect(self, context: PluginContext) -> EvidenceBundle:
        records: list[EvidenceRecord] = []
        artifacts: list[dict[str, Any]] = []
        next_queries: list[str] = []
        for profile in self._profiles(context):
            if profile.cnpjs:
                for cnpj in profile.cnpjs:
                    records.extend(self._collect_tcu_certidoes(cnpj, profile, artifacts))
            if profile.cpfs or profile.cnpjs or profile.search_terms:
                records.extend(self._collect_tcu_lists(profile, artifacts))
            else:
                next_queries.append(f"TCU: informe CPF, CNPJ ou termos de busca para {profile.title}.")
        return EvidenceBundle(plugin=self.name, records=records, next_queries=_dedupe(next_queries), artifacts=artifacts)

    def _collect_tcu_certidoes(self, cnpj: str, profile: TargetProfile, artifacts: list[dict[str, Any]]) -> list[EvidenceRecord]:
        url = "https://contas.tcu.gov.br/ords/f"
        params = {"p": f"704144:3:0:::3:P3_TIPO,P3_COD:1,{cnpj}"}
        try:
            text, response = fetch_text(url, query=params)
        except Exception:
            return []
        snippet = _text_snippet(text, [cnpj, profile.title], radius=260)
        artifacts.append(self._artifact_text(f"tcu-certidao-{cnpj}", response.url, snippet))
        return [
            EvidenceRecord(
                plugin=self.name,
                source_name=self.source_name,
                source_class=self.source_class,
                record_id=f"certidao-{cnpj}",
                url=response.url,
                title=f"TCU certidão: {cnpj}",
                claim=f"O TCU disponibiliza certidão pública consultável para o CNPJ {cnpj}.",
                excerpt=snippet,
                related_ids=[profile.canonical_id],
                chronology_date=date.today().isoformat(),
                confidence=0.7,
            )
        ]

    def _collect_tcu_lists(self, profile: TargetProfile, artifacts: list[dict[str, Any]]) -> list[EvidenceRecord]:
        page_url = "https://sites.tcu.gov.br/dados-abertos/inidoneos-irregulares"
        try:
            html, response = fetch_text(page_url)
        except Exception:
            return []
        csv_urls = re.findall(r"https://[^\"']+\.csv", html)
        artifacts.append(self._artifact_json("tcu-list-page", response.url, {"csv_urls": csv_urls[:10]}))
        records: list[EvidenceRecord] = []
        needles = {value for value in profile.cnpjs + profile.cpfs + profile.search_terms if value}
        normalized_needles = {_normalize_text(term) for term in profile.search_terms}
        for csv_url in csv_urls[:4]:
            try:
                csv_text, csv_response = fetch_text(csv_url)
            except Exception:
                continue
            reader = csv.DictReader(io.StringIO(csv_text))
            matches = 0
            for row in reader:
                haystack = " ".join(str(value) for value in row.values())
                digits_haystack = _digits(haystack)
                normalized_haystack = _normalize_text(haystack)
                if needles and any(needle in digits_haystack for needle in profile.cnpjs + profile.cpfs if needle):
                    matched = True
                else:
                    matched = any(term in normalized_haystack for term in normalized_needles if term)
                if not matched:
                    continue
                matches += 1
                records.append(
                    EvidenceRecord(
                        plugin=self.name,
                        source_name=self.source_name,
                        source_class=self.source_class,
                        record_id=f"lista-{_slug_fragment(json.dumps(row, ensure_ascii=False))}",
                        url=csv_response.url,
                        title="TCU listas públicas",
                        claim=f"{profile.title} aparece em lista pública disponibilizada pelo TCU.",
                        excerpt=_dict_excerpt(row),
                        related_ids=[profile.canonical_id],
                        chronology_date=date.today().isoformat(),
                        confidence=0.82,
                    )
                )
                if matches >= 5:
                    break
        return records


class QueridoDiarioPlugin(SourcePlugin):
    name = "querido-diario"
    source_name = "Querido Diário"
    source_class = "official_document"
    evidence_role = "investigative_signal"

    def collect(self, context: PluginContext) -> EvidenceBundle:
        records: list[EvidenceRecord] = []
        artifacts: list[dict[str, Any]] = []
        next_queries: list[str] = []
        base_url = os.environ.get("INVESTIGADOR_QD_BASE_URL", "https://api.queridodiario.ok.org.br")
        for profile in self._profiles(context):
            territory_ids = profile.territory_ids
            if not territory_ids:
                next_queries.append(f"Querido Diário: informe `metadata.territory_ids` ou `metadata.municipality_ibge` para {profile.title}.")
                continue
            for territory_id in territory_ids[:3]:
                for term in profile.search_terms[:3]:
                    params = {
                        "territory_ids": territory_id,
                        "querystring": term,
                        "excerpt_size": 500,
                        "number_of_excerpts": 1,
                        "size": 5,
                    }
                    try:
                        payload, response = fetch_json(f"{base_url.rstrip('/')}/gazettes", query=params)
                    except Exception:
                        continue
                    rows = _as_list_rows(payload)
                    if not rows and isinstance(payload, dict):
                        rows = _as_list_rows(payload.get("gazettes"))
                    artifacts.append(self._artifact_json(f"qd-{territory_id}-{_slug_fragment(term)}", response.url, rows[:10] if rows else payload))
                    for row in rows[:5]:
                        excerpt = _pick_value(row, "excerpt", "excerpts", "texto")
                        file_url = _pick_value(row, "url", "file_raw", "txt_url") or response.url
                        gazette_date = _pick_value(row, "date", "scraped_at", "published_at") or date.today().isoformat()
                        title = _pick_value(row, "edition", "edition_number", "territory_name") or f"Município {territory_id}"
                        records.append(
                            EvidenceRecord(
                                plugin=self.name,
                                source_name=self.source_name,
                                source_class=self.source_class,
                                record_id=f"{territory_id}-{_slug_fragment(file_url + term)}",
                                url=file_url,
                                title=f"Querido Diário: {title}",
                                claim=f"O termo {term!r} aparece em diário oficial indexado pelo Querido Diário para o território {territory_id}.",
                                excerpt=excerpt or _dict_excerpt(row),
                                related_ids=[profile.canonical_id],
                                chronology_date=gazette_date,
                                confidence=0.79,
                            )
                        )
        return EvidenceBundle(plugin=self.name, records=records, next_queries=_dedupe(next_queries), artifacts=artifacts)


class WebSearchPlugin(SourcePlugin):
    name = "web-search"
    source_name = "Contextual Web Search"
    source_class = "contextual_web"
    evidence_role = "contextual_lead"

    def collect(self, context: PluginContext) -> EvidenceBundle:
        records: list[EvidenceRecord] = []
        artifacts: list[dict[str, Any]] = []
        next_queries: list[str] = []
        proposed_links: list[dict[str, Any]] = []
        base_url = os.environ.get("INVESTIGADOR_WEB_SEARCH_URL", "https://html.duckduckgo.com/html/")
        provider = os.environ.get("INVESTIGADOR_WEB_SEARCH_PROVIDER", "duckduckgo_html")
        for profile in self._profiles(context):
            queries = self._queries(profile)
            if not queries:
                next_queries.append(f"Web search: faltam termos úteis para {profile.title}; confirme nome canônico e fatos básicos antes da busca contextual.")
                continue
            for query in queries[:4]:
                try:
                    html_text, response = fetch_text(base_url, query={"q": query})
                except Exception:
                    next_queries.append(f"Web search: falha ao consultar a web aberta para {profile.title} com a consulta {query!r}.")
                    continue
                parsed_results = self._parse_results(html_text)
                artifacts.append(
                    self._artifact_json(
                        f"web-search-{profile.canonical_id}-{_slug_fragment(query)}",
                        response.url,
                        {"query": query, "results": parsed_results[:10], "provider": provider},
                    )
                )
                if not parsed_results:
                    next_queries.append(f"Web search: nenhuma referência contextual útil para {profile.title} com a consulta {query!r}.")
                    continue
                for result in parsed_results[:5]:
                    resolved_url = _decode_search_url(result.get("url", ""))
                    if not resolved_url:
                        continue
                    parsed_url = parse.urlsplit(resolved_url)
                    domain = parsed_url.netloc.lower()
                    if self._is_low_value_result(domain, result):
                        continue
                    publisher = result.get("publisher") or domain
                    published_at = result.get("published_at") or ""
                    title = result.get("title") or domain or profile.title
                    snippet = result.get("snippet") or f"Referência contextual capturada para {profile.title}."
                    record_id = _slug_fragment(profile.canonical_id + resolved_url)
                    if self._looks_official_domain(domain):
                        proposed_links.append(
                            {
                                "plugin": self.name,
                                "query": query,
                                "url": resolved_url,
                                "domain": domain,
                                "reason": "Resultado contextual aponta para domínio oficial que merece coleta primária.",
                            }
                        )
                    records.append(
                        EvidenceRecord(
                            plugin=self.name,
                            source_name=self.source_name,
                            source_class=self.source_class,
                            record_id=record_id,
                            url=resolved_url,
                            title=f"Web: {title}",
                            claim=f"Busca web encontrou referência contextual potencialmente relacionada a {profile.title}: {title}",
                            excerpt=snippet,
                            related_ids=[profile.canonical_id],
                            chronology_date=published_at or date.today().isoformat(),
                            confidence=0.48,
                            source_metadata={
                                "query": query,
                                "domain": domain,
                                "publisher": publisher,
                                "published_at": published_at,
                                "retrieved_from": provider,
                            },
                        )
                    )
        return EvidenceBundle(
            plugin=self.name,
            records=records,
            next_queries=_dedupe(next_queries),
            proposed_links=proposed_links,
            artifacts=artifacts,
        )

    def _queries(self, profile: TargetProfile) -> list[str]:
        metadata = profile.metadata
        title = profile.title.strip()
        quoted = f'"{title}"' if title else ""
        candidates = [
            quoted,
            f'{quoted} "{metadata.get("office", "")}"'.strip(),
            " ".join(part for part in [quoted, str(metadata.get("party", "")).strip(), str(metadata.get("uf", "")).strip()] if part),
            " ".join(part for part in [quoted, str(metadata.get("election_year", "")).strip()] if part),
            " ".join(part for part in [quoted, str(metadata.get("registered_city", metadata.get("municipality", metadata.get("municipality_ibge", "")))).strip()] if part),
        ]
        if profile.cnpjs:
            candidates.append(f'{quoted} "{profile.cnpjs[0]}"'.strip())
        if metadata.get("ballot_name"):
            candidates.append(f'"{metadata["ballot_name"]}"')
        return _dedupe([candidate for candidate in candidates if candidate and candidate != '""'])

    def _parse_results(self, html_text: str) -> list[dict[str, str]]:
        blocks = re.findall(
            r'<a[^>]+class="[^"]*result__a[^"]*"[^>]+href="([^"]+)"[^>]*>(.*?)</a>(.*?)(?=<a[^>]+class="[^"]*result__a|$)',
            html_text,
            flags=re.IGNORECASE | re.DOTALL,
        )
        results: list[dict[str, str]] = []
        for href, raw_title, trailing in blocks:
            title = _strip_tags(raw_title)
            snippet_match = re.search(r'<a[^>]+class="[^"]*result__snippet[^"]*"[^>]*>(.*?)</a>|<div[^>]+class="[^"]*result__snippet[^"]*"[^>]*>(.*?)</div>', trailing, flags=re.IGNORECASE | re.DOTALL)
            publisher_match = re.search(r'<span[^>]+class="[^"]*result__url[^"]*"[^>]*>(.*?)</span>', trailing, flags=re.IGNORECASE | re.DOTALL)
            published_match = re.search(r'(\d{4}-\d{2}-\d{2})', trailing)
            snippet = _strip_tags(snippet_match.group(1) or snippet_match.group(2) or "") if snippet_match else ""
            publisher = _strip_tags(publisher_match.group(1)) if publisher_match else ""
            results.append(
                {
                    "url": href,
                    "title": title,
                    "snippet": snippet,
                    "publisher": publisher,
                    "published_at": published_match.group(1) if published_match else "",
                }
            )
        return results

    def _looks_official_domain(self, domain: str) -> bool:
        return any(
            domain.endswith(suffix)
            for suffix in (".gov.br", ".jus.br", ".leg.br", ".mp.br")
        )

    def _is_low_value_result(self, domain: str, result: dict[str, str]) -> bool:
        blocked_domains = (
            "facebook.com",
            "m.facebook.com",
            "pt.wikipedia.org",
            "wikipedia.org",
            "wiktionary.org",
            "dicio.com.br",
            "michaelis.uol.com.br",
            "sinonimos.com.br",
        )
        if any(domain.endswith(blocked) for blocked in blocked_domains):
            return True
        title = _normalize_text(result.get("title", ""))
        snippet = _normalize_text(result.get("snippet", ""))
        low_value_terms = ("dicionario", "wiktionary", "sinonimo", "tradução", "translation", "meaning")
        return any(term in title or term in snippet for term in low_value_terms)


class CamaraBasePlugin(SourcePlugin):
    api_base = "https://dadosabertos.camara.leg.br/api/v2"

    def _camara_id(self, profile: TargetProfile) -> str:
        explicit = str(profile.metadata.get("camara_id") or "").strip()
        if explicit:
            return explicit
        if str(profile.metadata.get("roster_plugin") or "").strip() == "camara-roster":
            return str(profile.metadata.get("roster_member_id") or "").strip()
        return ""


class CamaraProfilePlugin(CamaraBasePlugin):
    name = "camara-profile"
    source_name = "Câmara dos Deputados - Perfil"
    source_class = "official_structured"
    evidence_role = "identity_baseline"

    def collect(self, context: PluginContext) -> EvidenceBundle:
        records: list[EvidenceRecord] = []
        artifacts: list[dict[str, Any]] = []
        next_queries: list[str] = []
        for profile in self._profiles(context):
            camara_id = self._camara_id(profile)
            if not camara_id:
                continue
            url = f"{self.api_base}/deputados/{camara_id}"
            payload, response = fetch_json(url)
            row = payload.get("dados", {}) if isinstance(payload, dict) else {}
            if not isinstance(row, dict) or not row:
                next_queries.append(f"Câmara perfil: resposta vazia para o deputado {profile.title}.")
                continue
            ultimo_status = row.get("ultimoStatus", {}) if isinstance(row.get("ultimoStatus"), dict) else {}
            artifacts.append(self._artifact_json(f"camara-profile-{camara_id}", response.url, payload))
            civil_name = _pick_value(row, "nomeCivil")
            email = _pick_value(ultimo_status, "email")
            website = _pick_value(row, "urlWebsite")
            party = _pick_value(ultimo_status, "siglaPartido") or _pick_value(row, "siglaPartido") or profile.metadata.get("party", "")
            uf = _pick_value(ultimo_status, "siglaUf") or _pick_value(row, "siglaUf") or profile.metadata.get("uf", "")
            excerpt = _dict_excerpt(
                {
                    "nomeCivil": civil_name,
                    "siglaPartido": party,
                    "siglaUf": uf,
                    "gabinete": _pick_value(ultimo_status.get("gabinete", {}) if isinstance(ultimo_status.get("gabinete"), dict) else {}, "nome"),
                    "email": email,
                }
            )
            proposed_entities: list[ProposedEntity] = [
                ProposedEntity(
                    entity_type="organization",
                    identifier="public-body:camara-dos-deputados",
                    name="Câmara dos Deputados",
                    relation="órgão oficial do mandato federal registrado em perfil público",
                    confidence=0.96,
                    source_refs=[self._reference(f"{camara_id}-profile", response.url)],
                )
            ]
            if party:
                proposed_entities.append(
                    ProposedEntity(
                        entity_type="organization",
                        identifier=f"party:{party}",
                        name=party,
                        relation="partido informado no perfil oficial da Câmara",
                        confidence=0.86,
                        source_refs=[self._reference(f"{camara_id}-profile", response.url)],
                    )
                )
            records.append(
                EvidenceRecord(
                    plugin=self.name,
                    source_name=self.source_name,
                    source_class=self.source_class,
                    record_id=f"{camara_id}-profile",
                    url=response.url,
                    title=f"Câmara perfil: {profile.title}",
                    claim=f"{profile.title} aparece no perfil oficial da Câmara dos Deputados com mandato federal ativo ou histórico recente.",
                    excerpt=excerpt or f"Perfil oficial da Câmara localizado para {profile.title}.",
                    related_ids=[profile.canonical_id],
                    chronology_date=date.today().isoformat(),
                    confidence=0.88,
                    evidence_layer="identity_baseline",
                    case_relevance=30,
                    proposed_entities=proposed_entities,
                    metadata_updates={
                        "camara_id": camara_id,
                        "office": "Deputado Federal",
                        "legislature_level": "federal",
                        "party": party,
                        "uf": uf,
                        "civil_name": civil_name,
                        "email": email,
                        "website": website,
                    },
                )
            )
        return EvidenceBundle(plugin=self.name, records=records, next_queries=_dedupe(next_queries), artifacts=artifacts)


class CamaraExpensesPlugin(CamaraBasePlugin):
    name = "camara-expenses"
    source_name = "Câmara dos Deputados - Despesas"
    source_class = "official_structured"
    evidence_role = "investigative_signal"

    def collect(self, context: PluginContext) -> EvidenceBundle:
        records: list[EvidenceRecord] = []
        artifacts: list[dict[str, Any]] = []
        next_queries: list[str] = []
        for profile in self._profiles(context):
            camara_id = self._camara_id(profile)
            if not camara_id:
                continue
            url = f"{self.api_base}/deputados/{camara_id}/despesas"
            payload, response = fetch_json(url, query={"itens": 30, "ordem": "DESC", "ordenarPor": "ano"})
            rows = payload.get("dados", []) if isinstance(payload, dict) else []
            if not isinstance(rows, list) or not rows:
                next_queries.append(f"Câmara despesas: nenhuma despesa retornada para {profile.title}.")
                continue
            artifacts.append(self._artifact_json(f"camara-expenses-{camara_id}", response.url, payload))
            total = 0.0
            categories: list[str] = []
            proposed_entities: list[ProposedEntity] = []
            seen_entities: set[str] = set()
            supplier_totals: dict[str, dict[str, Any]] = {}
            category_totals: dict[str, float] = {}
            for row in rows[:20]:
                if not isinstance(row, dict):
                    continue
                amount = _safe_float(_pick_value(row, "valorLiquido", "valorDocumento", "valorGlosa")) or 0.0
                total += amount
                category = _pick_value(row, "tipoDespesa")
                if category:
                    categories.append(category)
                    category_totals[category] = category_totals.get(category, 0.0) + amount
                supplier_name = _pick_value(
                    row,
                    "nomeFornecedor",
                    "fornecedor",
                    "nomeEmitente",
                    "nomeEstabelecimento",
                    "nomeBeneficiario",
                )
                supplier_identifier = _digits(
                    _pick_value(
                        row,
                        "cnpjCpfFornecedor",
                        "cnpjCpfEmitente",
                        "cpfCnpj",
                        "documento",
                    )
                )
                if supplier_name:
                    entity_id = supplier_identifier or f"org:{_slug_fragment(supplier_name)}"
                    supplier_bucket = supplier_totals.setdefault(
                        entity_id,
                        {
                            "name": supplier_name,
                            "identifier": supplier_identifier or entity_id,
                            "total": 0.0,
                            "count": 0,
                            "categories": [],
                        },
                    )
                    supplier_bucket["total"] += amount
                    supplier_bucket["count"] += 1
                    if category:
                        supplier_bucket["categories"].append(category)
                    if entity_id not in seen_entities:
                        seen_entities.add(entity_id)
                        proposed_entities.append(
                            ProposedEntity(
                                entity_type="organization",
                                identifier=entity_id,
                                name=supplier_name,
                                relation="fornecedor ou contraparte em despesa parlamentar oficial da Câmara",
                                confidence=0.82,
                                source_refs=[self._reference(f"{camara_id}-expenses", response.url)],
                            )
                        )
            excerpt = f"Despesas recentes somam aproximadamente R$ {total:,.2f} nas categorias: {', '.join(_dedupe(categories)[:5]) or 'sem categoria legível'}."
            records.append(
                EvidenceRecord(
                    plugin=self.name,
                    source_name=self.source_name,
                    source_class=self.source_class,
                    record_id=f"{camara_id}-expenses",
                    url=response.url,
                    title=f"Câmara despesas: {profile.title}",
                    claim=f"A Câmara publica despesas recentes associadas ao deputado federal {profile.title}.",
                    excerpt=excerpt,
                    related_ids=[profile.canonical_id],
                    chronology_date=date.today().isoformat(),
                    confidence=0.81,
                    evidence_layer="investigative_signal",
                    case_relevance=72,
                    supports_hypothesis_types=["expense_anomaly", "relationship_network_risk"],
                    proposed_entities=proposed_entities,
                    metadata_updates={},
                    source_metadata={
                        "signal_type": "expense_summary",
                        "metrics": {
                            "recent_total": round(total, 2),
                            "supplier_count": len(supplier_totals),
                            "category_count": len(category_totals),
                        },
                    },
                )
            )
            ranked_suppliers = sorted(
                supplier_totals.values(),
                key=lambda item: (float(item["total"]), int(item["count"])),
                reverse=True,
            )
            for supplier in ranked_suppliers[:3]:
                if total <= 0 or float(supplier["total"]) <= 0:
                    continue
                share = float(supplier["total"]) / total
                if share < 0.18 and int(supplier["count"]) < 2:
                    continue
                supplier_entity = ProposedEntity(
                    entity_type="organization",
                    identifier=str(supplier["identifier"]),
                    name=str(supplier["name"]),
                    relation="fornecedor com concentração relevante em despesas parlamentares recentes",
                    confidence=0.88,
                    source_refs=[self._reference(f"{camara_id}-expenses", response.url)],
                )
                supplier_excerpt = (
                    f"O fornecedor concentrou aproximadamente R$ {float(supplier['total']):,.2f} "
                    f"({share:.0%} do recorte recente), em {int(supplier['count'])} lançamento(s)."
                )
                categories_text = ", ".join(_dedupe([str(item) for item in supplier.get("categories", [])])[:3])
                if categories_text:
                    supplier_excerpt += f" Categorias dominantes: {categories_text}."
                records.append(
                    EvidenceRecord(
                        plugin=self.name,
                        source_name=self.source_name,
                        source_class=self.source_class,
                        record_id=f"{camara_id}-supplier-{_slug_fragment(str(supplier['identifier']) + str(supplier['name']))}",
                        url=response.url,
                        title=f"Câmara despesas: concentração em fornecedor {supplier['name']}",
                        claim=(
                            f"As despesas recentes de {profile.title} mostram concentração relevante no fornecedor "
                            f"{supplier['name']}."
                        ),
                        excerpt=supplier_excerpt,
                        related_ids=[profile.canonical_id],
                        chronology_date=date.today().isoformat(),
                        confidence=0.86 if share >= 0.25 else 0.8,
                        evidence_layer="investigative_signal",
                        case_relevance=88 if share >= 0.25 else 78,
                        supports_hypothesis_types=["expense_anomaly", "relationship_network_risk"],
                        proposed_entities=[supplier_entity],
                        metadata_updates={},
                        source_metadata={
                            "signal_type": "supplier_concentration",
                            "metrics": {
                                "recent_total": round(float(supplier["total"]), 2),
                                "share": round(share, 4),
                                "count": int(supplier["count"]),
                            },
                        },
                    )
                )
                if int(supplier["count"]) >= 2:
                    records.append(
                        EvidenceRecord(
                            plugin=self.name,
                            source_name=self.source_name,
                            source_class=self.source_class,
                            record_id=f"{camara_id}-supplier-recurring-{_slug_fragment(str(supplier['identifier']) + str(supplier['name']))}",
                            url=response.url,
                            title=f"Câmara despesas: recorrência com fornecedor {supplier['name']}",
                            claim=(
                                f"As despesas recentes de {profile.title} repetem lançamentos com o fornecedor "
                                f"{supplier['name']}."
                            ),
                            excerpt=(
                                f"O fornecedor aparece em {int(supplier['count'])} lançamento(s) recentes, totalizando "
                                f"aproximadamente R$ {float(supplier['total']):,.2f}."
                            ),
                            related_ids=[profile.canonical_id],
                            chronology_date=date.today().isoformat(),
                            confidence=0.77,
                            evidence_layer="case_support",
                            case_relevance=76,
                            supports_hypothesis_types=["expense_anomaly", "relationship_network_risk"],
                            proposed_entities=[supplier_entity],
                            metadata_updates={},
                            source_metadata={
                                "signal_type": "supplier_recurrence",
                                "metrics": {
                                    "recent_total": round(float(supplier["total"]), 2),
                                    "count": int(supplier["count"]),
                                },
                            },
                        )
                    )
            ranked_categories = sorted(category_totals.items(), key=lambda item: item[1], reverse=True)
            if ranked_categories and total > 0:
                top_category, top_amount = ranked_categories[0]
                top_share = top_amount / total
                if top_share >= 0.3:
                    records.append(
                        EvidenceRecord(
                            plugin=self.name,
                            source_name=self.source_name,
                            source_class=self.source_class,
                            record_id=f"{camara_id}-category-{_slug_fragment(top_category)}",
                            url=response.url,
                            title=f"Câmara despesas: concentração na categoria {top_category}",
                            claim=(
                                f"As despesas recentes de {profile.title} concentram parcela relevante na categoria "
                                f"{top_category}."
                            ),
                            excerpt=(
                                f"A categoria {top_category} respondeu por aproximadamente R$ {top_amount:,.2f} "
                                f"({top_share:.0%} do recorte recente)."
                            ),
                            related_ids=[profile.canonical_id],
                            chronology_date=date.today().isoformat(),
                            confidence=0.78,
                            evidence_layer="case_support",
                            case_relevance=74,
                            supports_hypothesis_types=["expense_anomaly"],
                            metadata_updates={},
                            source_metadata={
                                "signal_type": "category_concentration",
                                "metrics": {
                                    "recent_total": round(top_amount, 2),
                                    "share": round(top_share, 4),
                                },
                            },
                        )
                    )
        return EvidenceBundle(plugin=self.name, records=records, next_queries=_dedupe(next_queries), artifacts=artifacts)


class CamaraOrgansPlugin(CamaraBasePlugin):
    name = "camara-organs"
    source_name = "Câmara dos Deputados - Órgãos"
    source_class = "official_structured"
    evidence_role = "identity_baseline"

    def collect(self, context: PluginContext) -> EvidenceBundle:
        records: list[EvidenceRecord] = []
        artifacts: list[dict[str, Any]] = []
        next_queries: list[str] = []
        for profile in self._profiles(context):
            camara_id = self._camara_id(profile)
            if not camara_id:
                continue
            url = f"{self.api_base}/deputados/{camara_id}/orgaos"
            payload, response = fetch_json(url, query={"itens": 20})
            rows = payload.get("dados", []) if isinstance(payload, dict) else []
            if not isinstance(rows, list) or not rows:
                next_queries.append(f"Câmara órgãos: nenhum órgão retornado para {profile.title}.")
                continue
            artifacts.append(self._artifact_json(f"camara-organs-{camara_id}", response.url, payload))
            organs = []
            for row in rows[:10]:
                if not isinstance(row, dict):
                    continue
                organ_name = _pick_value(row, "siglaOrgao", "nomeOrgao")
                role = _pick_value(row, "titulo", "nomePublicacao")
                organs.append(f"{organ_name} ({role})" if organ_name and role else organ_name or role)
            excerpt = "Participação institucional recente: " + ", ".join(item for item in _dedupe(organs)[:6] if item)
            records.append(
                EvidenceRecord(
                    plugin=self.name,
                    source_name=self.source_name,
                    source_class=self.source_class,
                    record_id=f"{camara_id}-organs",
                    url=response.url,
                    title=f"Câmara órgãos: {profile.title}",
                    claim=f"A Câmara publica vínculos institucionais e atuação em órgãos para o deputado {profile.title}.",
                    excerpt=excerpt or f"Órgãos relacionados ao deputado {profile.title} na Câmara.",
                    related_ids=[profile.canonical_id],
                    chronology_date=date.today().isoformat(),
                    confidence=0.8,
                    evidence_layer="case_support",
                    case_relevance=54,
                    supports_hypothesis_types=["relationship_network_risk"],
                )
            )
        return EvidenceBundle(plugin=self.name, records=records, next_queries=_dedupe(next_queries), artifacts=artifacts)


def _dict_excerpt(row: dict[str, Any], preferred: tuple[str, ...] = ()) -> str:
    parts: list[str] = []
    for key in preferred:
        value = _pick_value(row, key)
        if value:
            parts.append(f"{key}: {value}")
    if not parts:
        for key, value in row.items():
            if value in (None, ""):
                continue
            parts.append(f"{key}: {value}")
            if len(parts) >= 4:
                break
    return " | ".join(parts)[:320]


def _resolve_date_window(metadata: dict[str, Any]) -> tuple[date, date]:
    today = date.today()
    start = metadata.get("date_from")
    end = metadata.get("date_to")
    try:
        start_date = date.fromisoformat(str(start)) if start else today - timedelta(days=int(metadata.get("window_days", 7)))
    except ValueError:
        start_date = today - timedelta(days=7)
    try:
        end_date = date.fromisoformat(str(end)) if end else today
    except ValueError:
        end_date = today
    if start_date > end_date:
        start_date, end_date = end_date, start_date
    return start_date, end_date


def _date_range(start: date, end: date) -> Iterable[date]:
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


PLUGIN_REGISTRY = {
    "mock": MockPlugin,
    "tse": TSEPlugin,
    "cnpj-qsa": CNPJQSAPlugin,
    "pncp": PNCPPlugin,
    "datajud": DataJudPlugin,
    "dou": DOUPlugin,
    "portal-transparencia": PortalTransparenciaPlugin,
    "transferegov": TransfereGovPlugin,
    "tcu": TCUPlugin,
    "querido-diario": QueridoDiarioPlugin,
    "web-search": WebSearchPlugin,
    "camara-profile": CamaraProfilePlugin,
    "camara-expenses": CamaraExpensesPlugin,
    "camara-organs": CamaraOrgansPlugin,
}


def get_plugin(name: str) -> SourcePlugin:
    try:
        return PLUGIN_REGISTRY[name]()
    except KeyError as exc:
        known = ", ".join(sorted(PLUGIN_REGISTRY))
        raise ValueError(msg("plugin_desconhecido", plugin=name, known=known)) from exc


def iter_plugins(names: Iterable[str]) -> list[SourcePlugin]:
    return [get_plugin(name) for name in names]
