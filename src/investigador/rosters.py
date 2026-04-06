from __future__ import annotations

import hashlib
import html
import os
import re
import unicodedata
from dataclasses import dataclass
from typing import Any, Iterable
from urllib import parse

from .http import fetch_json, fetch_text
from .models import RosterMember, RosterResult, UF_CODES


def _normalize_text(text: str | None) -> str:
    value = unicodedata.normalize("NFKD", str(text or ""))
    value = "".join(char for char in value if not unicodedata.combining(char))
    return re.sub(r"\s+", " ", value).strip().lower()


def _slug_fragment(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()[:10]


def _dedupe(values: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        ordered.append(text)
    return ordered


def _strip_tags(text: str) -> str:
    without_tags = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", html.unescape(without_tags)).strip()


def _looks_like_person_name(text: str) -> bool:
    cleaned = re.sub(r"[^A-Za-zÀ-ÿ' -]", " ", text).strip()
    words = [word for word in cleaned.split() if word]
    if len(words) < 2:
        return False
    blacklist = {
        "deputados",
        "deputado",
        "parlamentares",
        "mesa",
        "comissoes",
        "comissões",
        "noticias",
        "notícias",
        "agenda",
        "contato",
        "mandatos",
        "assembleia",
        "legislativa",
        "liderancas",
        "lideranças",
    }
    lowered = {word.casefold() for word in words}
    if lowered & blacklist:
        return False
    alpha_words = [word for word in words if re.search(r"[A-Za-zÀ-ÿ]", word)]
    return len(alpha_words) >= 2


def _candidate_id(source_plugin: str, value: str) -> str:
    if str(value).startswith(("http://", "https://")):
        parsed = parse.urlsplit(value)
        segments = [segment for segment in parsed.path.split("/") if segment]
        if segments:
            value = segments[-1]
    normalized = re.sub(r"[^a-z0-9]+", "-", _normalize_text(value))
    normalized = normalized.strip("-")
    if normalized:
        return normalized[:80]
    return f"{source_plugin}-{_slug_fragment(value)}"


def _extract_party(fragment: str) -> str:
    label_match = re.search(
        r"(?:partido|sigla|bancada)\s*[:\-]?\s*([A-Z]{2,6})",
        fragment,
        flags=re.IGNORECASE,
    )
    if label_match:
        return label_match.group(1).upper()
    small_caps = re.findall(r"\b([A-Z]{2,6})\b", fragment)
    for candidate in small_caps:
        if candidate not in {"CPF", "CEP", "ALE", "AL", "UF", "HTML", "JPEG", "PDF"}:
            return candidate
    return ""


def _artifact_html(name: str, source_url: str, html_text: str) -> dict[str, Any]:
    return {
        "name": name,
        "filename": f"{name}.html",
        "content_type": "text/html",
        "source_url": source_url,
        "text": html_text,
    }


def _artifact_json(name: str, source_url: str, payload: Any) -> dict[str, Any]:
    return {
        "name": name,
        "filename": f"{name}.json",
        "content_type": "application/json",
        "source_url": source_url,
        "json": payload,
    }


@dataclass(frozen=True, slots=True)
class StateAssemblyConfig:
    uf: str
    assembly_name: str
    base_url: str
    candidate_paths: tuple[str, ...]
    member_path_hints: tuple[str, ...] = ()


STATE_ASSEMBLIES: dict[str, StateAssemblyConfig] = {
    "AC": StateAssemblyConfig("AC", "Assembleia Legislativa do Acre", "https://www.al.ac.leg.br", ("/deputados", "/parlamentares")),
    "AL": StateAssemblyConfig("AL", "Assembleia Legislativa de Alagoas", "https://www.al.al.leg.br", ("/deputados", "/deputados-estaduais")),
    "AP": StateAssemblyConfig("AP", "Assembleia Legislativa do Amapá", "https://www.al.ap.gov.br", ("/deputados", "/parlamentares")),
    "AM": StateAssemblyConfig("AM", "Assembleia Legislativa do Amazonas", "https://www.aleam.gov.br", ("/deputados", "/parlamentares")),
    "BA": StateAssemblyConfig("BA", "Assembleia Legislativa da Bahia", "https://www.al.ba.leg.br", ("/deputados", "/deputados-estaduais")),
    "CE": StateAssemblyConfig("CE", "Assembleia Legislativa do Ceará", "https://www.al.ce.gov.br", ("/deputados", "/parlamentares")),
    "DF": StateAssemblyConfig("DF", "Câmara Legislativa do Distrito Federal", "https://www.cl.df.gov.br", ("/deputados", "/parlamentares", "/web/guest/deputados")),
    "ES": StateAssemblyConfig("ES", "Assembleia Legislativa do Espírito Santo", "https://www.al.es.gov.br", ("/deputados", "/parlamentares")),
    "GO": StateAssemblyConfig("GO", "Assembleia Legislativa de Goiás", "https://www.al.go.leg.br", ("/deputados", "/parlamentares")),
    "MA": StateAssemblyConfig("MA", "Assembleia Legislativa do Maranhão", "https://www.al.ma.leg.br", ("/deputados", "/parlamentares")),
    "MT": StateAssemblyConfig("MT", "Assembleia Legislativa do Mato Grosso", "https://www.al.mt.gov.br", ("/deputados", "/parlamentares")),
    "MS": StateAssemblyConfig("MS", "Assembleia Legislativa do Mato Grosso do Sul", "https://www.al.ms.gov.br", ("/deputados", "/parlamentares")),
    "MG": StateAssemblyConfig("MG", "Assembleia Legislativa de Minas Gerais", "https://www.almg.gov.br", ("/deputados", "/deputados/lista_deputados.html")),
    "PA": StateAssemblyConfig("PA", "Assembleia Legislativa do Pará", "https://www.alepa.pa.gov.br", ("/deputados", "/parlamentares")),
    "PB": StateAssemblyConfig("PB", "Assembleia Legislativa da Paraíba", "https://www.al.pb.leg.br", ("/deputados", "/parlamentares")),
    "PR": StateAssemblyConfig("PR", "Assembleia Legislativa do Paraná", "https://www.alep.pr.gov.br", ("/deputados", "/parlamentares")),
    "PE": StateAssemblyConfig("PE", "Assembleia Legislativa de Pernambuco", "https://www.alepe.pe.gov.br", ("/deputados", "/parlamentares")),
    "PI": StateAssemblyConfig("PI", "Assembleia Legislativa do Piauí", "https://www.al.pi.leg.br", ("/deputados", "/parlamentares")),
    "RJ": StateAssemblyConfig("RJ", "Assembleia Legislativa do Rio de Janeiro", "https://www.alerj.rj.gov.br", ("/Deputados", "/deputados")),
    "RN": StateAssemblyConfig("RN", "Assembleia Legislativa do Rio Grande do Norte", "https://www.al.rn.leg.br", ("/deputados", "/parlamentares")),
    "RS": StateAssemblyConfig("RS", "Assembleia Legislativa do Rio Grande do Sul", "https://www.al.rs.gov.br", ("/deputados", "/parlamentares")),
    "RO": StateAssemblyConfig("RO", "Assembleia Legislativa de Rondônia", "https://www.al.ro.leg.br", ("/deputados", "/parlamentares")),
    "RR": StateAssemblyConfig("RR", "Assembleia Legislativa de Roraima", "https://www.al.rr.leg.br", ("/deputados", "/parlamentares")),
    "SC": StateAssemblyConfig("SC", "Assembleia Legislativa de Santa Catarina", "https://www.alesc.sc.gov.br", ("/deputados", "/deputados/legislatura-atual")),
    "SP": StateAssemblyConfig("SP", "Assembleia Legislativa de São Paulo", "https://www.al.sp.gov.br", ("/deputados", "/deputado", "/web/deputados")),
    "SE": StateAssemblyConfig("SE", "Assembleia Legislativa de Sergipe", "https://www.al.se.leg.br", ("/deputados", "/parlamentares")),
    "TO": StateAssemblyConfig("TO", "Assembleia Legislativa do Tocantins", "https://www.al.to.leg.br", ("/deputados", "/parlamentares")),
}


class RosterSource:
    name = "base-roster"
    scope = "state"

    def list_current_members(self) -> RosterResult:
        raise NotImplementedError


class CamaraRosterSource(RosterSource):
    name = "camara-roster"
    scope = "federal"

    def list_current_members(self) -> RosterResult:
        url = os.environ.get(
            "INVESTIGADOR_CAMARA_ROSTER_URL",
            "https://dadosabertos.camara.leg.br/api/v2/deputados",
        )
        payload, response = fetch_json(url, query={"itens": 600, "ordem": "ASC", "ordenarPor": "nome"})
        rows = payload.get("dados", []) if isinstance(payload, dict) else []
        members: list[RosterMember] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            deputy_id = str(row.get("id") or row.get("idLegislatura") or "").strip()
            if not deputy_id:
                continue
            full_name = str(row.get("nome") or row.get("nomeCivil") or "").strip()
            parliamentary_name = str(row.get("nomeEleitoral") or row.get("nome") or full_name).strip()
            party = str(row.get("siglaPartido") or "").strip().upper()
            uf = str(row.get("siglaUf") or "").strip().upper()
            roster_url = str(row.get("uri") or response.url).strip()
            aliases = _dedupe([full_name, parliamentary_name])
            members.append(
                RosterMember(
                    scope="federal",
                    uf=uf or "BR",
                    source_plugin=self.name,
                    source_member_id=deputy_id,
                    full_name=full_name or parliamentary_name,
                    parliamentary_name=parliamentary_name or full_name,
                    party=party,
                    status="active_roster",
                    roster_url=roster_url,
                    roster_confidence=0.99,
                    roster_validated=True,
                    roster_source_kind="official_api",
                    aliases=aliases,
                    metadata={
                        "camara_id": deputy_id,
                        "deputy_page_url": roster_url,
                        "assembly_name": "Câmara dos Deputados",
                        "legislature_level": "federal",
                        "office": "Deputado Federal",
                    },
                )
            )
        return RosterResult(
            plugin=self.name,
            members=members,
            source_url=response.url,
            artifacts=[_artifact_json("camara-roster", response.url, payload)],
        )


class StateAssemblyRosterSource(RosterSource):
    scope = "state"

    def __init__(self, config: StateAssemblyConfig) -> None:
        self.config = config
        self.name = f"assembleia-{config.uf.lower()}-roster"

    def list_current_members(self) -> RosterResult:
        override = os.environ.get(f"INVESTIGADOR_ROSTER_{self.config.uf}_URL", "").strip()
        candidate_urls = []
        if override:
            candidate_urls.append(override)
        for path in self.config.candidate_paths:
            candidate_urls.append(parse.urljoin(self.config.base_url.rstrip("/") + "/", path.lstrip("/")))
        errors: list[str] = []
        artifacts: list[dict[str, Any]] = []
        for url in _dedupe(candidate_urls):
            try:
                html_text, response = fetch_text(url)
            except Exception as exc:
                errors.append(f"{url}: {exc}")
                continue
            artifacts.append(_artifact_html(f"{self.name}-{_slug_fragment(url)}", response.url, html_text))
            members = self._parse_members(html_text, response.url)
            validated, validation_error = self._validate_member_batch(members, response.url)
            if validated:
                return RosterResult(
                    plugin=self.name,
                    members=validated,
                    source_url=response.url,
                    artifacts=artifacts,
                    errors=errors,
                )
            if validation_error:
                errors.append(validation_error)
            else:
                errors.append(f"{response.url}: nenhuma deputada ou deputado atual identificado com o parser heurístico.")
        return RosterResult(plugin=self.name, members=[], source_url=candidate_urls[0] if candidate_urls else self.config.base_url, artifacts=artifacts, errors=errors)

    def _parse_members(self, html_text: str, page_url: str) -> list[RosterMember]:
        links = list(
            re.finditer(
                r"<a[^>]+href=['\"]([^'\"]+)['\"][^>]*>(.*?)</a>",
                html_text,
                flags=re.IGNORECASE | re.DOTALL,
            )
        )
        members: list[RosterMember] = []
        seen: set[str] = set()
        for match in links:
            href, raw_title = match.groups()
            title = _strip_tags(raw_title)
            if not _looks_like_person_name(title):
                continue
            block = html_text[max(0, match.start() - 600) : min(len(html_text), match.end() + 800)]
            party = _extract_party(_strip_tags(block))
            absolute_url = parse.urljoin(page_url, href)
            if not self._looks_like_member_profile_url(absolute_url, page_url):
                continue
            key = _normalize_text(title)
            if key in seen:
                continue
            seen.add(key)
            member_id = _candidate_id(self.name, absolute_url if absolute_url and absolute_url != page_url else title)
            confidence = self._member_confidence(title, absolute_url, block, party)
            members.append(
                RosterMember(
                    scope="state",
                    uf=self.config.uf,
                    source_plugin=self.name,
                    source_member_id=member_id,
                    full_name=title,
                    parliamentary_name=title,
                    party=party,
                    status="active_roster",
                    roster_url=absolute_url or page_url,
                    roster_confidence=confidence,
                    roster_validated=confidence >= 0.82,
                    roster_source_kind="official_html",
                    aliases=[title],
                    metadata={
                        "assembly_name": self.config.assembly_name,
                        "legislature_level": "state",
                        "office": "Deputado Estadual",
                        "deputy_page_url": absolute_url or page_url,
                    },
                )
            )
        return members

    def _looks_like_member_profile_url(self, absolute_url: str, page_url: str) -> bool:
        if not absolute_url or absolute_url == page_url:
            return False
        candidate = parse.urlsplit(absolute_url)
        source = parse.urlsplit(page_url)
        if not candidate.netloc or candidate.netloc.casefold() != source.netloc.casefold():
            return False
        path = _normalize_text(candidate.path)
        if not path:
            return False
        rejected_fragments = {
            "noticia",
            "noticias",
            "noticias-e-eventos",
            "noticias?page",
            "comissao",
            "comissoes",
            "comite",
            "ouvidoria",
            "diario",
            "regimento",
            "lei",
            "leis",
            "rss",
            "feed",
            "login",
            "transparencia",
            "concurso",
            "escola",
            "tv",
            "radio",
            "podcast",
            "memorial",
            "historia",
            "hist-ria",
            "acervo",
            "mapa-do-site",
            "agenda",
            "p-",
            "page-id",
            "cat-",
            "tag-",
            "wp-content",
            "portal",
            "sistema",
            "biblioteca",
            "projeto",
            "resolucao",
            "constituicao",
            "orcamento",
            "orcamento-estadual",
            "prestacao-de-contas",
            "contas",
            "documentacao",
            "departamento",
            "coordenadoria",
            "diretoria",
            "procuradoria",
        }
        if any(fragment in path for fragment in rejected_fragments):
            return False
        allowed_hints = {
            "deputad",
            "parlament",
            "perfil",
            "bio",
            "ficha",
            "author",
        }
        allowed_hints.update(_normalize_text(item).strip("/") for item in self.config.candidate_paths)
        allowed_hints.update(_normalize_text(item).strip("/") for item in self.config.member_path_hints)
        return any(hint and hint in path for hint in allowed_hints)

    def _member_confidence(self, title: str, absolute_url: str, block: str, party: str) -> float:
        confidence = 0.82
        path = _normalize_text(parse.urlsplit(absolute_url).path)
        block_text = _normalize_text(_strip_tags(block))
        if party:
            confidence += 0.06
        if any(keyword in path for keyword in ("deputad", "parlament", "perfil", "author", "bio", "ficha")):
            confidence += 0.06
        if "deputad" in block_text or "parlament" in block_text:
            confidence += 0.04
        if len(title.split()) >= 3:
            confidence += 0.02
        return min(confidence, 0.99)

    def _validate_member_batch(self, members: list[RosterMember], page_url: str) -> tuple[list[RosterMember], str]:
        if not members:
            return [], f"{page_url}: nenhuma deputada ou deputado atual identificado com validação estrita."
        validated = [member for member in members if member.roster_validated]
        if not validated:
            return [], f"{page_url}: candidatos encontrados, mas nenhum passou na validação estrita do roster."
        if len(validated) > 120:
            return [], f"{page_url}: quantidade implausível de parlamentares ({len(validated)}); roster rejeitado."
        if len(validated) >= 10 and sum(1 for item in validated if item.party) / len(validated) < 0.2:
            return [], f"{page_url}: roster com baixa qualidade estrutural; poucos membros com partido legível."
        return validated, ""


def list_roster_sources(scopes: Iterable[str]) -> list[RosterSource]:
    requested = {scope.strip().lower() for scope in scopes if str(scope).strip()}
    sources: list[RosterSource] = []
    if "federal" in requested:
        sources.append(CamaraRosterSource())
    if "state" in requested:
        for uf in UF_CODES:
            config = STATE_ASSEMBLIES.get(uf)
            if config:
                sources.append(StateAssemblyRosterSource(config))
    return sources
