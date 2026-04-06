from __future__ import annotations

import json
import os
import ssl
from datetime import UTC, datetime
from typing import Any
from urllib import error, request

from .messages import msg
from .models import AGENT_ROLES, AgentRunResult, Note, ProposedChange


def _utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


class AgentProvider:
    name = "base"

    def run(self, role: str, project: Note, context: dict[str, Any]) -> AgentRunResult:
        raise NotImplementedError


class MockAgentProvider(AgentProvider):
    name = "mock"

    def run(self, role: str, project: Note, context: dict[str, Any]) -> AgentRunResult:
        if role not in AGENT_ROLES:
            raise ValueError(msg("papel_desconhecido", role=role))
        summary = (
            f"Execução determinística para o papel `{role}` no projeto "
            f"`{project.frontmatter['project_slug']}` em {_utc_now()}."
        )
        return AgentRunResult(
            provider=self.name,
            role=role,
            content=summary,
            mode="offline",
            proposed_changes=[],
            raw_payload={"context_keys": sorted(context.keys())},
        )


class GeminiOpenAICompatibleProvider(AgentProvider):
    name = "gemini"

    def __init__(self) -> None:
        self.base_url = os.environ.get(
            "INVESTIGADOR_GEMINI_BASE_URL",
            "https://generativelanguage.googleapis.com/v1beta/openai",
        ).rstrip("/")
        self.model = os.environ.get("INVESTIGADOR_GEMINI_MODEL", "gemini-3.1-flash-lite-preview")
        self.api_key = os.environ.get("INVESTIGADOR_GEMINI_API_KEY", "")

    def run(self, role: str, project: Note, context: dict[str, Any]) -> AgentRunResult:
        if role not in AGENT_ROLES:
            raise ValueError(msg("papel_desconhecido", role=role))
        prompt = (
            "Você é um agente do Investigador CLI. Trabalhe apenas com linguagem de hipótese, "
            "cite lacunas probatórias e nunca conclua culpa.\n\n"
            f"Papel: {role}\n"
            f"Projeto: {project.frontmatter['title']}\n"
            f"Contexto: {json.dumps(context, ensure_ascii=False)}"
        )
        if not self.api_key:
            return AgentRunResult(
                provider=self.name,
                role=role,
                content="Dry-run do provider Gemini: chave não configurada.",
                mode="dry_run",
                raw_payload={"prompt": prompt, "base_url": self.base_url, "model": self.model},
            )
        payload = json.dumps(
            {
                "model": self.model,
                "messages": [
                    {"role": "system", "content": "Responda em português e use linguagem cautelosa."},
                    {"role": "user", "content": prompt},
                ],
            }
        ).encode("utf-8")
        http_request = request.Request(
            url=f"{self.base_url}/chat/completions",
            data=payload,
            method="POST",
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            },
        )
        try:
            with request.urlopen(http_request, timeout=30) as response:
                body = json.loads(response.read().decode("utf-8"))
        except (error.URLError, error.HTTPError, TimeoutError, ConnectionError, ssl.SSLError, OSError) as exc:
            return AgentRunResult(
                provider=self.name,
                role=role,
                content=f"Dry-run do provider Gemini após falha HTTP: {exc}",
                mode="dry_run",
                raw_payload={"prompt": prompt, "error": str(exc)},
            )
        content = ""
        choices = body.get("choices") or []
        if choices:
            content = choices[0].get("message", {}).get("content", "")
        return AgentRunResult(
            provider=self.name,
            role=role,
            content=content or "Resposta vazia do provider Gemini.",
            mode="remote",
            raw_payload=body,
        )


PROVIDERS = {
    "mock": MockAgentProvider,
    "gemini": GeminiOpenAICompatibleProvider,
}


def get_provider(name: str | None = None) -> AgentProvider:
    resolved = name or os.environ.get("INVESTIGADOR_AGENT_PROVIDER", "mock")
    try:
        return PROVIDERS[resolved]()
    except KeyError as exc:
        known = ", ".join(sorted(PROVIDERS))
        raise ValueError(msg("provedor_desconhecido", provider=resolved, known=known)) from exc


def deterministic_proposals(role: str, context: dict[str, Any]) -> list[ProposedChange]:
    if role == "orchestrator":
        return [
            ProposedChange(
                action="create_task",
                note_type="task",
                payload={
                    "title": f"Revisar cobertura das fontes para {context['project_slug']}",
                    "instructions": "Conferir lacunas, validar cronologia e priorizar próxima coleta pública.",
                },
                rationale="Toda investigação deve registrar próximas ações auditáveis.",
                confidence=0.74,
            )
        ]
    if role == "collector_analyst":
        return [
            ProposedChange(
                action="create_task",
                note_type="task",
                payload={
                    "title": f"Analisar lacunas de coleta em {context['project_slug']}",
                    "instructions": "Comparar cobertura atual de plugins com hipóteses e evidências já coletadas.",
                },
                rationale="A etapa de análise precisa explicitar lacunas documentais.",
                confidence=0.7,
            )
        ]
    if role == "skeptic":
        return [
            ProposedChange(
                action="create_task",
                note_type="task",
                payload={
                    "title": f"Contestação do caso {context['project_slug']}",
                    "instructions": "Buscar homônimos, explicações normativas e contraevidências para reduzir falsos positivos.",
                },
                rationale="O papel cético precisa estar presente antes do dossiê.",
                confidence=0.83,
            )
        ]
    return []
