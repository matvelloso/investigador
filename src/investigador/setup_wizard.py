from __future__ import annotations

import getpass
import importlib.util
import os
import shlex
import ssl
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Callable
from urllib import error

from .http import fetch_json


InputFn = Callable[[str], str]


@dataclass(frozen=True, slots=True)
class SecretPrompt:
    name: str
    label: str
    help_text: str
    secret: bool = True
    default: str | None = None


PROMPTS = (
    SecretPrompt(
        name="INVESTIGADOR_GEMINI_API_KEY",
        label="Chave de API do Gemini",
        help_text="Usada pelo provedor de agentes compatível com OpenAI para Gemini.",
    ),
    SecretPrompt(
        name="INVESTIGADOR_PORTAL_API_KEY",
        label="Portal da Transparência token",
        help_text="Usado na API oficial do Portal da Transparência (header `chave-api-dados`).",
    ),
    SecretPrompt(
        name="INVESTIGADOR_DATAJUD_API_KEY",
        label="Chave de API do DataJud",
        help_text="Usada na API pública do DataJud do CNJ.",
    ),
    SecretPrompt(
        name="INVESTIGADOR_DOU_COOKIE",
        label="Cookie do INLABS / DOU",
        help_text="Usado para acesso autenticado a XML/ZIP do INLABS quando necessário.",
    ),
    SecretPrompt(
        name="INVESTIGADOR_WEB_SEARCH_PROVIDER",
        label="Provedor de busca contextual na web",
        help_text="Usado pelo plugin `web-search` para descoberta secundária e cronologia. O parser embutido atual espera HTML do DuckDuckGo.",
        secret=False,
        default="duckduckgo_html",
    ),
    SecretPrompt(
        name="INVESTIGADOR_WEB_SEARCH_URL",
        label="URL da busca contextual na web",
        help_text="Endpoint HTML de busca usado pelo plugin `web-search`. O valor padrão funciona sem chave de API na maioria dos ambientes.",
        secret=False,
        default="https://html.duckduckgo.com/html/",
    ),
)


DEFAULT_ENV = {
    "INVESTIGADOR_GEMINI_BASE_URL": "https://generativelanguage.googleapis.com/v1beta/openai",
    "INVESTIGADOR_GEMINI_MODEL": "gemini-3.1-flash-lite-preview",
    "INVESTIGADOR_CNPJ_BASE_URL": "https://brasilapi.com.br/api/cnpj/v1",
    "INVESTIGADOR_WEB_SEARCH_PROVIDER": "duckduckgo_html",
    "INVESTIGADOR_WEB_SEARCH_URL": "https://html.duckduckgo.com/html/",
}


def setup_env_path(root: Path) -> Path:
    return root / ".investigador" / "env.sh"


def _parse_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :]
        if "=" not in line:
            continue
        key, raw_value = line.split("=", 1)
        key = key.strip()
        raw_value = raw_value.strip()
        if not key:
            continue
        try:
            values[key] = shlex.split(raw_value)[0] if raw_value else ""
        except ValueError:
            values[key] = raw_value.strip("'\"")
    return values


def _render_env_file(values: dict[str, str]) -> str:
    lines = [
        "# Ambiente do Investigador",
        "# Gerado por `python3 -m investigador setup`.",
        "# Carregue este arquivo no shell antes de rodar sincronizações reais:",
        "#   source .investigador/env.sh",
        "",
    ]
    for key in sorted(values):
        if values[key] == "":
            continue
        lines.append(f"export {key}={shlex.quote(values[key])}")
    lines.append("")
    return "\n".join(lines)


def _print_header() -> None:
    print("Configuração do Investigador")
    print("")
    print("Este assistente verifica o básico de Python/SSL e depois pede credenciais e padrões dos conectores, um por um.")
    print("Pressione Enter em qualquer pergunta para pular por enquanto.")
    print("")


def _prompt_yes_no(message: str, default: bool = False, input_fn: InputFn = input) -> bool:
    suffix = "[S/n]" if default else "[s/N]"
    answer = input_fn(f"{message} {suffix} ").strip().lower()
    if not answer:
        return default
    return answer in {"s", "sim", "y", "yes"}


def _find_macos_certificate_script() -> Path | None:
    candidates = [
        Path(f"/Applications/Python {sys.version_info.major}.{sys.version_info.minor}/Install Certificates.command"),
        Path("/Applications/Python 3.13/Install Certificates.command"),
        Path("/Applications/Python 3.12/Install Certificates.command"),
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def _certifi_path() -> str | None:
    if importlib.util.find_spec("certifi") is None:
        return None
    import certifi

    return certifi.where()


def _install_certifi(python_executable: str) -> bool:
    command = [python_executable, "-m", "pip", "install", "--upgrade", "certifi"]
    print(f"Executando: {' '.join(command)}")
    result = subprocess.run(command, check=False)
    return result.returncode == 0


def _apply_ca_env(env_values: dict[str, str], ca_path: str | None) -> None:
    if not ca_path:
        return
    env_values["SSL_CERT_FILE"] = ca_path
    os.environ["SSL_CERT_FILE"] = ca_path
    os.environ["REQUESTS_CA_BUNDLE"] = ca_path


def _run_install_certificates(script_path: Path) -> bool:
    print(f"Executando: /bin/bash {shlex.quote(str(script_path))}")
    result = subprocess.run(["/bin/bash", str(script_path)], check=False)
    return result.returncode == 0


def _https_probe() -> tuple[bool, str]:
    try:
        payload, _ = fetch_json(
            "https://dadosabertos.tse.jus.br/api/3/action/package_search",
            query={"q": "candidatos", "rows": 1},
            timeout=15,
        )
    except error.URLError as exc:
        return False, str(exc)
    except Exception as exc:  # pragma: no cover - defensive runtime fallback
        return False, str(exc)
    success = bool(payload.get("success")) if isinstance(payload, dict) else True
    return success, "Teste HTTPS para o TSE concluído com sucesso."


def _configure_certificates(root: Path, env_values: dict[str, str], input_fn: InputFn = input) -> list[str]:
    notes: list[str] = []
    print("Configuração de SSL / certificados")
    print(f"- Executável do Python: {sys.executable}")
    print(f"- Versão do Python: {sys.version.split()[0]}")
    verify_paths = ssl.get_default_verify_paths()
    print(f"- Arquivo CA do OpenSSL: {verify_paths.openssl_cafile or 'não definido'}")
    print("")

    certifi_path = _certifi_path()
    if certifi_path:
        _apply_ca_env(env_values, certifi_path)
        print(f"Usando o bundle CA do certifi: {certifi_path}")
    else:
        print("O pacote certifi não está instalado no momento.")
        if _prompt_yes_no("Instalar/atualizar o certifi agora?", default=True, input_fn=input_fn):
            if _install_certifi(sys.executable):
                certifi_path = _certifi_path()
                if certifi_path:
                    _apply_ca_env(env_values, certifi_path)
                    print(f"Certifi instalado e SSL_CERT_FILE definido como {certifi_path}")
                else:
                    notes.append("A instalação do certifi terminou, mas o módulo não pôde ser importado em seguida.")
            else:
                notes.append("Não foi possível instalar o certifi automaticamente.")
    if sys.platform == "darwin":
        script_path = _find_macos_certificate_script()
        if script_path and _prompt_yes_no("Executar o assistente `Install Certificates.command` do macOS?", default=False, input_fn=input_fn):
            if _run_install_certificates(script_path):
                notes.append("O assistente de certificados do macOS foi concluído. O teste HTTPS abaixo usa o ambiente atual do processo.")
            else:
                notes.append("Não foi possível executar `Install Certificates.command` automaticamente.")
    print("")
    ok, message = _https_probe()
    if ok:
        print(message)
    else:
        print(f"O teste HTTPS falhou: {message}")
        manual_path = input_fn("Opcional: informe um caminho de bundle CA personalizado para SSL_CERT_FILE (ou pressione Enter para pular): ").strip()
        if manual_path:
            expanded = str(Path(manual_path).expanduser().resolve())
            if Path(expanded).exists():
                _apply_ca_env(env_values, expanded)
                print(f"Usando bundle CA manual: {expanded}")
                ok, message = _https_probe()
                if ok:
                    print(message)
                else:
                    print(f"O teste HTTPS continuou falhando mesmo após configurar o bundle CA manual: {message}")
                    notes.append(
                        "A verificação HTTPS ainda está falhando mesmo com o bundle CA informado. Isso geralmente significa que algum proxy, VPN ou ferramenta de segurança está interceptando o TLS com outro certificado-raiz."
                    )
            else:
                notes.append(f"O caminho informado para o bundle CA não existe: {expanded}")
        if not ok:
            notes.append(
                "A verificação HTTPS ainda está falhando. Carregue o arquivo de ambiente gerado e, se necessário, aponte SSL_CERT_FILE para o bundle CA da sua empresa ou autoridade-raiz."
            )
    print("")
    return notes


def _prompt_for_missing_values(existing: dict[str, str], input_fn: InputFn = input) -> dict[str, str]:
    values = dict(existing)
    for prompt in PROMPTS:
        current = values.get(prompt.name) or os.environ.get(prompt.name, "")
        if current:
            print(f"{prompt.label}: já configurado no ambiente ou no arquivo env existente.")
            values[prompt.name] = current
            continue
        print(f"{prompt.label}")
        print(f"  {prompt.help_text}")
        prompt_suffix = "ou pressione Enter para pular"
        if prompt.default:
            prompt_suffix = f"ou pressione Enter para usar o padrão: {prompt.default}"
        if prompt.secret and sys.stdin.isatty() and input_fn is input:
            entered = getpass.getpass(f"  Informe {prompt.name} ({prompt_suffix}): ").strip()
        else:
            entered = input_fn(f"  Informe {prompt.name} ({prompt_suffix}): ").strip()
        if entered:
            values[prompt.name] = entered
        elif prompt.default:
            print(f"  Usando o padrão: {prompt.default}")
        print("")
    return values


def _finalize_defaults(values: dict[str, str]) -> dict[str, str]:
    finalized = dict(values)
    for key, value in DEFAULT_ENV.items():
        finalized.setdefault(key, value)
    if finalized.get("INVESTIGADOR_GEMINI_API_KEY"):
        finalized.setdefault("INVESTIGADOR_AGENT_PROVIDER", "gemini")
    return finalized


def run_setup(root: Path, *, input_fn: InputFn = input) -> Path:
    root = root.resolve()
    (root / ".investigador").mkdir(parents=True, exist_ok=True)
    env_path = setup_env_path(root)
    existing = _parse_env_file(env_path)

    _print_header()
    notes = _configure_certificates(root, existing, input_fn=input_fn)
    updated = _prompt_for_missing_values(existing, input_fn=input_fn)
    finalized = _finalize_defaults(updated)
    env_path.write_text(_render_env_file(finalized), encoding="utf-8")

    print("Arquivo de ambiente gravado:")
    print(f"  {env_path}")
    print("")
    print("Próximos passos:")
    print(f"  source {shlex.quote(str(env_path))}")
    print("  python3 -m investigador validate")
    print("  python3 -m investigador source sync <project> --plugin web-search")
    print("  python3 -m investigador project advance <project>")
    print("  python3 -m investigador source sync <project> --plugin tse --plugin datajud")
    if notes:
        print("")
        print("Observações:")
        for note in notes:
            print(f"- {note}")
    return env_path
