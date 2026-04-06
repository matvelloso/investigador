from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .core import (
    add_target,
    advance_project,
    build_dossier,
    create_project,
    diagnose_project,
    format_progress_event,
    init_workspace,
    refresh_project_materialized_views,
    run_agent,
    sync_sources_detailed,
    update_target,
    validate_workspace,
)
from .portfolio import (
    build_portfolio_leaderboard,
    create_portfolio,
    diagnose_portfolio,
    portfolio_tick,
    portfolio_status,
    repair_portfolio,
    run_portfolio,
    sync_portfolio_roster,
)
from .messages import msg
from .setup_wizard import run_setup

try:  # pragma: no cover - exercised only when Typer is installed
    import typer
except ImportError:  # pragma: no cover - default in this sandbox
    typer = None


def _resolve_root(root: str | None) -> Path:
    return Path(root or ".").resolve()


def _project_progress_printer(event: dict[str, object]) -> None:
    message = format_progress_event(event)
    if message:
        print(message)


TOP_LEVEL_ALIASES = {
    "inicializar": "init",
    "configurar": "setup",
    "projeto": "project",
    "fonte": "source",
    "agente": "agent",
    "dossie": "dossier",
    "validar": "validate",
}

PROJECT_COMMAND_ALIASES = {
    "criar": "create",
    "adicionar-alvo": "add-target",
    "atualizar-alvo": "update-target",
    "atualizar": "refresh",
    "diagnosticar": "diagnose",
    "avancar": "advance",
}

SOURCE_COMMAND_ALIASES = {
    "sincronizar": "sync",
}

AGENT_COMMAND_ALIASES = {
    "executar": "run",
}

DOSSIER_COMMAND_ALIASES = {
    "gerar": "build",
}

PORTFOLIO_COMMAND_ALIASES = {
    "criar": "create",
    "sincronizar-roster": "sync-roster",
    "reparar": "repair",
    "ciclo": "tick",
    "executar": "run",
    "ranking": "leaderboard",
    "diagnosticar": "diagnose",
    "estado": "status",
}


def _parse_metadata(entries: list[str] | None) -> dict[str, object]:
    metadata: dict[str, object] = {}
    for entry in entries or []:
        if "=" not in entry:
            raise ValueError(msg("entrada_metadata_invalida", entry=entry))
        key, raw_value = entry.split("=", 1)
        value = raw_value.strip()
        if not key.strip():
            raise ValueError(msg("chave_metadata_vazia", entry=entry))
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            lowered = value.lower()
            if lowered in {"true", "false"}:
                parsed = lowered == "true"
            else:
                try:
                    parsed = int(value)
                except ValueError:
                    try:
                        parsed = float(value)
                    except ValueError:
                        parsed = value
        metadata[key.strip()] = parsed
    return metadata


def _canonicalize(value: str | None, aliases: dict[str, str]) -> str | None:
    if value is None:
        return None
    return aliases.get(value, value)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="investigador",
        description="CLI investigativa orientada a Markdown para rastrear indícios públicos com proveniência auditável.",
    )
    parser.add_argument("--root", default=".", help="Diretório raiz da área de trabalho.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("init", aliases=["inicializar"], help="Inicializa a área de trabalho local.")
    subparsers.add_parser("setup", aliases=["configurar"], help="Executa o assistente guiado de configuração.")

    project = subparsers.add_parser("project", aliases=["projeto"], help="Gerencia projetos investigativos.")
    project_sub = project.add_subparsers(dest="project_command", required=True)

    create = project_sub.add_parser("create", aliases=["criar"], help="Cria um novo projeto.")
    create.add_argument("slug")
    create.add_argument("--title", default=None, help="Título legível do projeto.")
    create.add_argument("--meta", action="append", default=[], help="Metadado extra no formato chave=valor.")

    add_target_parser = project_sub.add_parser("add-target", aliases=["adicionar-alvo"], help="Adiciona um alvo ao projeto.")
    add_target_parser.add_argument("project")
    add_target_parser.add_argument("entity_type")
    add_target_parser.add_argument("identifier")
    add_target_parser.add_argument("--title", default=None, help="Nome legível do alvo.")
    add_target_parser.add_argument("--alias", action="append", default=[], help="Alias adicional para o alvo.")
    add_target_parser.add_argument("--meta", action="append", default=[], help="Metadado extra no formato chave=valor.")

    update_target_parser = project_sub.add_parser("update-target", aliases=["atualizar-alvo"], help="Atualiza um alvo existente.")
    update_target_parser.add_argument("project")
    update_target_parser.add_argument("target")
    update_target_parser.add_argument("--title", default=None, help="Novo nome legível do alvo.")
    update_target_parser.add_argument("--alias", action="append", default=[], help="Alias adicional para o alvo.")
    update_target_parser.add_argument("--meta", action="append", default=[], help="Metadado extra no formato chave=valor.")

    refresh_target_parser = project_sub.add_parser("refresh", aliases=["atualizar"], help="Regenera as notas materializadas do projeto.")
    refresh_target_parser.add_argument("project")

    diagnose_project_parser = project_sub.add_parser("diagnose", aliases=["diagnosticar"], help="Exibe o diagnóstico estruturado do projeto.")
    diagnose_project_parser.add_argument("project")

    advance_parser = project_sub.add_parser("advance", aliases=["avancar"], help="Executa o ciclo supervisionado do projeto.")
    advance_parser.add_argument("project")
    advance_parser.add_argument("--provider", default=None, help="Provedor de agente a utilizar.")
    advance_parser.add_argument("--mode", default="deep", choices=["baseline", "deep"], help="Modo de execução: baseline ou deep.")

    portfolio = subparsers.add_parser("portfolio", help="Opera um portfólio de projetos.")
    portfolio_sub = portfolio.add_subparsers(dest="portfolio_command", required=True)

    portfolio_create = portfolio_sub.add_parser("create", aliases=["criar"], help="Cria um novo portfólio.")
    portfolio_create.add_argument("slug")
    portfolio_create.add_argument("--population", default="deputados-current", help="População inicial do portfólio.")
    portfolio_create.add_argument("--scope", default="federal,state", help="Escopo inicial separado por vírgulas.")

    portfolio_sync = portfolio_sub.add_parser("sync-roster", aliases=["sincronizar-roster"], help="Sincroniza o roster oficial do portfólio.")
    portfolio_sync.add_argument("slug")

    portfolio_repair = portfolio_sub.add_parser("repair", aliases=["reparar"], help="Executa a migração incremental do portfólio.")
    portfolio_repair.add_argument("slug")
    portfolio_repair.add_argument("--scope", default="federal", choices=["federal", "state", "all"])
    portfolio_repair.add_argument("--batch-size", type=int, default=25)
    portfolio_repair.add_argument("--resume", dest="resume", action="store_true")
    portfolio_repair.add_argument("--no-resume", dest="resume", action="store_false")
    portfolio_repair.set_defaults(resume=True)

    portfolio_tick_parser = portfolio_sub.add_parser("tick", aliases=["ciclo"], help="Processa um ciclo da fila do portfólio.")
    portfolio_tick_parser.add_argument("slug")
    portfolio_tick_parser.add_argument("--max-projects", type=int, default=200)
    portfolio_tick_parser.add_argument("--provider", default=None)
    portfolio_tick_parser.add_argument("--max-concurrent", type=int, default=4)
    portfolio_tick_parser.add_argument("--sync-roster", default="auto", choices=["auto", "force", "skip"])
    portfolio_tick_parser.add_argument("--roster-max-age-hours", type=int, default=24)
    portfolio_tick_parser.add_argument("--scope", default="all", choices=["federal", "state", "all"])
    portfolio_tick_parser.add_argument("--only-status", default="all", choices=["active_validated", "all"])

    portfolio_run_parser = portfolio_sub.add_parser("run", aliases=["executar"], help="Executa o worker contínuo do portfólio.")
    portfolio_run_parser.add_argument("slug")
    portfolio_run_parser.add_argument("--loop", action="store_true")
    portfolio_run_parser.add_argument("--max-projects", type=int, default=200)
    portfolio_run_parser.add_argument("--sleep-seconds", type=int, default=300)
    portfolio_run_parser.add_argument("--max-concurrent", type=int, default=4)
    portfolio_run_parser.add_argument("--provider", default=None)
    portfolio_run_parser.add_argument("--sync-roster", default="auto", choices=["auto", "force", "skip"])
    portfolio_run_parser.add_argument("--roster-max-age-hours", type=int, default=24)
    portfolio_run_parser.add_argument("--scope", default="all", choices=["federal", "state", "all"])
    portfolio_run_parser.add_argument("--only-status", default="all", choices=["active_validated", "all"])

    portfolio_leaderboard = portfolio_sub.add_parser("leaderboard", aliases=["ranking"], help="Gera o ranking do portfólio.")
    portfolio_leaderboard.add_argument("slug")

    portfolio_diagnose = portfolio_sub.add_parser("diagnose", aliases=["diagnosticar"], help="Exibe o diagnóstico do portfólio.")
    portfolio_diagnose.add_argument("slug")
    portfolio_diagnose.add_argument("--limit", type=int, default=25)

    portfolio_status_parser = portfolio_sub.add_parser("status", aliases=["estado"], help="Exibe o status operacional do portfólio.")
    portfolio_status_parser.add_argument("slug")

    source = subparsers.add_parser("source", aliases=["fonte"], help="Executa coletores de fonte.")
    source_sub = source.add_subparsers(dest="source_command", required=True)
    sync = source_sub.add_parser("sync", aliases=["sincronizar"], help="Sincroniza uma ou mais fontes para um projeto.")
    sync.add_argument("project")
    sync.add_argument("--plugin", action="append", default=[], help="Nome técnico do plugin a executar.")

    agent = subparsers.add_parser("agent", aliases=["agente"], help="Executa papéis de agente sobre um projeto.")
    agent_sub = agent.add_subparsers(dest="agent_command", required=True)
    run = agent_sub.add_parser("run", aliases=["executar"], help="Executa um papel de agente.")
    run.add_argument("project")
    run.add_argument("--role", required=True)
    run.add_argument("--provider", default=None)

    dossier = subparsers.add_parser("dossier", aliases=["dossie"], help="Gera dossiês derivados.")
    dossier_sub = dossier.add_subparsers(dest="dossier_command", required=True)
    build = dossier_sub.add_parser("build", aliases=["gerar"], help="Gera o dossiê de um projeto.")
    build.add_argument("project")

    validate = subparsers.add_parser("validate", aliases=["validar"], help="Valida a área de trabalho atual.")
    validate.add_argument(
        "--publish-mode",
        "--modo-publicacao",
        action="store_true",
        help="Executa a auditoria de publicação segura para um repositório source-only.",
    )
    return parser


def _render_sync_report(report: dict[str, object]) -> list[str]:
    lines: list[str] = []
    for plugin in report.get("plugins", []):
        plugin_name = str(plugin.get("plugin", ""))
        stage = str(plugin.get("stage", "specific"))
        record_count = int(plugin.get("record_count", 0))
        written_paths = [Path(item) for item in plugin.get("written_paths", [])]
        lines.append(f"[{stage}] {plugin_name}: {record_count} registro(s), {len(written_paths)} nota(s) gravada(s)")
        error = str(plugin.get("error", "")).strip()
        if error:
            lines.append(f"  erro: {error}")
        for update in plugin.get("applied_metadata_updates", []):
            metadata_keys = ", ".join(update.get("metadata_keys", []))
            lines.append(f"  metadata -> {update.get('path')}: {metadata_keys}")
        next_queries = plugin.get("next_queries", [])
        if next_queries:
            lines.append("  próximos passos sugeridos:")
            for suggestion in next_queries:
                lines.append(f"    - {suggestion}")
        for path in written_paths:
            lines.append(str(path))
    return lines


def _handle_cli(args: argparse.Namespace) -> int:
    root = _resolve_root(args.root)
    command = _canonicalize(args.command, TOP_LEVEL_ALIASES)
    project_command = _canonicalize(getattr(args, "project_command", None), PROJECT_COMMAND_ALIASES)
    portfolio_command = _canonicalize(getattr(args, "portfolio_command", None), PORTFOLIO_COMMAND_ALIASES)
    source_command = _canonicalize(getattr(args, "source_command", None), SOURCE_COMMAND_ALIASES)
    agent_command = _canonicalize(getattr(args, "agent_command", None), AGENT_COMMAND_ALIASES)
    dossier_command = _canonicalize(getattr(args, "dossier_command", None), DOSSIER_COMMAND_ALIASES)
    if command == "init":
        init_workspace(root)
        print(msg("area_trabalho_inicializada", root=root))
        return 0
    if command == "setup":
        run_setup(root)
        return 0
    if command == "project" and project_command == "create":
        path = create_project(root, args.slug, args.title, _parse_metadata(args.meta))
        print(path)
        return 0
    if command == "project" and project_command == "add-target":
        path = add_target(
            root,
            args.project,
            args.entity_type,
            args.identifier,
            args.title,
            aliases=args.alias,
            metadata=_parse_metadata(args.meta),
        )
        print(path)
        return 0
    if command == "project" and project_command == "update-target":
        path = update_target(
            root,
            args.project,
            args.target,
            args.title,
            aliases=args.alias,
            metadata=_parse_metadata(args.meta),
        )
        print(path)
        return 0
    if command == "project" and project_command == "refresh":
        for path in refresh_project_materialized_views(root, args.project):
            print(path)
        return 0
    if command == "project" and project_command == "diagnose":
        print(json.dumps(diagnose_project(root, args.project), indent=2, ensure_ascii=False))
        return 0
    if command == "project" and project_command == "advance":
        print(advance_project(root, args.project, args.provider, args.mode, progress=_project_progress_printer))
        return 0
    if command == "portfolio" and portfolio_command == "create":
        scope = [item.strip() for item in str(args.scope).split(",") if item.strip()]
        print(create_portfolio(root, args.slug, args.population, scope))
        return 0
    if command == "portfolio" and portfolio_command == "sync-roster":
        print(sync_portfolio_roster(root, args.slug, progress=print))
        return 0
    if command == "portfolio" and portfolio_command == "repair":
        print(
            repair_portfolio(
                root,
                args.slug,
                scope=args.scope,
                batch_size=args.batch_size,
                resume=args.resume,
                progress=print,
            )
        )
        return 0
    if command == "portfolio" and portfolio_command == "tick":
        print(
            portfolio_tick(
                root,
                args.slug,
                max_projects=args.max_projects,
                provider_name=args.provider,
                max_concurrent=args.max_concurrent,
                sync_roster_mode=args.sync_roster,
                roster_max_age_hours=args.roster_max_age_hours,
                scope=args.scope,
                only_status=args.only_status,
                progress=print,
            )
        )
        return 0
    if command == "portfolio" and portfolio_command == "run":
        print(
            run_portfolio(
                root,
                args.slug,
                loop=args.loop,
                max_projects=args.max_projects,
                sleep_seconds=args.sleep_seconds,
                max_concurrent=args.max_concurrent,
                provider_name=args.provider,
                sync_roster_mode=args.sync_roster,
                roster_max_age_hours=args.roster_max_age_hours,
                scope=args.scope,
                only_status=args.only_status,
                progress=print,
            )
        )
        return 0
    if command == "portfolio" and portfolio_command == "leaderboard":
        print(build_portfolio_leaderboard(root, args.slug))
        return 0
    if command == "portfolio" and portfolio_command == "diagnose":
        print(json.dumps(diagnose_portfolio(root, args.slug, limit=args.limit), indent=2, ensure_ascii=False))
        return 0
    if command == "portfolio" and portfolio_command == "status":
        print(json.dumps(portfolio_status(root, args.slug), indent=2, ensure_ascii=False))
        return 0
    if command == "source" and source_command == "sync":
        report = sync_sources_detailed(root, args.project, args.plugin or None)
        for line in _render_sync_report(report):
            print(line)
        return 0
    if command == "agent" and agent_command == "run":
        path = run_agent(root, args.project, args.role, args.provider)
        print(path)
        return 0
    if command == "dossier" and dossier_command == "build":
        path = build_dossier(root, args.project)
        print(path)
        return 0
    if command == "validate":
        errors = validate_workspace(root, publish_mode=bool(getattr(args, "publish_mode", False)))
        if errors:
            for item in errors:
                print(item, file=sys.stderr)
            return 1
        print(msg("area_trabalho_valida"))
        return 0
    raise ValueError(msg("combinacao_cli_invalida"))


def _build_typer_app():  # pragma: no cover - exercised only when Typer is installed
    app = typer.Typer(
        no_args_is_help=True,
        help="CLI investigativa orientada a Markdown para rastrear indícios públicos com proveniência auditável.",
    )
    project_app = typer.Typer(no_args_is_help=True, help="Opera projetos investigativos.")
    source_app = typer.Typer(no_args_is_help=True, help="Executa coletores de fonte.")
    agent_app = typer.Typer(no_args_is_help=True, help="Executa papéis de agente.")
    dossier_app = typer.Typer(no_args_is_help=True, help="Gera dossiês derivados.")
    portfolio_app = typer.Typer(no_args_is_help=True, help="Opera portfólios de projetos.")

    @app.command("init", help="Inicializa a área de trabalho local.")
    @app.command("inicializar", help="Alias em português de `init`.")
    def init(root: str = typer.Option(".", "--root", help="Diretório raiz da área de trabalho.")) -> None:
        init_workspace(_resolve_root(root))
        typer.echo(msg("area_trabalho_inicializada", root=_resolve_root(root)))

    @app.command("setup", help="Executa o assistente guiado de configuração.")
    @app.command("configurar", help="Alias em português de `setup`.")
    def setup(root: str = typer.Option(".", "--root", help="Diretório raiz da área de trabalho.")) -> None:
        run_setup(_resolve_root(root))

    @project_app.command("create", help="Cria um novo projeto.")
    @project_app.command("criar", help="Alias em português de `create`.")
    def project_create(
        slug: str,
        title: str | None = typer.Option(None, "--title", help="Título legível do projeto."),
        meta: list[str] = typer.Option([], "--meta", help="Metadado extra no formato chave=valor."),
        root: str = typer.Option(".", "--root", help="Diretório raiz da área de trabalho."),
    ) -> None:
        typer.echo(create_project(_resolve_root(root), slug, title, _parse_metadata(meta)))

    @project_app.command("add-target", help="Adiciona um alvo ao projeto.")
    @project_app.command("adicionar-alvo", help="Alias em português de `add-target`.")
    def project_add_target(
        project: str,
        entity_type: str,
        identifier: str,
        title: str | None = typer.Option(None, "--title", help="Nome legível do alvo."),
        alias: list[str] = typer.Option([], "--alias", help="Alias adicional para o alvo."),
        meta: list[str] = typer.Option([], "--meta", help="Metadado extra no formato chave=valor."),
        root: str = typer.Option(".", "--root", help="Diretório raiz da área de trabalho."),
        ) -> None:
        typer.echo(
            add_target(
                _resolve_root(root),
                project,
                entity_type,
                identifier,
                title,
                aliases=alias,
                metadata=_parse_metadata(meta),
            )
        )

    @project_app.command("update-target", help="Atualiza um alvo já existente.")
    @project_app.command("atualizar-alvo", help="Alias em português de `update-target`.")
    def project_update_target(
        project: str,
        target: str,
        title: str | None = typer.Option(None, "--title", help="Novo nome legível do alvo."),
        alias: list[str] = typer.Option([], "--alias", help="Alias adicional para o alvo."),
        meta: list[str] = typer.Option([], "--meta", help="Metadado extra no formato chave=valor."),
        root: str = typer.Option(".", "--root", help="Diretório raiz da área de trabalho."),
        ) -> None:
        typer.echo(
            update_target(
                _resolve_root(root),
                project,
                target,
                title,
                aliases=alias,
                metadata=_parse_metadata(meta),
            )
        )

    @project_app.command("refresh", help="Regenera as notas materializadas do projeto.")
    @project_app.command("atualizar", help="Alias em português de `refresh`.")
    def project_refresh(
        project: str,
        root: str = typer.Option(".", "--root", help="Diretório raiz da área de trabalho."),
    ) -> None:
        for path in refresh_project_materialized_views(_resolve_root(root), project):
            typer.echo(path)

    @project_app.command("diagnose", help="Exibe o diagnóstico estruturado do projeto.")
    @project_app.command("diagnosticar", help="Alias em português de `diagnose`.")
    def project_diagnose(
        project: str,
        root: str = typer.Option(".", "--root", help="Diretório raiz da área de trabalho."),
    ) -> None:
        typer.echo(json.dumps(diagnose_project(_resolve_root(root), project), indent=2, ensure_ascii=False))

    @project_app.command("advance", help="Executa o ciclo supervisionado do projeto.")
    @project_app.command("avancar", help="Alias em português de `advance`.")
    def project_advance(
        project: str,
        provider: str | None = typer.Option(None, "--provider", help="Provedor de agente a utilizar."),
        mode: str = typer.Option("deep", "--mode", help="Modo de execução: baseline ou deep."),
        root: str = typer.Option(".", "--root", help="Diretório raiz da área de trabalho."),
    ) -> None:
        typer.echo(
            advance_project(
                _resolve_root(root),
                project,
                provider,
                mode,
                progress=lambda event: typer.echo(format_progress_event(event)) if format_progress_event(event) else None,
            )
        )

    @portfolio_app.command("create", help="Cria um novo portfólio.")
    @portfolio_app.command("criar", help="Alias em português de `create`.")
    def portfolio_create(
        slug: str,
        population: str = typer.Option("deputados-current", "--population"),
        scope: str = typer.Option("federal,state", "--scope"),
        root: str = typer.Option(".", "--root", help="Diretório raiz da área de trabalho."),
    ) -> None:
        typer.echo(create_portfolio(_resolve_root(root), slug, population, [item.strip() for item in scope.split(",") if item.strip()]))

    @portfolio_app.command("sync-roster", help="Sincroniza o roster oficial do portfólio.")
    @portfolio_app.command("sincronizar-roster", help="Alias em português de `sync-roster`.")
    def portfolio_sync_roster(
        slug: str,
        root: str = typer.Option(".", "--root", help="Diretório raiz da área de trabalho."),
    ) -> None:
        typer.echo(sync_portfolio_roster(_resolve_root(root), slug, progress=typer.echo))

    @portfolio_app.command("repair", help="Executa a migração incremental do portfólio.")
    @portfolio_app.command("reparar", help="Alias em português de `repair`.")
    def portfolio_repair_command(
        slug: str,
        scope: str = typer.Option("federal", "--scope"),
        batch_size: int = typer.Option(25, "--batch-size"),
        resume: bool = typer.Option(True, "--resume/--no-resume"),
        root: str = typer.Option(".", "--root", help="Diretório raiz da área de trabalho."),
    ) -> None:
        typer.echo(
            repair_portfolio(
                _resolve_root(root),
                slug,
                scope=scope,
                batch_size=batch_size,
                resume=resume,
                progress=typer.echo,
            )
        )

    @portfolio_app.command("tick", help="Processa um ciclo da fila do portfólio.")
    @portfolio_app.command("ciclo", help="Alias em português de `tick`.")
    def portfolio_tick_command(
        slug: str,
        max_projects: int = typer.Option(200, "--max-projects"),
        provider: str | None = typer.Option(None, "--provider"),
        max_concurrent: int = typer.Option(4, "--max-concurrent"),
        sync_roster: str = typer.Option("auto", "--sync-roster"),
        roster_max_age_hours: int = typer.Option(24, "--roster-max-age-hours"),
        scope: str = typer.Option("all", "--scope"),
        only_status: str = typer.Option("all", "--only-status"),
        root: str = typer.Option(".", "--root", help="Diretório raiz da área de trabalho."),
    ) -> None:
        typer.echo(
            portfolio_tick(
                _resolve_root(root),
                slug,
                max_projects=max_projects,
                provider_name=provider,
                max_concurrent=max_concurrent,
                sync_roster_mode=sync_roster,
                roster_max_age_hours=roster_max_age_hours,
                scope=scope,
                only_status=only_status,
                progress=typer.echo,
            )
        )

    @portfolio_app.command("run", help="Executa o worker contínuo do portfólio.")
    @portfolio_app.command("executar", help="Alias em português de `run`.")
    def portfolio_run_command(
        slug: str,
        loop: bool = typer.Option(False, "--loop"),
        max_projects: int = typer.Option(200, "--max-projects"),
        sleep_seconds: int = typer.Option(300, "--sleep-seconds"),
        max_concurrent: int = typer.Option(4, "--max-concurrent"),
        provider: str | None = typer.Option(None, "--provider"),
        sync_roster: str = typer.Option("auto", "--sync-roster"),
        roster_max_age_hours: int = typer.Option(24, "--roster-max-age-hours"),
        scope: str = typer.Option("all", "--scope"),
        only_status: str = typer.Option("all", "--only-status"),
        root: str = typer.Option(".", "--root", help="Diretório raiz da área de trabalho."),
    ) -> None:
        typer.echo(
            run_portfolio(
                _resolve_root(root),
                slug,
                loop=loop,
                max_projects=max_projects,
                sleep_seconds=sleep_seconds,
                max_concurrent=max_concurrent,
                provider_name=provider,
                sync_roster_mode=sync_roster,
                roster_max_age_hours=roster_max_age_hours,
                scope=scope,
                only_status=only_status,
                progress=typer.echo,
            )
        )

    @portfolio_app.command("leaderboard", help="Gera o ranking do portfólio.")
    @portfolio_app.command("ranking", help="Alias em português de `leaderboard`.")
    def portfolio_leaderboard_command(
        slug: str,
        root: str = typer.Option(".", "--root", help="Diretório raiz da área de trabalho."),
    ) -> None:
        typer.echo(build_portfolio_leaderboard(_resolve_root(root), slug))

    @portfolio_app.command("diagnose", help="Exibe o diagnóstico do portfólio.")
    @portfolio_app.command("diagnosticar", help="Alias em português de `diagnose`.")
    def portfolio_diagnose_command(
        slug: str,
        limit: int = typer.Option(25, "--limit"),
        root: str = typer.Option(".", "--root", help="Diretório raiz da área de trabalho."),
    ) -> None:
        typer.echo(json.dumps(diagnose_portfolio(_resolve_root(root), slug, limit=limit), indent=2, ensure_ascii=False))

    @portfolio_app.command("status", help="Exibe o status operacional do portfólio.")
    @portfolio_app.command("estado", help="Alias em português de `status`.")
    def portfolio_status_command(
        slug: str,
        root: str = typer.Option(".", "--root", help="Diretório raiz da área de trabalho."),
    ) -> None:
        typer.echo(json.dumps(portfolio_status(_resolve_root(root), slug), indent=2, ensure_ascii=False))

    @source_app.command("sync", help="Sincroniza uma ou mais fontes para um projeto.")
    @source_app.command("sincronizar", help="Alias em português de `sync`.")
    def source_sync(
        project: str,
        plugin: list[str] = typer.Option([], "--plugin"),
        root: str = typer.Option(".", "--root", help="Diretório raiz da área de trabalho."),
    ) -> None:
        report = sync_sources_detailed(_resolve_root(root), project, plugin or None)
        for line in _render_sync_report(report):
            typer.echo(line)

    @agent_app.command("run", help="Executa um papel de agente.")
    @agent_app.command("executar", help="Alias em português de `run`.")
    def agent_run(
        project: str,
        role: str = typer.Option(..., "--role"),
        provider: str | None = typer.Option(None, "--provider"),
        root: str = typer.Option(".", "--root", help="Diretório raiz da área de trabalho."),
    ) -> None:
        typer.echo(run_agent(_resolve_root(root), project, role, provider))

    @dossier_app.command("build", help="Gera o dossiê de um projeto.")
    @dossier_app.command("gerar", help="Alias em português de `build`.")
    def dossier_build(
        project: str,
        root: str = typer.Option(".", "--root", help="Diretório raiz da área de trabalho."),
    ) -> None:
        typer.echo(build_dossier(_resolve_root(root), project))

    @app.command("validate", help="Valida a área de trabalho atual.")
    @app.command("validar", help="Alias em português de `validate`.")
    def validate(
        root: str = typer.Option(".", "--root", help="Diretório raiz da área de trabalho."),
        publish_mode: bool = typer.Option(
            False,
            "--publish-mode",
            "--modo-publicacao",
            help="Executa a auditoria de publicação segura para um repositório source-only.",
        ),
    ) -> None:
        errors = validate_workspace(_resolve_root(root), publish_mode=publish_mode)
        if errors:
            for item in errors:
                typer.echo(item, err=True)
            raise typer.Exit(code=1)
        typer.echo(msg("area_trabalho_valida"))

    app.add_typer(project_app, name="project", help="Operações sobre projetos.")
    app.add_typer(project_app, name="projeto", help="Alias em português de `project`.")
    app.add_typer(source_app, name="source", help="Operações sobre fontes.")
    app.add_typer(source_app, name="fonte", help="Alias em português de `source`.")
    app.add_typer(agent_app, name="agent", help="Operações sobre agentes.")
    app.add_typer(agent_app, name="agente", help="Alias em português de `agent`.")
    app.add_typer(dossier_app, name="dossier", help="Operações sobre dossiês.")
    app.add_typer(dossier_app, name="dossie", help="Alias em português de `dossier`.")
    app.add_typer(portfolio_app, name="portfolio")
    return app


def main(argv: list[str] | None = None) -> int:
    argv = list(argv if argv is not None else sys.argv[1:])
    if typer is not None:
        app = _build_typer_app()
        try:
            app(args=argv, standalone_mode=False)
            return 0
        except SystemExit as exc:  # pragma: no cover - Typer exit behavior
            return int(exc.code)
    parser = _build_parser()
    return _handle_cli(parser.parse_args(argv))


def entrypoint() -> int:
    return main()
