from __future__ import annotations

import io
import json
import os
import sys
import tempfile
import unittest
import zipfile
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import investigador.core as core_module
from investigador.agents import GeminiOpenAICompatibleProvider, MockAgentProvider
from investigador.cli import _build_parser, _handle_cli, _parse_metadata
from investigador.core import (
    add_target,
    advance_project,
    build_dossier,
    create_project,
    diagnose_project,
    init_workspace,
    project_case_metrics_batch,
    project_case_metrics,
    refresh_project_materialized_views_batch,
    refresh_project_materialized_views,
    run_agent,
    sync_sources,
    sync_sources_detailed,
    update_target,
    validate_workspace,
)
from investigador.frontmatter import dump_frontmatter, read_note, write_note
from investigador.http import HttpResponse
from investigador.portfolio import (
    build_portfolio_leaderboard,
    create_portfolio,
    diagnose_portfolio,
    portfolio_tick,
    portfolio_status,
    repair_portfolio,
    run_portfolio,
    sync_portfolio_roster,
)
from investigador.setup_wizard import run_setup


class InvestigadorMVPTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tempdir.name)
        init_workspace(self.root)

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_scaffold_and_project_creation(self) -> None:
        project_path = create_project(self.root, "rio-capital", "Rio Capital")
        self.assertTrue((self.root / "registry" / "people").exists())
        self.assertTrue((self.root / ".investigador" / "cache" / "index.json").exists())
        self.assertTrue(project_path.exists())
        project = read_note(project_path)
        self.assertEqual(project.frontmatter["project_slug"], "rio-capital")

    def test_write_note_emits_body_before_machine_metadata_footer(self) -> None:
        note_path = self.root / "projects" / "rodape" / "note.md"
        write_note(
            note_path,
            {
                "id": "note-rodape",
                "type": "entity",
                "title": "Nota de Rodapé",
                "status": "active",
                "source_class": "derived_workspace",
                "source_refs": [],
                "related_ids": [],
                "project_ids": [],
                "confidence": 1.0,
                "updated_at": "2026-04-01T00:00:00+00:00",
            },
            "# Nota de Rodapé\n\n## Resumo\n\nCorpo humano primeiro.",
        )
        raw = note_path.read_text(encoding="utf-8")
        parsed = read_note(note_path)
        self.assertTrue(raw.startswith("# Nota de Rodapé"))
        self.assertIn("## Metadados da Máquina", raw)
        self.assertIn("```toml", raw)
        self.assertEqual(parsed.frontmatter["id"], "note-rodape")
        self.assertEqual(parsed.body, "# Nota de Rodapé\n\n## Resumo\n\nCorpo humano primeiro.")

    def test_read_note_accepts_legacy_top_frontmatter(self) -> None:
        note_path = self.root / "legacy.md"
        note_path.write_text(
            dump_frontmatter(
                {
                    "id": "legacy-note",
                    "type": "entity",
                    "title": "Nota Legada",
                    "status": "active",
                    "source_class": "derived_workspace",
                    "source_refs": [],
                    "related_ids": [],
                    "project_ids": [],
                    "confidence": 1.0,
                    "updated_at": "2026-04-01T00:00:00+00:00",
                }
            )
            + "# Nota Legada\n\nConteúdo.\n",
            encoding="utf-8",
        )
        parsed = read_note(note_path)
        self.assertEqual(parsed.frontmatter["id"], "legacy-note")
        self.assertEqual(parsed.body, "# Nota Legada\n\nConteúdo.")
        self.assertEqual(parsed.storage_format, "legacy_frontmatter")

    def test_cli_aliases_em_portugues_criam_projeto(self) -> None:
        parser = _build_parser()
        exit_code = _handle_cli(parser.parse_args(["--root", str(self.root), "projeto", "criar", "caso-pt", "--title", "Caso PT"]))
        self.assertEqual(exit_code, 0)
        self.assertTrue((self.root / "projects" / "caso-pt" / "project.md").exists())

    def test_cli_help_principal_esta_em_portugues(self) -> None:
        help_text = _build_parser().format_help()
        self.assertIn("Diretório raiz da área de trabalho", help_text)
        self.assertIn("projeto", help_text)
        self.assertIn("validar", help_text)

    def test_parse_metadata_usa_mensagem_em_portugues(self) -> None:
        with self.assertRaises(ValueError) as ctx:
            _parse_metadata(["sem-separador"])
        self.assertIn("deve estar no formato chave=valor", str(ctx.exception))

    def test_canonical_entity_reuse_across_projects(self) -> None:
        create_project(self.root, "caso-a")
        create_project(self.root, "caso-b")
        first = add_target(self.root, "caso-a", "person", "JOAO-123", "João Silva")
        second = add_target(self.root, "caso-b", "person", "JOAO-123", "João Silva")
        self.assertTrue(first.exists())
        self.assertTrue(second.exists())
        canonical_files = list((self.root / "registry" / "people").glob("*.md"))
        self.assertEqual(len(canonical_files), 1)
        canonical = read_note(canonical_files[0])
        self.assertEqual(sorted(canonical.frontmatter["project_ids"]), ["caso-a", "caso-b"])

    def test_update_target_merges_aliases_and_metadata(self) -> None:
        create_project(self.root, "caso-update")
        target_path = add_target(self.root, "caso-update", "person", "ALVO-UPDATE", "Nome Inicial")
        updated_path = update_target(
            self.root,
            "caso-update",
            "ALVO-UPDATE",
            title="Nome Atualizado",
            aliases=["Nome Completo Atualizado"],
            metadata={"election_year": 2022, "uf": "SP"},
        )
        self.assertEqual(target_path, updated_path)
        target = read_note(target_path)
        canonical = read_note(self.root / target.frontmatter["canonical_path"])
        self.assertEqual(target.frontmatter["title"], "Nome Atualizado")
        self.assertIn("Nome Completo Atualizado", target.frontmatter["aliases"])
        self.assertEqual(target.frontmatter["metadata"]["election_year"], 2022)
        self.assertEqual(canonical.frontmatter["title"], "Nome Atualizado")
        self.assertEqual(canonical.frontmatter["metadata"]["uf"], "SP")

    def test_refresh_rewrites_legacy_note_to_footer_format(self) -> None:
        create_project(self.root, "caso-migracao")
        target_path = add_target(self.root, "caso-migracao", "person", "ALVO-MIG", "Alvo Migração")
        target_note = read_note(target_path)
        target_path.write_text(
            dump_frontmatter(target_note.frontmatter) + target_note.body + "\n",
            encoding="utf-8",
        )
        refresh_project_materialized_views(self.root, "caso-migracao")
        raw = target_path.read_text(encoding="utf-8")
        self.assertTrue(raw.startswith("# Alvo Migração"))
        self.assertIn("## Metadados da Máquina", raw)
        self.assertNotIn("+++\n# Alvo Migração", raw)

    def test_source_sync_writes_provenanced_evidence_and_is_idempotent(self) -> None:
        create_project(self.root, "caso-sync")
        add_target(
            self.root,
            "caso-sync",
            "person",
            "ALVO-1",
            "João da Silva",
            aliases=["Joao da Silva"],
            metadata={
                "cpf": "12345678901",
                "cnpj": "12345678000190",
                "territory_ids": ["3304557"],
                "election_year": 2024,
                "tribunal_aliases": ["api_publica_tse"],
                "modalidades_contratacao": [8],
                "date_from": "2026-01-01",
                "date_to": "2026-03-27",
            },
        )
        with patch("investigador.plugins.fetch_json", side_effect=self._fake_fetch_json), patch(
            "investigador.plugins.fetch_bytes", side_effect=self._fake_fetch_bytes
        ), patch("investigador.plugins.fetch_text", side_effect=self._fake_fetch_text):
            first_report = sync_sources_detailed(
                self.root,
                "caso-sync",
                [
                    "mock",
                    "tse",
                    "cnpj-qsa",
                    "pncp",
                    "datajud",
                    "dou",
                    "portal-transparencia",
                    "transferegov",
                    "tcu",
                    "querido-diario",
                ],
            )
            second_report = sync_sources_detailed(self.root, "caso-sync", ["mock", "tse", "cnpj-qsa", "pncp"])
        evidence_files = sorted((self.root / "projects" / "caso-sync" / "evidence").glob("*.md"))
        self.assertGreaterEqual(len(first_report["written_paths"]), 9)
        self.assertGreaterEqual(len(second_report["written_paths"]), 4)
        self.assertGreaterEqual(len(evidence_files), 9)
        note = read_note(evidence_files[0])
        self.assertIn(note.frontmatter["source_class"], {"official_structured", "official_document", "contextual_web"})
        self.assertTrue(note.frontmatter["source_refs"])
        artifact_dir = self.root / "projects" / "caso-sync" / "runs" / "artifacts"
        self.assertTrue(any(artifact_dir.rglob("*")))
        target_note = read_note(self.root / "projects" / "caso-sync" / "targets" / "person-alvo-1.md")
        self.assertEqual(target_note.frontmatter["metadata"]["party"], "ABC")
        self.assertEqual(target_note.frontmatter["metadata"]["office"], "Prefeito")
        self.assertEqual(target_note.frontmatter["metadata"]["uf"], "RJ")
        self.assertEqual(target_note.frontmatter["metadata"]["election_year"], 2024)
        self.assertIn("## Mandato e fatos básicos", target_note.body)
        self.assertIn("Partido", target_note.body)
        entity_note = read_note(self.root / "registry" / "people" / "jo-o-da-silva.md")
        self.assertIn("## Linha do tempo", entity_note.body)
        self.assertIn("TSE candidatura", entity_note.body)
        tse_evidence = read_note(self.root / "projects" / "caso-sync" / "evidence" / "tse-person-jo-o-da-silva-alvo-1-9999-2024.md")
        self.assertIn("## Enriquecimento aplicado", tse_evidence.body)
        self.assertEqual(tse_evidence.frontmatter["metadata_updates"]["party"], "ABC")
        self.assertEqual(first_report["plugins"][0]["plugin"], "tse")
        self.assertEqual(first_report["plugins"][0]["stage"], "broad_facts")
        self.assertEqual(first_report["plugins"][0]["record_count"], 1)
        self.assertTrue(first_report["plugins"][0]["applied_metadata_updates"])

    def test_web_search_plugin_is_contextual_and_does_not_raise_priority_alone(self) -> None:
        create_project(self.root, "caso-web")
        add_target(
            self.root,
            "caso-web",
            "person",
            "ALVO-WEB",
            "Maria Exemplo",
            metadata={"office": "Deputada Federal", "party": "ABC", "uf": "SP", "election_year": 2022},
        )
        with patch("investigador.plugins.fetch_text", side_effect=self._fake_fetch_text):
            report = sync_sources_detailed(self.root, "caso-web", ["web-search"])
        self.assertEqual(report["plugins"][0]["plugin"], "web-search")
        evidence_files = sorted((self.root / "projects" / "caso-web" / "evidence").glob("*.md"))
        self.assertEqual(len(evidence_files), 2)
        note = read_note(evidence_files[0])
        self.assertEqual(note.frontmatter["source_class"], "contextual_web")
        self.assertEqual(note.frontmatter["source_refs"][0]["retrieved_from"], "duckduckgo_html")
        self.assertIn("query", note.frontmatter["source_refs"][0])
        dossier_path = build_dossier(self.root, "caso-web")
        dossier = read_note(dossier_path)
        self.assertEqual(dossier.frontmatter["priority"], "pista")

    def test_project_advance_runs_fixed_stages_and_is_idempotent(self) -> None:
        create_project(self.root, "caso-advance")
        project_path = self.root / "projects" / "caso-advance" / "project.md"
        project_note = read_note(project_path)
        project_frontmatter = dict(project_note.frontmatter)
        project_frontmatter["plugin_names"] = [
            "tse",
            "cnpj-qsa",
            "pncp",
            "datajud",
            "dou",
            "portal-transparencia",
            "transferegov",
            "tcu",
            "querido-diario",
        ]
        write_note(project_path, project_frontmatter, project_note.body)
        add_target(
            self.root,
            "caso-advance",
            "person",
            "ALVO-ADV",
            "João da Silva",
            aliases=["Joao da Silva"],
            metadata={
                "cpf": "12345678901",
                "cnpj": "12345678000190",
                "election_year": 2024,
                "modalidades_contratacao": [8],
                "tribunal_aliases": ["api_publica_tse"],
                "date_from": "2026-01-01",
                "date_to": "2026-03-27",
            },
        )
        with patch.dict(os.environ, {}, clear=True), patch(
            "investigador.plugins.fetch_json", side_effect=self._fake_fetch_json
        ), patch(
            "investigador.plugins.fetch_bytes", side_effect=self._fake_fetch_bytes
        ), patch(
            "investigador.plugins.fetch_text", side_effect=self._fake_fetch_text
        ):
            first_path = advance_project(self.root, "caso-advance", provider_name="mock")
            second_path = advance_project(self.root, "caso-advance", provider_name="mock")
        first = json.loads(first_path.read_text(encoding="utf-8"))
        second = json.loads(second_path.read_text(encoding="utf-8"))
        self.assertEqual(
            first["stage_order"],
            ["broad_facts", "metadata_gap_check", "specific_official", "contextual_search", "hypothesis_engine", "review", "materialize"],
        )
        skipped = {item["plugin"]: item["reason"] for item in first["plugins_skipped"]}
        self.assertIn("portal-transparencia", skipped)
        self.assertIn("querido-diario", skipped)
        self.assertGreaterEqual(first["new_official_evidence_count"], 2)
        self.assertGreaterEqual(first["new_contextual_evidence_count"], 1)
        self.assertEqual(first["tasks_created_count"], 2)
        self.assertTrue(first["validation"]["ok"])
        self.assertEqual(second["new_evidence_count"], 0)
        self.assertEqual(second["tasks_created_count"], 0)
        self.assertEqual(second["stop_reason"], "no_new_evidence_or_metadata")
        task_files = list((self.root / "projects" / "caso-advance" / "tasks").glob("*.md"))
        self.assertEqual(len(task_files), 2)
        self.assertTrue(any((self.root / "registry" / "hypotheses").glob("*.md")))
        self.assertTrue(any((self.root / "registry" / "laws").glob("*.md")))

    def test_project_advance_baseline_skips_skeptic_and_dossier(self) -> None:
        create_project(self.root, "caso-baseline")
        project_path = self.root / "projects" / "caso-baseline" / "project.md"
        project_note = read_note(project_path)
        project_frontmatter = dict(project_note.frontmatter)
        project_frontmatter["plugin_names"] = ["camara-profile", "camara-expenses", "tse", "dou", "datajud"]
        project_frontmatter["baseline_plugin_names"] = ["camara-profile", "camara-expenses"]
        write_note(project_path, project_frontmatter, project_note.body)
        add_target(
            self.root,
            "caso-baseline",
            "person",
            "ALVO-BL-2041",
            "Maria Souza",
            aliases=["Maria Souza"],
            metadata={
                "camara_id": "2041",
                "legislature_level": "federal",
                "office": "Deputado Federal",
                "party": "ABC",
                "uf": "SP",
            },
        )
        with patch("investigador.plugins.fetch_json", side_effect=self._fake_fetch_json), patch("investigador.plugins.fetch_text", side_effect=self._fake_fetch_text):
            summary_path = advance_project(self.root, "caso-baseline", provider_name="mock", mode="baseline")
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        self.assertEqual(summary["mode"], "baseline")
        self.assertEqual(summary["stage_order"], ["broad_facts", "metadata_gap_check", "specific_official", "contextual_search", "materialize"])
        self.assertEqual(
            [item["plugin"] for item in summary["stages"][2]["plugins_run"]],
            ["camara-profile", "camara-expenses"],
        )
        self.assertEqual(summary["tasks_created_count"], 0)
        self.assertFalse((self.root / "projects" / "caso-baseline" / "dossiers" / "draft.md").exists())
        self.assertTrue((self.root / "projects" / "caso-baseline" / "runs" / "sync-camara-profile.json").exists())
        self.assertTrue((self.root / "projects" / "caso-baseline" / "runs" / "sync-camara-expenses.json").exists())
        project = read_note(project_path)
        self.assertGreater(project.frontmatter["metadata"]["lead_score"], 0)

    def test_portfolio_sync_roster_creates_projects_and_handles_homonyms(self) -> None:
        create_portfolio(self.root, "deputados")
        with patch("investigador.rosters.fetch_json", side_effect=self._fake_roster_fetch_json), patch(
            "investigador.rosters.fetch_text", side_effect=self._fake_roster_fetch_text
        ):
            run_path = sync_portfolio_roster(self.root, "deputados")
        payload = json.loads(run_path.read_text(encoding="utf-8"))
        self.assertGreaterEqual(payload["created_projects"], 3)
        projects = sorted((self.root / "projects").glob("dep-*/project.md"))
        self.assertEqual(len(projects), 3)
        self.assertTrue(any("dep-federal-sp-maria-souza-2041" in str(path) for path in projects))
        self.assertTrue(any("dep-estadual-rj-jo-o-silva-joao-silva-rj" in str(path) for path in projects))
        self.assertTrue(any("dep-estadual-sp-jo-o-silva-joao-silva-sp" in str(path) for path in projects))
        member_notes = sorted((self.root / "portfolios" / "deputados" / "members").glob("*.md"))
        self.assertEqual(len(member_notes), 3)
        federal_project = read_note(self.root / "projects" / "dep-federal-sp-maria-souza-2041" / "project.md")
        self.assertEqual(federal_project.frontmatter["baseline_plugin_names"], ["camara-profile", "camara-expenses"])
        state_project = read_note(self.root / "projects" / "dep-estadual-rj-jo-o-silva-joao-silva-rj" / "project.md")
        self.assertNotIn("camara-profile", state_project.frontmatter["baseline_plugin_names"])
        self.assertNotIn("camara-expenses", state_project.frontmatter["baseline_plugin_names"])
        broken_frontmatter = dict(state_project.frontmatter)
        broken_frontmatter["baseline_plugin_names"] = ["tse", "dou", "datajud", "camara-profile", "camara-expenses"]
        write_note(state_project.path, broken_frontmatter, state_project.body)
        repair_path = repair_portfolio(self.root, "deputados", scope="all", batch_size=10)
        repair_payload = json.loads(repair_path.read_text(encoding="utf-8"))
        repaired_project = read_note(state_project.path)
        self.assertNotIn("camara-profile", repaired_project.frontmatter["baseline_plugin_names"])
        self.assertNotIn("camara-expenses", repaired_project.frontmatter["baseline_plugin_names"])
        self.assertGreaterEqual(repair_payload["summary"]["repaired_projects"], 1)

    def test_portfolio_repair_requeues_federal_baseline_and_normalizes_legacy_state_member(self) -> None:
        create_portfolio(self.root, "deputados")
        with patch("investigador.rosters.fetch_json", side_effect=self._fake_roster_fetch_json), patch(
            "investigador.rosters.fetch_text", side_effect=self._fake_roster_fetch_text
        ):
            sync_portfolio_roster(self.root, "deputados")
        federal_member_path = self.root / "portfolios" / "deputados" / "members" / "dep-federal-sp-maria-souza-2041.md"
        federal_member = read_note(federal_member_path)
        federal_frontmatter = dict(federal_member.frontmatter)
        federal_frontmatter["baseline_completed_at"] = "2026-03-29T00:00:00+00:00"
        federal_frontmatter["queue_state"] = "idle"
        write_note(federal_member_path, federal_frontmatter, federal_member.body)

        state_member_path = self.root / "portfolios" / "deputados" / "members" / "dep-estadual-rj-jo-o-silva-joao-silva-rj.md"
        state_member = read_note(state_member_path)
        state_frontmatter = dict(state_member.frontmatter)
        state_frontmatter["title"] = "Sessão plenária"
        state_frontmatter["roster_validated"] = False
        state_frontmatter["status"] = "active_roster"
        state_frontmatter["queue_state"] = "pending_baseline"
        write_note(state_member_path, state_frontmatter, state_member.body)

        repair_path = repair_portfolio(self.root, "deputados", scope="all", batch_size=10)

        repaired_federal = read_note(federal_member_path)
        repaired_state = read_note(state_member_path)
        repaired_federal_project = read_note(self.root / "projects" / "dep-federal-sp-maria-souza-2041" / "project.md")
        repair_payload = json.loads(repair_path.read_text(encoding="utf-8"))
        self.assertEqual(repaired_federal.frontmatter["queue_state"], "pending_baseline")
        self.assertEqual(repaired_federal.frontmatter["baseline_completed_at"], "")
        self.assertEqual(repaired_federal_project.frontmatter["baseline_plugin_names"], ["camara-profile", "camara-expenses"])
        self.assertEqual(repaired_state.frontmatter["status"], "failed_roster")
        self.assertEqual(repaired_state.frontmatter["queue_state"], "roster_failed")
        self.assertEqual(repair_payload["scope"], "all")
        self.assertTrue(any((self.root / "portfolios" / "deputados" / "runs").glob("repair-progress-*.json")))
        self.assertTrue((self.root / "portfolios" / "deputados" / "runs" / "repair-checkpoint.json").exists())

    def test_portfolio_repair_marks_ambiguous_state_member_as_provisional(self) -> None:
        create_portfolio(self.root, "deputados")
        with patch("investigador.rosters.fetch_json", side_effect=self._fake_roster_fetch_json), patch(
            "investigador.rosters.fetch_text", side_effect=self._fake_roster_fetch_text
        ):
            sync_portfolio_roster(self.root, "deputados")
        state_member_path = self.root / "portfolios" / "deputados" / "members" / "dep-estadual-rj-jo-o-silva-joao-silva-rj.md"
        state_member = read_note(state_member_path)
        state_frontmatter = dict(state_member.frontmatter)
        state_frontmatter["title"] = "Assembleia Legislativa em sessão especial"
        state_frontmatter["status"] = "active_roster"
        state_frontmatter["roster_validated"] = True
        write_note(state_member_path, state_frontmatter, state_member.body)

        repair_portfolio(self.root, "deputados", scope="state", batch_size=10)

        repaired_state = read_note(state_member_path)
        self.assertEqual(repaired_state.frontmatter["status"], "provisional_roster")
        self.assertEqual(repaired_state.frontmatter["queue_state"], "provisional_roster")

    def test_portfolio_sync_roster_reports_progress(self) -> None:
        create_portfolio(self.root, "deputados")
        messages: list[str] = []
        with patch("investigador.rosters.fetch_json", side_effect=self._fake_roster_fetch_json), patch(
            "investigador.rosters.fetch_text", side_effect=self._fake_roster_fetch_text
        ):
            sync_portfolio_roster(self.root, "deputados", progress=messages.append)
        self.assertTrue(any("[roster] coletando camara-roster" in message for message in messages))
        self.assertTrue(any("Maria Souza" in message for message in messages))
        self.assertTrue(any("João Silva" in message for message in messages))

    def test_bad_state_roster_fails_closed(self) -> None:
        create_portfolio(self.root, "deputados", scope=["state"])

        def bad_fetch_text(
            url: str,
            *,
            query: dict[str, object] | None = None,
            headers: dict[str, str] | None = None,
            timeout: int = 30,
        ) -> tuple[str, HttpResponse]:
            del query, headers, timeout
            text = '<html><body><a href="/noticias/123">João Silva</a><a href="/regimento">Maria Souza</a></body></html>'
            return text, self._text_response(url, text)

        with patch("investigador.rosters.fetch_text", side_effect=bad_fetch_text):
            run_path = sync_portfolio_roster(self.root, "deputados")
        payload = json.loads(run_path.read_text(encoding="utf-8"))
        self.assertEqual(payload["created_projects"], 0)
        self.assertTrue(payload["source_failures"])

    def test_portfolio_tick_updates_leaderboard_and_is_idempotent(self) -> None:
        create_portfolio(self.root, "deputados")
        with patch("investigador.rosters.fetch_json", side_effect=self._fake_roster_fetch_json), patch(
            "investigador.rosters.fetch_text", side_effect=self._fake_roster_fetch_text
        ), patch(
            "investigador.plugins.fetch_json", side_effect=self._fake_fetch_json
        ), patch(
            "investigador.plugins.fetch_bytes", side_effect=self._fake_fetch_bytes
        ), patch(
            "investigador.plugins.fetch_text", side_effect=self._fake_fetch_text
        ):
            first_tick = portfolio_tick(self.root, "deputados", max_projects=10, provider_name="mock")
            second_tick = portfolio_tick(self.root, "deputados", max_projects=10, provider_name="mock")
        first_payload = json.loads(first_tick.read_text(encoding="utf-8"))
        second_payload = json.loads(second_tick.read_text(encoding="utf-8"))
        self.assertEqual(
            first_payload["stage_order"],
            ["sync_roster", "seed_missing_projects", "baseline_pending", "deep_pending", "stale_high_priority", "stale_other", "crossref", "leaderboard"],
        )
        self.assertFalse(first_payload["stages"][0]["skipped"])
        self.assertTrue(second_payload["stages"][0]["skipped"])
        self.assertTrue((self.root / "portfolios" / "deputados" / "leaderboard.md").exists())
        leaderboard = read_note(self.root / "portfolios" / "deputados" / "leaderboard.md")
        self.assertIn("## Watchlist de baseline", leaderboard.body)
        self.assertEqual(second_payload["failure_count"], 0)
        checkpoint = json.loads((self.root / "portfolios" / "deputados" / "runs" / "checkpoint.json").read_text(encoding="utf-8"))
        self.assertIn("processed_projects", checkpoint)

    def test_portfolio_tick_skip_roster_processes_queue_without_roster_calls(self) -> None:
        create_portfolio(self.root, "deputados")
        with patch("investigador.rosters.fetch_json", side_effect=self._fake_roster_fetch_json), patch(
            "investigador.rosters.fetch_text", side_effect=self._fake_roster_fetch_text
        ):
            sync_portfolio_roster(self.root, "deputados")
        repair_portfolio(self.root, "deputados", scope="federal", batch_size=10)
        with patch("investigador.plugins.fetch_json", side_effect=self._fake_fetch_json), patch(
            "investigador.plugins.fetch_bytes", side_effect=self._fake_fetch_bytes
        ), patch(
            "investigador.plugins.fetch_text", side_effect=self._fake_fetch_text
        ):
            tick_path = portfolio_tick(
                self.root,
                "deputados",
                max_projects=5,
                provider_name="mock",
                sync_roster_mode="skip",
                scope="federal",
                only_status="active_validated",
            )
        payload = json.loads(tick_path.read_text(encoding="utf-8"))
        self.assertTrue(payload["stages"][0]["skipped"])
        self.assertEqual(payload["stages"][0]["mode"], "skip")
        self.assertTrue(payload["processed_projects"])
        maria_project = self.root / "projects" / "dep-federal-sp-maria-souza-2041" / "runs"
        self.assertTrue((maria_project / "sync-camara-profile.json").exists())
        self.assertTrue((maria_project / "sync-camara-expenses.json").exists())
        metrics = project_case_metrics(self.root, "dep-federal-sp-maria-souza-2041")
        self.assertGreaterEqual(metrics["organization_count"], 1)

    def test_portfolio_tick_auto_skips_recent_successful_roster_sync(self) -> None:
        create_portfolio(self.root, "deputados")
        with patch("investigador.rosters.fetch_json", side_effect=self._fake_roster_fetch_json), patch(
            "investigador.rosters.fetch_text", side_effect=self._fake_roster_fetch_text
        ):
            sync_portfolio_roster(self.root, "deputados")
        with patch("investigador.plugins.fetch_json", side_effect=self._fake_fetch_json), patch(
            "investigador.plugins.fetch_bytes", side_effect=self._fake_fetch_bytes
        ), patch(
            "investigador.plugins.fetch_text", side_effect=self._fake_fetch_text
        ):
            tick_path = portfolio_tick(
                self.root,
                "deputados",
                max_projects=5,
                provider_name="mock",
                sync_roster_mode="auto",
                scope="federal",
                only_status="active_validated",
            )
        payload = json.loads(tick_path.read_text(encoding="utf-8"))
        self.assertTrue(payload["stages"][0]["skipped"])
        self.assertEqual(payload["stages"][0]["mode"], "auto")

    def test_portfolio_status_reports_repair_and_tick_progress(self) -> None:
        create_portfolio(self.root, "deputados")
        with patch("investigador.rosters.fetch_json", side_effect=self._fake_roster_fetch_json), patch(
            "investigador.rosters.fetch_text", side_effect=self._fake_roster_fetch_text
        ):
            sync_portfolio_roster(self.root, "deputados")
        repair_portfolio(self.root, "deputados", scope="federal", batch_size=1)
        status = portfolio_status(self.root, "deputados")
        self.assertEqual(status["repair"]["status"], "completed")
        self.assertEqual(status["repair"]["scope"], "federal")
        self.assertGreaterEqual(status["repair"]["processed_count"], 1)
        self.assertEqual(status["tick"]["current_project_slug"], "")

    def test_portfolio_tick_keyboard_interrupt_writes_failure_and_preserves_current_pointer(self) -> None:
        create_portfolio(self.root, "deputados")
        with patch("investigador.rosters.fetch_json", side_effect=self._fake_roster_fetch_json), patch(
            "investigador.rosters.fetch_text", side_effect=self._fake_roster_fetch_text
        ):
            sync_portfolio_roster(self.root, "deputados")
        repair_portfolio(self.root, "deputados", scope="federal", batch_size=10)

        def interrupting_advance(
            root: Path,
            project_slug: str,
            provider_name: str | None = None,
            mode: str = "deep",
            progress: object | None = None,
        ) -> Path:
            del root, provider_name
            if callable(progress):
                progress(
                    {
                        "event": "plugin_start",
                        "project_slug": project_slug,
                        "project_title": "Deputado Federal Maria Souza (SP)",
                        "mode": mode,
                        "stage": "specific_official",
                        "plugin": "camara-profile",
                        "at": "2026-03-31T00:00:00+00:00",
                    }
                )
            raise KeyboardInterrupt()

        with patch("investigador.portfolio.advance_project", side_effect=interrupting_advance):
            tick_path = portfolio_tick(
                self.root,
                "deputados",
                max_projects=5,
                provider_name="mock",
                sync_roster_mode="skip",
                scope="federal",
                only_status="active_validated",
            )

        payload = json.loads(tick_path.read_text(encoding="utf-8"))
        checkpoint = json.loads((self.root / "portfolios" / "deputados" / "runs" / "checkpoint.json").read_text(encoding="utf-8"))
        status = portfolio_status(self.root, "deputados")
        self.assertEqual(payload["status"], "failed")
        self.assertEqual(payload["error"], "interrupted")
        self.assertEqual(checkpoint["current_project_slug"], "dep-federal-sp-maria-souza-2041")
        self.assertEqual(checkpoint["current_plugin"], "camara-profile")
        self.assertEqual(status["tick"]["current_project_slug"], "dep-federal-sp-maria-souza-2041")
        self.assertEqual(status["tick"]["current_plugin"], "camara-profile")
        self.assertTrue(any((self.root / "portfolios" / "deputados" / "runs").glob("tick-failure-*.json")))

    def test_portfolio_run_rejects_existing_lock(self) -> None:
        create_portfolio(self.root, "deputados")
        lock_path = self.root / ".investigador" / "locks" / "portfolio-deputados.lock"
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        lock_path.write_text("busy", encoding="utf-8")
        with self.assertRaises(RuntimeError):
            run_portfolio(self.root, "deputados")

    def test_entity_resolver_creates_related_entities_from_evidence(self) -> None:
        create_project(self.root, "caso-entity")
        add_target(self.root, "caso-entity", "person", "ALVO-2", "Alvo 2")
        sync_sources(self.root, "caso-entity", ["mock"])
        run_agent(self.root, "caso-entity", "entity_resolver", "mock")
        org_files = list((self.root / "registry" / "organizations").glob("*.md"))
        self.assertEqual(len(org_files), 1)

    def test_baseline_materializes_organizations_without_entity_resolver(self) -> None:
        create_project(self.root, "caso-org-baseline")
        add_target(self.root, "caso-org-baseline", "person", "ALVO-ORG", "Alvo Org")
        sync_sources(self.root, "caso-org-baseline", ["mock"])
        org_files = list((self.root / "registry" / "organizations").glob("*.md"))
        self.assertEqual(len(org_files), 1)

    def test_identity_only_evidence_does_not_raise_priority(self) -> None:
        create_project(self.root, "caso-identidade")
        add_target(
            self.root,
            "caso-identidade",
            "person",
            "ALVO-ID",
            "Maria Souza",
            metadata={"camara_id": "2041", "legislature_level": "federal", "office": "Deputado Federal", "party": "ABC", "uf": "SP"},
        )
        with patch("investigador.plugins.fetch_json", side_effect=self._fake_fetch_json):
            sync_sources_detailed(self.root, "caso-identidade", ["camara-profile"])
        metrics = project_case_metrics(self.root, "caso-identidade")
        self.assertEqual(metrics["official_identity_count"], 1)
        self.assertEqual(metrics["official_signal_count"], 0)
        self.assertEqual(metrics["priority"], "pista")

    def test_signal_evidence_raises_priority_to_anomaly(self) -> None:
        create_project(self.root, "caso-sinal")
        add_target(
            self.root,
            "caso-sinal",
            "person",
            "ALVO-SINAL",
            "Maria Souza",
            metadata={"camara_id": "2041", "legislature_level": "federal", "office": "Deputado Federal", "party": "ABC", "uf": "SP"},
        )
        with patch("investigador.plugins.fetch_json", side_effect=self._fake_fetch_json):
            sync_sources_detailed(self.root, "caso-sinal", ["camara-expenses"])
        metrics = project_case_metrics(self.root, "caso-sinal")
        self.assertGreaterEqual(metrics["official_signal_count"], 1)
        self.assertEqual(metrics["priority"], "anomalia_consistente")
        self.assertGreaterEqual(metrics["organization_count"], 1)

    def test_portfolio_watchlist_does_not_elevate_single_signal_without_strong_context(self) -> None:
        create_portfolio(self.root, "deputados", scope=["federal"])
        with patch("investigador.rosters.fetch_json", side_effect=self._fake_roster_fetch_json):
            sync_portfolio_roster(self.root, "deputados")
        repair_portfolio(self.root, "deputados", scope="federal", batch_size=10)
        def no_context_fetch_text(
            url: str,
            *,
            query: dict[str, object] | None = None,
            headers: dict[str, str] | None = None,
            timeout: int = 30,
        ) -> tuple[str, HttpResponse]:
            if "duckduckgo.com" in url:
                empty_html = "<html><body><div class='results'></div></body></html>"
                return empty_html, self._text_response(url, empty_html)
            return self._fake_fetch_text(url, query=query, headers=headers, timeout=timeout)

        with patch("investigador.plugins.fetch_json", side_effect=self._fake_fetch_json), patch(
            "investigador.plugins.fetch_text", side_effect=no_context_fetch_text
        ):
            portfolio_tick(
                self.root,
                "deputados",
                max_projects=1,
                provider_name="mock",
                sync_roster_mode="skip",
                scope="federal",
                only_status="active_validated",
            )
        member = read_note(self.root / "portfolios" / "deputados" / "members" / "dep-federal-sp-maria-souza-2041.md")
        portfolio = read_note(self.root / "portfolios" / "deputados" / "portfolio.md")
        self.assertGreaterEqual(member.frontmatter["official_signal_count"], 1)
        self.assertFalse(member.frontmatter.get("portfolio_elevated", False))
        self.assertNotIn("corroboração contextual com link oficial", member.frontmatter.get("strong_context_reasons", []))
        self.assertEqual(portfolio.frontmatter["elevated_count"], 0)

    def test_portfolio_elevates_case_with_one_signal_and_strong_context(self) -> None:
        create_portfolio(self.root, "deputados", scope=["federal"])
        with patch("investigador.rosters.fetch_json", side_effect=self._fake_roster_fetch_json):
            sync_portfolio_roster(self.root, "deputados")
        repair_portfolio(self.root, "deputados", scope="federal", batch_size=10)
        with patch("investigador.plugins.fetch_json", side_effect=self._fake_fetch_json), patch(
            "investigador.plugins.fetch_text", side_effect=self._fake_fetch_text
        ):
            portfolio_tick(
                self.root,
                "deputados",
                max_projects=1,
                provider_name="mock",
                sync_roster_mode="skip",
                scope="federal",
                only_status="active_validated",
            )
            portfolio_tick(
                self.root,
                "deputados",
                max_projects=1,
                provider_name="mock",
                sync_roster_mode="skip",
                scope="federal",
                only_status="active_validated",
            )
        member = read_note(self.root / "portfolios" / "deputados" / "members" / "dep-federal-sp-maria-souza-2041.md")
        project = read_note(self.root / "projects" / "dep-federal-sp-maria-souza-2041" / "project.md")
        leaderboard = read_note(self.root / "portfolios" / "deputados" / "leaderboard.md")
        self.assertGreaterEqual(member.frontmatter["hypothesis_count"], 1)
        self.assertTrue(member.frontmatter.get("portfolio_elevated", False))
        self.assertTrue(project.frontmatter["metadata"].get("portfolio_elevated", False))
        self.assertIn("Maria Souza", leaderboard.body)

    def test_dou_nonzip_response_becomes_hint_not_plugin_error(self) -> None:
        create_project(self.root, "caso-dou-html")
        add_target(
            self.root,
            "caso-dou-html",
            "person",
            "ALVO-DOU",
            "João da Silva",
            metadata={"date_from": "2026-03-27", "date_to": "2026-03-27"},
        )

        def nonzip_fetch_bytes(
            url: str,
            *,
            query: dict[str, object] | None = None,
            headers: dict[str, str] | None = None,
            timeout: int = 30,
        ) -> HttpResponse:
            del query, headers, timeout
            return self._bytes_response(url, b"<html><body>login required</body></html>")

        with patch("investigador.plugins.fetch_bytes", side_effect=nonzip_fetch_bytes):
            report = sync_sources_detailed(self.root, "caso-dou-html", ["dou"])
        plugin_report = report["plugins"][0]
        self.assertEqual(plugin_report["plugin"], "dou")
        self.assertEqual(plugin_report["error"], "")
        self.assertIn("ZIP", " ".join(plugin_report["next_queries"]))

    def test_project_diagnose_reports_blockers_and_plugin_status(self) -> None:
        create_project(self.root, "caso-diagnose")
        add_target(
            self.root,
            "caso-diagnose",
            "person",
            "ALVO-DIAG",
            "Maria Exemplo",
            metadata={"office": "Deputada Federal", "party": "ABC", "uf": "SP", "election_year": 2022},
        )
        with patch("investigador.plugins.fetch_text", side_effect=self._fake_fetch_text):
            sync_sources_detailed(self.root, "caso-diagnose", ["web-search"])
        diagnosis = diagnose_project(self.root, "caso-diagnose")
        self.assertFalse(diagnosis["deep_ready"])
        self.assertTrue(diagnosis["deep_blockers"])
        self.assertTrue(any(item["plugin"] == "web-search" and item["status"] == "records" for item in diagnosis["plugins"]))

    def test_project_diagnose_reports_repaired_config_not_yet_rerun(self) -> None:
        create_portfolio(self.root, "deputados", scope=["federal"])
        with patch("investigador.rosters.fetch_json", side_effect=self._fake_roster_fetch_json):
            sync_portfolio_roster(self.root, "deputados")
        diagnosis = diagnose_project(self.root, "dep-federal-sp-maria-souza-2041")
        camara_profile = next(item for item in diagnosis["plugins"] if item["plugin"] == "camara-profile")
        self.assertEqual(camara_profile["status"], "not_run")
        self.assertEqual(camara_profile["reason"], "configuração reparada, mas ainda sem nova execução")
        self.assertTrue(any("reparada" in blocker.lower() for blocker in diagnosis["deep_blockers"]))

    def test_portfolio_diagnose_reports_queue_totals(self) -> None:
        create_portfolio(self.root, "deputados")
        with patch("investigador.rosters.fetch_json", side_effect=self._fake_roster_fetch_json), patch(
            "investigador.rosters.fetch_text", side_effect=self._fake_roster_fetch_text
        ), patch(
            "investigador.plugins.fetch_json", side_effect=self._fake_fetch_json
        ), patch(
            "investigador.plugins.fetch_bytes", side_effect=self._fake_fetch_bytes
        ), patch(
            "investigador.plugins.fetch_text", side_effect=self._fake_fetch_text
        ):
            portfolio_tick(self.root, "deputados", max_projects=10, provider_name="mock", scope="federal", only_status="active_validated")
        diagnosis = diagnose_portfolio(self.root, "deputados", limit=5)
        self.assertIn("SP", diagnosis["queue_totals_by_state"])
        self.assertIn("RJ", diagnosis["queue_totals_by_state"])
        self.assertIn("stuck_cases", diagnosis)
        self.assertLessEqual(len(diagnosis["stuck_cases"]), 5)
        self.assertIn("provisional_roster", diagnosis["queue_totals_by_state"]["RJ"])

    def test_portfolio_diagnose_is_read_only(self) -> None:
        create_portfolio(self.root, "deputados")
        with patch("investigador.rosters.fetch_json", side_effect=self._fake_roster_fetch_json), patch(
            "investigador.rosters.fetch_text", side_effect=self._fake_roster_fetch_text
        ):
            sync_portfolio_roster(self.root, "deputados")
        member_path = self.root / "portfolios" / "deputados" / "members" / "dep-federal-sp-maria-souza-2041.md"
        before = read_note(member_path).frontmatter["updated_at"]
        diagnose_portfolio(self.root, "deputados", limit=5)
        after = read_note(member_path).frontmatter["updated_at"]
        self.assertEqual(before, after)

    def test_dossier_build_uses_safe_language(self) -> None:
        create_project(self.root, "caso-dossie", "Caso Dossiê")
        add_target(self.root, "caso-dossie", "person", "ALVO-3", "Alvo 3")
        sync_sources(self.root, "caso-dossie", ["mock"])
        run_agent(self.root, "caso-dossie", "skeptic", "mock")
        dossier_path = build_dossier(self.root, "caso-dossie")
        dossier = read_note(dossier_path)
        self.assertIn("hipóteses", dossier.body.lower())
        self.assertIn("não culpa", dossier.body.lower())

    def test_tse_homonyms_do_not_pollute_case_evidence(self) -> None:
        create_project(self.root, "caso-barcelos")
        add_target(
            self.root,
            "caso-barcelos",
            "person",
            "BARCELOS-BA",
            "Barcelos",
            aliases=["João Carlos Barcelos Batista"],
            metadata={"office": "Deputado Federal", "party": "PV", "uf": "BA", "election_year": 2022},
        )

        def fake_tse_fetch_json(
            url: str,
            *,
            query: dict[str, object] | None = None,
            headers: dict[str, str] | None = None,
            timeout: int = 30,
            method: str = "GET",
            data: bytes | None = None,
        ) -> tuple[object, HttpResponse]:
            del query, headers, timeout, method, data
            payload = {
                "success": True,
                "result": {
                    "results": [
                        {
                            "name": "candidatos-2022",
                            "title": "Candidatos 2022",
                            "resources": [
                                {
                                    "name": "consulta_cand_2022_BRASIL",
                                    "format": "ZIP",
                                    "url": "https://files.example/tse-barcelos.zip",
                                }
                            ],
                        }
                    ]
                },
            }
            return payload, self._json_response(url, payload)

        def fake_tse_fetch_bytes(
            url: str,
            *,
            query: dict[str, object] | None = None,
            headers: dict[str, str] | None = None,
            timeout: int = 30,
        ) -> HttpResponse:
            del query, headers, timeout
            if "tse-barcelos.zip" in url:
                return self._bytes_response(url, self._build_barcelos_tse_zip())
            raise AssertionError(f"Unexpected bytes URL {url}")

        with patch("investigador.plugins.fetch_json", side_effect=fake_tse_fetch_json), patch(
            "investigador.plugins.fetch_bytes", side_effect=fake_tse_fetch_bytes
        ):
            sync_sources_detailed(self.root, "caso-barcelos", ["tse"])

        evidence_files = sorted((self.root / "projects" / "caso-barcelos" / "evidence").glob("tse-*.md"))
        self.assertEqual(len(evidence_files), 1)
        evidence = read_note(evidence_files[0])
        self.assertIn("JOÃO CARLOS BARCELOS BATISTA", evidence.frontmatter["claim"])
        self.assertEqual(evidence.frontmatter["identity_resolution_status"], "confirmed_identity_match")
        run_payload = json.loads((self.root / "projects" / "caso-barcelos" / "runs" / "sync-tse.json").read_text(encoding="utf-8"))
        artifact = next(item for item in run_payload["artifacts"] if item["name"].startswith("tse-resource-"))
        self.assertTrue(artifact["json"]["possible_matches"] or artifact["json"]["rejected_matches"])

    def test_publish_validation_flags_generated_workspace_and_local_secrets(self) -> None:
        env_path = self.root / ".investigador" / "env.sh"
        env_path.write_text('export INVESTIGADOR_GEMINI_API_KEY="chave-real-super-secreta"\n', encoding="utf-8")
        errors = validate_workspace(self.root, publish_mode=True)
        self.assertTrue(any(item.startswith("generated_workspace_present:") for item in errors))
        self.assertTrue(any(item.startswith("secret_detected:") for item in errors))

    def test_publish_validation_accepts_source_only_with_placeholders(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            (root / "README.md").write_text(
                "\n".join(
                    [
                        "# Exemplo",
                        "## Publicação segura",
                        "Este repositório é source-only.",
                        "Os dados gerados são locais e não acompanham o pacote final.",
                        "Os segredos nunca devem ser commitados nem distribuídos.",
                        "Publique sempre a partir de um clone limpo.",
                        "",
                        'export INVESTIGADOR_GEMINI_API_KEY="sua-chave"',
                    ]
                ),
                encoding="utf-8",
            )
            (root / "src").mkdir()
            (root / "src" / "sample.py").write_text("print('ok')\n", encoding="utf-8")
            errors = validate_workspace(root, publish_mode=True)
            self.assertEqual(errors, [])

    def test_publish_validation_flags_real_case_fixture_names(self) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            root = Path(tempdir)
            (root / "README.md").write_text(
                "\n".join(
                    [
                        "# Exemplo",
                        "source-only",
                        "dados gerados são locais",
                        "segredos nunca devem ser commitados",
                        "clone limpo",
                    ]
                ),
                encoding="utf-8",
            )
            (root / "tests").mkdir()
            (root / "tests" / "fixture_test.py").write_text(
                'ALVO = "João Carlos ' + "Bace" + 'lar Batista"\n',
                encoding="utf-8",
            )
            errors = validate_workspace(root, publish_mode=True)
            self.assertTrue(any(item.startswith("real_case_fixture_detected:") for item in errors))

    def test_current_test_file_no_longer_uses_real_homonym_fixture(self) -> None:
        text = Path(__file__).read_text(encoding="utf-8")
        self.assertNotIn("Bace" "lar", text)
        self.assertNotIn("João Carlos Bace" "lar Batista", text)

    def test_deep_dossier_uses_case_hypotheses_and_investigative_matrix(self) -> None:
        create_project(self.root, "caso-dossie-profundo")
        project_path = self.root / "projects" / "caso-dossie-profundo" / "project.md"
        project_note = read_note(project_path)
        project_frontmatter = dict(project_note.frontmatter)
        project_frontmatter["plugin_names"] = ["camara-profile", "camara-expenses", "camara-organs"]
        project_frontmatter["baseline_plugin_names"] = ["camara-profile", "camara-expenses"]
        write_note(project_path, project_frontmatter, project_note.body)
        add_target(
            self.root,
            "caso-dossie-profundo",
            "person",
            "ALVO-PROFUNDO",
            "Maria Souza",
            metadata={"camara_id": "2041", "legislature_level": "federal", "office": "Deputado Federal", "party": "ABC", "uf": "SP"},
        )
        with patch("investigador.plugins.fetch_json", side_effect=self._fake_fetch_json), patch(
            "investigador.plugins.fetch_text", side_effect=self._fake_fetch_text
        ):
            advance_project(self.root, "caso-dossie-profundo", provider_name="mock", mode="deep")
        hypothesis_files = sorted((self.root / "projects" / "caso-dossie-profundo" / "hypotheses").glob("*.md"))
        self.assertTrue(hypothesis_files)
        dossier = read_note(self.root / "projects" / "caso-dossie-profundo" / "dossiers" / "draft.md")
        project = read_note(self.root / "projects" / "caso-dossie-profundo" / "project.md")
        self.assertIn("## Por que este caso importa", dossier.body)
        self.assertIn("## Por que o caso foi elevado", dossier.body)
        self.assertIn("## Hipóteses do caso e lacunas", dossier.body)
        self.assertIn("| Hipótese | Afirmação observável | Tipo de evidência | Fonte | Força | Papel no caso | Lacuna restante |", dossier.body)
        self.assertIn("## Próximos passos oficiais", dossier.body)
        self.assertIn("## Identidade e histórico confirmado", dossier.body)
        self.assertNotIn("Lista plana de evidências", dossier.body)
        self.assertEqual(project.frontmatter["metadata"].get("render_version"), 2)
        self.assertEqual(dossier.frontmatter["metadata"].get("render_version"), 2)

    def test_cross_project_alert_body_explains_connection(self) -> None:
        create_portfolio(self.root, "deputados")
        with patch("investigador.rosters.fetch_json", side_effect=self._fake_roster_fetch_json), patch(
            "investigador.rosters.fetch_text", side_effect=self._fake_roster_fetch_text
        ):
            sync_portfolio_roster(self.root, "deputados")
        repair_portfolio(self.root, "deputados", scope="federal", batch_size=10)
        with patch("investigador.plugins.fetch_json", side_effect=self._fake_fetch_json), patch(
            "investigador.plugins.fetch_bytes", side_effect=self._fake_fetch_bytes
        ), patch(
            "investigador.plugins.fetch_text", side_effect=self._fake_fetch_text
        ):
            portfolio_tick(
                self.root,
                "deputados",
                max_projects=3,
                provider_name="mock",
                sync_roster_mode="skip",
                scope="all",
                only_status="active_validated",
            )
        alert_files = sorted((self.root / "portfolios" / "deputados" / "alerts").glob("*.md"))
        self.assertTrue(alert_files)
        alert = read_note(alert_files[0])
        self.assertIn("## Por que este alerta importa", alert.body)
        self.assertIn("## Relevância analítica", alert.body)
        self.assertIn("## Projetos conectados", alert.body)
        self.assertIn("## Hipóteses potencialmente reforçadas", alert.body)

    def test_trivial_alerts_do_not_dominate_dossier_or_project(self) -> None:
        create_project(self.root, "caso-alerta-trivial")
        add_target(self.root, "caso-alerta-trivial", "person", "ALVO-TRIVIAL", "Maria Exemplo")
        project_path = self.root / "projects" / "caso-alerta-trivial" / "project.md"
        project = read_note(project_path)
        project_frontmatter = dict(project.frontmatter)
        project_frontmatter["metadata"] = {"portfolio_slug": "deputados"}
        write_note(project_path, project_frontmatter, project.body)
        alert_root = self.root / "portfolios" / "deputados" / "alerts"
        alert_root.mkdir(parents=True, exist_ok=True)
        write_note(
            alert_root / "trivial.md",
            {
                "id": "alert-trivial",
                "type": "portfolio_alert",
                "title": "Alerta trivial",
                "status": "active",
                "source_class": "derived_workspace",
                "source_refs": [],
                "related_ids": [],
                "project_ids": ["caso-alerta-trivial"],
                "confidence": 0.7,
                "updated_at": "2026-04-04T00:00:00+00:00",
                "metadata": {
                    "alert_relevance": "trivial_shared_source",
                    "explainer": "ZIP genérico compartilhado do TSE.",
                },
            },
            "# Alerta trivial\n\nNão deve aparecer no núcleo do caso.",
        )
        write_note(
            alert_root / "forte.md",
            {
                "id": "alert-forte",
                "type": "portfolio_alert",
                "title": "Alerta forte",
                "status": "active",
                "source_class": "derived_workspace",
                "source_refs": [],
                "related_ids": [],
                "project_ids": ["caso-alerta-trivial"],
                "confidence": 0.9,
                "updated_at": "2026-04-04T00:00:00+00:00",
                "metadata": {
                    "alert_relevance": "high_signal",
                    "explainer": "Fornecedor compartilhado com evidência oficial em múltiplos projetos.",
                },
            },
            "# Alerta forte\n\nDeve aparecer no núcleo do caso.",
        )
        build_dossier(self.root, "caso-alerta-trivial")
        refresh_project_materialized_views(self.root, "caso-alerta-trivial")
        dossier = read_note(self.root / "projects" / "caso-alerta-trivial" / "dossiers" / "draft.md")
        project = read_note(project_path)
        self.assertIn("Fornecedor compartilhado", dossier.body)
        self.assertNotIn("ZIP genérico compartilhado do TSE", dossier.body)
        self.assertIn("Fornecedor compartilhado", project.body)
        self.assertNotIn("ZIP genérico compartilhado do TSE", project.body)

    def test_project_metrics_detect_need_for_rebuild_when_render_is_stale(self) -> None:
        create_project(self.root, "caso-rebuild")
        project_path = self.root / "projects" / "caso-rebuild" / "project.md"
        project_note = read_note(project_path)
        project_frontmatter = dict(project_note.frontmatter)
        project_frontmatter["plugin_names"] = ["camara-profile", "camara-expenses", "camara-organs"]
        project_frontmatter["baseline_plugin_names"] = ["camara-profile", "camara-expenses"]
        write_note(project_path, project_frontmatter, project_note.body)
        add_target(
            self.root,
            "caso-rebuild",
            "person",
            "ALVO-REBUILD",
            "Maria Souza",
            metadata={"camara_id": "2041", "legislature_level": "federal", "office": "Deputado Federal", "party": "ABC", "uf": "SP"},
        )
        with patch("investigador.plugins.fetch_json", side_effect=self._fake_fetch_json), patch(
            "investigador.plugins.fetch_text", side_effect=self._fake_fetch_text
        ):
            advance_project(self.root, "caso-rebuild", provider_name="mock", mode="deep")
        fresh_metrics = project_case_metrics(self.root, "caso-rebuild")
        self.assertFalse(fresh_metrics["needs_rebuild"])
        project = read_note(project_path)
        downgraded_frontmatter = dict(project.frontmatter)
        downgraded_frontmatter["metadata"] = dict(downgraded_frontmatter.get("metadata", {}))
        downgraded_frontmatter["metadata"]["render_version"] = 0
        write_note(project_path, downgraded_frontmatter, project.body)
        stale_metrics = project_case_metrics(self.root, "caso-rebuild")
        self.assertTrue(stale_metrics["needs_rebuild"])

    def test_camara_expenses_emits_recurring_supplier_signal(self) -> None:
        create_project(self.root, "caso-recorrencia")
        add_target(
            self.root,
            "caso-recorrencia",
            "person",
            "ALVO-RECORRENCIA",
            "Maria Souza",
            metadata={"camara_id": "2041", "legislature_level": "federal", "office": "Deputado Federal", "party": "ABC", "uf": "SP"},
        )

        def recurring_fetch_json(
            url: str,
            *,
            query: dict[str, object] | None = None,
            headers: dict[str, str] | None = None,
            timeout: int = 30,
            method: str = "GET",
            data: bytes | None = None,
        ) -> tuple[object, HttpResponse]:
            if "dadosabertos.camara.leg.br/api/v2/deputados/2041/despesas" in url:
                payload = {
                    "dados": [
                        {
                            "tipoDespesa": "Divulgação",
                            "valorLiquido": "1200,50",
                            "nomeFornecedor": "Fornecedor Alpha",
                            "cnpjCpfFornecedor": "11111111000191",
                        },
                        {
                            "tipoDespesa": "Divulgação",
                            "valorLiquido": "899,90",
                            "nomeFornecedor": "Fornecedor Alpha",
                            "cnpjCpfFornecedor": "11111111000191",
                        },
                    ]
                }
                return payload, self._json_response(url, payload)
            return self._fake_fetch_json(url, query=query, headers=headers, timeout=timeout, method=method, data=data)

        with patch("investigador.plugins.fetch_json", side_effect=recurring_fetch_json):
            sync_sources_detailed(self.root, "caso-recorrencia", ["camara-expenses"])
        plugin_payload = json.loads(
            (self.root / "projects" / "caso-recorrencia" / "runs" / "sync-camara-expenses.json").read_text(encoding="utf-8")
        )
        claims = [record["claim"] for record in plugin_payload["records"]]
        self.assertTrue(any("repetem lançamentos com o fornecedor" in claim for claim in claims))

    def test_portfolio_tick_writes_summary_even_on_late_stage_failure(self) -> None:
        create_portfolio(self.root, "deputados")
        with patch("investigador.rosters.fetch_json", side_effect=self._fake_roster_fetch_json), patch(
            "investigador.rosters.fetch_text", side_effect=self._fake_roster_fetch_text
        ), patch(
            "investigador.plugins.fetch_json", side_effect=self._fake_fetch_json
        ), patch(
            "investigador.plugins.fetch_bytes", side_effect=self._fake_fetch_bytes
        ), patch(
            "investigador.plugins.fetch_text", side_effect=self._fake_fetch_text
        ), patch(
            "investigador.portfolio.build_portfolio_leaderboard",
            side_effect=RuntimeError("leaderboard exploded"),
        ):
            tick_path = portfolio_tick(
                self.root,
                "deputados",
                max_projects=3,
                provider_name="mock",
                scope="federal",
                only_status="active_validated",
            )
        payload = json.loads(tick_path.read_text(encoding="utf-8"))
        self.assertEqual(payload["status"], "failed")
        self.assertTrue((self.root / "portfolios" / "deputados" / "runs" / "checkpoint.json").exists())
        self.assertTrue(any((self.root / "portfolios" / "deputados" / "runs").glob("tick-failure-*.json")))

    def test_batch_helpers_reduce_repeated_note_index_scans(self) -> None:
        create_portfolio(self.root, "deputados")
        with patch("investigador.rosters.fetch_json", side_effect=self._fake_roster_fetch_json), patch(
            "investigador.rosters.fetch_text", side_effect=self._fake_roster_fetch_text
        ):
            sync_portfolio_roster(self.root, "deputados")
        with patch("investigador.core.build_note_index", wraps=core_module.build_note_index) as mocked_index:
            repair_portfolio(self.root, "deputados", scope="federal", batch_size=2)
        self.assertLess(mocked_index.call_count, 25)

    def test_project_batch_helpers_work_with_shared_index(self) -> None:
        create_project(self.root, "caso-batch-a")
        create_project(self.root, "caso-batch-b")
        add_target(self.root, "caso-batch-a", "person", "ALVO-A", "Alvo A")
        add_target(self.root, "caso-batch-b", "person", "ALVO-B", "Alvo B")
        sync_sources(self.root, "caso-batch-a", ["mock"])
        sync_sources(self.root, "caso-batch-b", ["mock"])
        metrics = project_case_metrics_batch(self.root, ["caso-batch-a", "caso-batch-b"])
        touched = refresh_project_materialized_views_batch(self.root, ["caso-batch-a", "caso-batch-b"])
        self.assertIn("caso-batch-a", metrics)
        self.assertIn("caso-batch-b", metrics)
        self.assertIsInstance(touched, list)

    def test_validation_rejects_missing_provenance(self) -> None:
        create_project(self.root, "caso-validate")
        evidence_path = self.root / "projects" / "caso-validate" / "evidence" / "broken.md"
        write_note(
            evidence_path,
            {
                "id": "evidence-broken",
                "type": "evidence",
                "title": "Broken evidence",
                "status": "collected",
                "source_class": "invalid",
                "source_refs": [],
                "related_ids": [],
                "project_ids": ["caso-validate"],
                "confidence": 0.4,
                "updated_at": "2026-03-27T00:00:00+00:00",
            },
            "# Broken",
        )
        errors = validate_workspace(self.root)
        self.assertTrue(any("source_class de evidência inválida" in item for item in errors))
        self.assertTrue(any("não possui claim" in item for item in errors))

    def test_refresh_prunes_missing_related_ids_and_ignores_repo_readme(self) -> None:
        create_project(self.root, "caso-refresh")
        add_target(self.root, "caso-refresh", "person", "ALVO-REFRESH", "Alvo Refresh")
        root_readme = self.root / "README.md"
        root_readme.write_text("# Repo docs\n", encoding="utf-8")
        entity_path = self.root / "registry" / "people" / "alvo-refresh.md"
        entity = read_note(entity_path)
        entity_frontmatter = dict(entity.frontmatter)
        entity_frontmatter["related_ids"] = ["missing-related-id"]
        write_note(entity_path, entity_frontmatter, entity.body)
        touched = refresh_project_materialized_views(self.root, "caso-refresh")
        refreshed = read_note(entity_path)
        self.assertEqual(refreshed.frontmatter["related_ids"], [])
        self.assertIn(entity_path, touched)
        errors = validate_workspace(self.root)
        self.assertFalse(any(str(root_readme) in item for item in errors))

    def test_agent_providers_share_contract(self) -> None:
        project = read_note(create_project(self.root, "caso-provider"))
        context = {"project_slug": "caso-provider", "target_ids": [], "evidence_ids": [], "plugin_names": []}
        mock_result = MockAgentProvider().run("orchestrator", project, context)
        gemini_result = GeminiOpenAICompatibleProvider().run("orchestrator", project, context)
        self.assertEqual(mock_result.role, "orchestrator")
        self.assertEqual(gemini_result.role, "orchestrator")
        self.assertIn(gemini_result.mode, {"dry_run", "remote"})
        self.assertIsInstance(mock_result.to_dict(), dict)
        self.assertIsInstance(gemini_result.to_dict(), dict)

    def test_gemini_provider_converts_connection_reset_into_dry_run(self) -> None:
        project = read_note(create_project(self.root, "caso-provider-reset"))
        context = {"project_slug": "caso-provider-reset", "target_ids": [], "evidence_ids": [], "plugin_names": []}
        with patch.dict(os.environ, {"INVESTIGADOR_GEMINI_API_KEY": "fake-key"}, clear=False), patch(
            "investigador.agents.request.urlopen",
            side_effect=ConnectionResetError(54, "Connection reset by peer"),
        ):
            result = GeminiOpenAICompatibleProvider().run("orchestrator", project, context)
        self.assertEqual(result.mode, "dry_run")
        self.assertIn("Connection reset by peer", result.content)

    def test_agent_run_writes_run_json(self) -> None:
        create_project(self.root, "caso-agent")
        add_target(self.root, "caso-agent", "person", "ALVO-4", "Alvo 4")
        sync_sources(self.root, "caso-agent", ["mock"])
        run_path = run_agent(self.root, "caso-agent", "orchestrator", "mock")
        payload = json.loads(run_path.read_text(encoding="utf-8"))
        self.assertEqual(payload["role"], "orchestrator")
        self.assertTrue((self.root / "projects" / "caso-agent" / "tasks").glob("*.md"))

    def test_setup_wizard_writes_env_file_and_defaults(self) -> None:
        answers = iter(["gemini-key", "portal-token", "datajud-key", "", "", ""])
        stdout = io.StringIO()
        with patch.dict(os.environ, {}, clear=False), patch("investigador.setup_wizard._configure_certificates", return_value=[]), redirect_stdout(stdout):
            env_path = run_setup(self.root, input_fn=lambda _prompt: next(answers))
        content = env_path.read_text(encoding="utf-8")
        self.assertIn("export INVESTIGADOR_GEMINI_API_KEY=gemini-key", content)
        self.assertIn("export INVESTIGADOR_PORTAL_API_KEY=portal-token", content)
        self.assertIn("export INVESTIGADOR_DATAJUD_API_KEY=datajud-key", content)
        self.assertIn("export INVESTIGADOR_AGENT_PROVIDER=gemini", content)
        self.assertIn("export INVESTIGADOR_GEMINI_BASE_URL=", content)
        self.assertIn("export INVESTIGADOR_CNPJ_BASE_URL=", content)
        self.assertIn("export INVESTIGADOR_WEB_SEARCH_PROVIDER=duckduckgo_html", content)
        self.assertIn("export INVESTIGADOR_WEB_SEARCH_URL=https://html.duckduckgo.com/html/", content)
        output = stdout.getvalue()
        self.assertIn("Configuração do Investigador", output)
        self.assertIn("Próximos passos:", output)
        self.assertIn("Usando o padrão: duckduckgo_html", output)

    def test_configure_certificates_accepts_manual_ca_bundle(self) -> None:
        ca_bundle = self.root / "corp-root.pem"
        ca_bundle.write_text("dummy cert", encoding="utf-8")
        answers = iter(["", "", str(ca_bundle)])
        with patch.dict(os.environ, {}, clear=True), patch(
            "investigador.setup_wizard._certifi_path",
            return_value=None,
        ), patch(
            "investigador.setup_wizard._install_certifi",
            return_value=False,
        ), patch(
            "investigador.setup_wizard._https_probe",
            side_effect=[
                (False, "certificate verify failed"),
                (True, "HTTPS probe to TSE succeeded."),
            ],
        ):
            from investigador.setup_wizard import _configure_certificates

            env_values: dict[str, str] = {}
            notes = _configure_certificates(self.root, env_values, input_fn=lambda _prompt: next(answers))
        self.assertEqual(env_values["SSL_CERT_FILE"], str(ca_bundle.resolve()))
        self.assertFalse(any("HTTPS verification is still failing" in note for note in notes))

    def _fake_fetch_json(
        self,
        url: str,
        *,
        query: dict[str, object] | None = None,
        headers: dict[str, str] | None = None,
        timeout: int = 30,
        method: str = "GET",
        data: bytes | None = None,
    ) -> tuple[object, HttpResponse]:
        del headers, timeout, method, data
        query = query or {}
        if "dadosabertos.tse.jus.br/api/3/action/package_search" in url:
            payload = {
                "success": True,
                "result": {
                    "results": [
                        {
                            "name": "transferencia-do-eleitorado-2024",
                            "title": "Transferência do eleitorado 2024",
                            "resources": [
                                {
                                    "name": "transferencia_eleitorado_2024",
                                    "format": "CSV",
                                    "url": "https://files.example/tse-transferencia.csv",
                                }
                            ],
                        },
                        {
                            "name": "candidatos-2024",
                            "title": "Candidatos 2024",
                            "resources": [
                                {
                                    "name": "consulta_cand_2024_BRASIL",
                                    "format": "ZIP",
                                    "url": "https://files.example/tse-candidatos.zip",
                                }
                            ],
                        }
                    ]
                },
            }
            return payload, self._json_response(url, payload)
        if "dadosabertos.camara.leg.br/api/v2/deputados/2041/despesas" in url:
            payload = {
                "dados": [
                    {
                        "tipoDespesa": "Divulgação",
                        "valorLiquido": "1200,50",
                        "nomeFornecedor": "Fornecedor Alpha",
                        "cnpjCpfFornecedor": "11111111000191",
                    },
                    {
                        "tipoDespesa": "Locomoção",
                        "valorLiquido": "899,90",
                        "nomeFornecedor": "Fornecedor Beta",
                        "cnpjCpfFornecedor": "22222222000191",
                    },
                ]
            }
            return payload, self._json_response(url, payload)
        if "dadosabertos.camara.leg.br/api/v2/deputados/2041/orgaos" in url:
            payload = {
                "dados": [
                    {"siglaOrgao": "CCJC", "titulo": "Titular"},
                    {"siglaOrgao": "CMULHER", "titulo": "Suplente"},
                ]
            }
            return payload, self._json_response(url, payload)
        if "dadosabertos.camara.leg.br/api/v2/deputados/2041" in url:
            payload = {
                "dados": {
                    "id": 2041,
                    "nomeCivil": "Maria Souza",
                    "urlWebsite": "https://www.camara.leg.br/deputados/2041",
                    "ultimoStatus": {
                        "siglaPartido": "ABC",
                        "siglaUf": "SP",
                        "email": "maria@camara.leg.br",
                        "gabinete": {"nome": "512"},
                    },
                }
            }
            return payload, self._json_response(url, payload)
        if "brasilapi.com.br/api/cnpj/v1" in url:
            payload = {
                "cnpj": "12345678000190",
                "razao_social": "Empresa Exemplo LTDA",
                "descricao_situacao_cadastral": "ATIVA",
                "municipio": "Rio de Janeiro",
                "uf": "RJ",
                "natureza_juridica": "Sociedade Empresária Limitada",
                "qsa": [
                    {
                        "nome_socio": "João da Silva",
                        "cnpj_cpf_do_socio": "12345678901",
                        "qualificacao_socio": "Sócio-Administrador",
                        "identificador_de_socio": 1,
                    }
                ],
            }
            return payload, self._json_response(url, payload)
        if "/api/pncp/v1/orgaos/" in url:
            payload = [
                {
                    "idItem": 1,
                    "nomeFuturaContratacao": "Aquisição de software investigativo",
                    "unidadeResponsavel": "Secretaria Municipal de Administração",
                }
            ]
            return payload, self._json_response(url, payload)
        if "/api/consulta/v1/contratacoes/publicacao" in url:
            payload = [
                {
                    "numeroControlePNCP": "123",
                    "objetoCompra": "Licença de plataforma de análise",
                    "valorTotalEstimado": "150000",
                    "cnpjFornecedor": "12345678000190",
                    "dataPublicacaoPncp": "2026-02-01",
                }
            ]
            return payload, self._json_response(url, payload)
        if "api-publica.datajud.cnj.jus.br/api_publica_tse/_search" in url:
            payload = {
                "hits": {
                    "hits": [
                        {
                            "_source": {
                                "numeroProcesso": "0001234-56.2026.6.00.0000",
                                "classe": {"nome": "Prestação de Contas"},
                                "orgaoJulgador": {"nome": "Tribunal Superior Eleitoral"},
                                "dataAjuizamento": "2026-01-15",
                            }
                        }
                    ]
                }
            }
            return payload, self._json_response(url, payload)
        if "api.portaldatransparencia.gov.br/v3/api-docs" in url:
            payload = {
                "paths": {
                    "/api-de-dados/ceis": {
                        "get": {
                            "parameters": [
                                {"name": "pagina"},
                                {"name": "tamanhoPagina"},
                                {"name": "codigoSancionado"},
                            ]
                        }
                    }
                }
            }
            return payload, self._json_response(url, payload)
        if "api.portaldatransparencia.gov.br/api-de-dados/ceis" in url:
            payload = [
                {
                    "nomeSancionado": "Empresa Exemplo LTDA",
                    "descricaoSancao": "Suspensão temporária",
                    "orgaoSancionador": "Órgão Federal Exemplo",
                    "dataInicialSancao": "2026-01-10",
                }
            ]
            return payload, self._json_response(url, payload)
        if "executor_especial" in url:
            if query.get("limit") == 1:
                payload = [{"cnpj_executor": "12345678000190", "nome_executor": "Prefeitura do Rio", "ano_emenda": 2026}]
            else:
                payload = [{"cnpj_executor": "12345678000190", "nome_executor": "Prefeitura do Rio", "ano_emenda": 2026, "valor_pago": "550000"}]
            return payload, self._json_response(url, payload)
        if "relatorio_gestao_novo_especial" in url:
            if query.get("limit") == 1:
                payload = [{"nome_beneficiario": "Prefeitura do Rio", "municipio_ibge": "3304557", "ano": 2026}]
            else:
                payload = [{"nome_beneficiario": "Prefeitura do Rio", "municipio_ibge": "3304557", "ano": 2026, "valor_total": "990000"}]
            return payload, self._json_response(url, payload)
        if "api.queridodiario.ok.org.br/gazettes" in url:
            payload = {
                "results": [
                    {
                        "excerpt": "João da Silva foi citado em aditivo contratual.",
                        "url": "https://qd.example/1",
                        "date": "2026-03-01",
                        "territory_name": "Rio de Janeiro",
                    }
                ]
            }
            return payload, self._json_response(url, payload)
        raise AssertionError(f"Unexpected JSON URL {url}")

    def _fake_fetch_bytes(
        self,
        url: str,
        *,
        query: dict[str, object] | None = None,
        headers: dict[str, str] | None = None,
        timeout: int = 30,
    ) -> HttpResponse:
        del query, headers, timeout
        if "tse-candidatos.zip" in url:
            return self._bytes_response(url, self._build_tse_zip())
        if "inlabs.in.gov.br" in url:
            return self._bytes_response(url, self._build_dou_zip())
        raise AssertionError(f"Unexpected bytes URL {url}")

    def _fake_fetch_text(
        self,
        url: str,
        *,
        query: dict[str, object] | None = None,
        headers: dict[str, str] | None = None,
        timeout: int = 30,
    ) -> tuple[str, HttpResponse]:
        del query, headers, timeout
        if "html.duckduckgo.com/html/" in url:
            text = self._build_search_html()
            return text, self._text_response(url, text)
        if "datajud-wiki.cnj.jus.br" in url:
            text = "Use Authorization: APIKey PUBLIC-KEY-123"
            return text, self._text_response(url, text)
        if "sites.tcu.gov.br/dados-abertos/inidoneos-irregulares" in url:
            text = '<a href="https://dados.example/tcu-inidoneos.csv">CSV</a>'
            return text, self._text_response(url, text)
        if "dados.example/tcu-inidoneos.csv" in url:
            text = "nome,documento,situacao\nEmpresa Exemplo LTDA,12345678000190,Irregular\n"
            return text, self._text_response(url, text)
        if "contas.tcu.gov.br/ords/f" in url:
            text = "Certidão consultada para CNPJ 12345678000190: Nada consta."
            return text, self._text_response(url, text)
        raise AssertionError(f"Unexpected text URL {url}")

    def _build_search_html(self) -> str:
        return """
        <html>
          <body>
            <a class="result__a" href="https://example.com/noticia-zambelli">Matéria contextual</a>
            <div class="result__snippet">2026-03-12 Relato jornalístico com cronologia pública.</div>
            <span class="result__url">example.com</span>
            <a class="result__a" href="https://www.gov.br/cgu/pt-br/assuntos/controle">Página oficial</a>
            <div class="result__snippet">2026-03-10 Página oficial que pode render prova primária.</div>
            <span class="result__url">gov.br</span>
          </body>
        </html>
        """.strip()

    def _fake_roster_fetch_json(
        self,
        url: str,
        *,
        query: dict[str, object] | None = None,
        headers: dict[str, str] | None = None,
        timeout: int = 30,
        method: str = "GET",
        data: bytes | None = None,
    ) -> tuple[object, HttpResponse]:
        del query, headers, timeout, method, data
        if "dadosabertos.camara.leg.br/api/v2/deputados" in url:
            payload = {
                "dados": [
                    {
                        "id": 2041,
                        "nome": "Maria Souza",
                        "nomeEleitoral": "Maria Souza",
                        "siglaPartido": "ABC",
                        "siglaUf": "SP",
                        "uri": "https://www.camara.leg.br/deputados/2041",
                    }
                ]
            }
            return payload, self._json_response(url, payload)
        raise AssertionError(f"Unexpected roster JSON URL {url}")

    def _fake_roster_fetch_text(
        self,
        url: str,
        *,
        query: dict[str, object] | None = None,
        headers: dict[str, str] | None = None,
        timeout: int = 30,
    ) -> tuple[str, HttpResponse]:
        del query, headers, timeout
        if "alerj.rj.gov.br" in url.lower():
            text = """
            <html><body>
              <a href="/Deputado/joao-silva-rj">João Silva</a>
              <div>Partido: XYZ</div>
            </body></html>
            """.strip()
            return text, self._text_response(url, text)
        if "al.sp.gov.br" in url.lower():
            text = """
            <html><body>
              <a href="/deputado/joao-silva-sp">João Silva</a>
              <div>Partido: QWE</div>
            </body></html>
            """.strip()
            return text, self._text_response(url, text)
        text = "<html><body><p>Sem parlamentares reconhecidos nesta fixture.</p></body></html>"
        return text, self._text_response(url, text)

    def _build_tse_zip(self) -> bytes:
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
            archive.writestr(
                "consulta_cand_2024_BRASIL.csv",
                "NM_CANDIDATO;SG_PARTIDO;DS_CARGO;SG_UF;DS_SIT_TOT_TURNO;NR_CPF_CANDIDATO;SQ_CANDIDATO\n"
                "João da Silva;ABC;Prefeito;RJ;DEFERIDO;12345678901;9999\n",
            )
            archive.writestr(
                "consulta_cand_2024_RJ.csv",
                "NM_CANDIDATO;SG_PARTIDO;DS_CARGO;SG_UF;DS_SIT_TOT_TURNO;NR_CPF_CANDIDATO;SQ_CANDIDATO\n"
                "João da Silva;ABC;Prefeito;RJ;DEFERIDO;12345678901;9999\n",
            )
        return buffer.getvalue()

    def _build_barcelos_tse_zip(self) -> bytes:
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
            archive.writestr(
                "consulta_cand_2022_BA.csv",
                "NM_CANDIDATO;NM_URNA_CANDIDATO;SG_PARTIDO;DS_CARGO;SG_UF;DS_SIT_TOT_TURNO;NR_CPF_CANDIDATO;SQ_CANDIDATO\n"
                "JOÃO CARLOS BARCELOS BATISTA;BARCELOS;PV;DEPUTADO FEDERAL;BA;ELEITO POR QP;11111111111;50001605351\n"
                "JOÃO CARLOS PAOLILO BARCELOS FILHO;JOÃO CARLOS BARCELOS;PL;DEPUTADO FEDERAL;BA;ELEITO POR QP;22222222222;50001609371\n"
                "VERÔNICA MOTA BARCELOS;VERONICA BARCELOS;PP;VEREADOR;BA;SUPLENTE;33333333333;50002345395\n",
            )
        return buffer.getvalue()

    def _build_dou_zip(self) -> bytes:
        xml = """
        <article>
          <Identifica>Portaria de Nomeação</Identifica>
          <Texto>João da Silva foi designado para comissão de acompanhamento contratual.</Texto>
        </article>
        """.strip()
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
            archive.writestr("dou.xml", xml)
        return buffer.getvalue()

    def _json_response(self, url: str, payload: object) -> HttpResponse:
        return HttpResponse(url=url, status=200, headers={"content-type": "application/json; charset=utf-8"}, content=json.dumps(payload).encode("utf-8"))

    def _text_response(self, url: str, text: str) -> HttpResponse:
        return HttpResponse(url=url, status=200, headers={"content-type": "text/plain; charset=utf-8"}, content=text.encode("utf-8"))

    def _bytes_response(self, url: str, payload: bytes) -> HttpResponse:
        return HttpResponse(url=url, status=200, headers={"content-type": "application/octet-stream"}, content=payload)


if __name__ == "__main__":
    unittest.main()
