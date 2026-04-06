from __future__ import annotations


def workspace_readme_body() -> str:
    return """# Investigador Workspace

Este workspace organiza entidades canônicas, projetos investigativos, evidências, tarefas e dossiês.

## Regras operacionais

- Toda afirmação precisa apontar para uma evidência com proveniência.
- Hipóteses não equivalem a culpa.
- O conteúdo público deve passar por revisão humana.
"""


def project_body(title: str) -> str:
    return f"""# {title}

## Escopo

Projeto investigativo em andamento.

## Hipóteses em aberto

- Nenhuma hipótese consolidada ainda.

## Próximas ações

- Definir alvos iniciais.
- Rodar fontes públicas prioritárias.
"""


def target_body(identifier: str, canonical_link: str) -> str:
    return f"""# Alvo do projeto

## Identificação

- Identificador informado: `{identifier}`
- Entidade canônica: [{canonical_link}]({canonical_link})

## Observações

- Aguardar evidências e resolução adicional.
"""


def entity_body(name: str, entity_type: str) -> str:
    return f"""# {name}

## Resumo

Entidade canônica do tipo `{entity_type}`.

## Linha do tempo

- Aguardando eventos corroborados.

## Relações

- Aguardando ligações públicas documentadas.

## Evidências

- Aguardando coleta.

## Lacunas

- Necessário confirmar vínculos com fontes primárias.
"""


def evidence_body(claim: str, excerpt: str, source_name: str) -> str:
    return f"""# Evidência

## Afirmação observável

{claim}

## Trecho utilizado

> {excerpt}

## Fonte

- {source_name}

## Notas do analista

- Classificar o peso dessa evidência dentro do contexto do caso.
"""


def task_body(title: str, instructions: str) -> str:
    return f"""# {title}

## Objetivo

{instructions}

## Saída esperada

- Atualização rastreável do workspace.
- Sem linguagem conclusiva de culpa.
"""
