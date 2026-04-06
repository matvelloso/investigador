# Investigador CLI

O Investigador é apenas um protótipo, criado em poucos dias e em grande parte não testado. 
A ideia geral é ter um CLI em Python, orientado a Markdown, para montar áreas de trabalho investigativas sobre possíveis indícios públicos de corrupção no Brasil.

Ele não deve ser utilizado como instrumento de decisão, julgamento ou para quaisquer conclusões sobre nenhum indivíduo our organização. Ele serve apenas como uma possível arquitetura que poderia ser evoluída para facilitar o trabalho investigativo. 

O MVP atual cobre:

- registros canônicos globais de pessoas, organizações, leis e hipóteses
- projetos locais com alvos, evidências, execuções, tarefas e dossiês
- orquestração por portfólio para populações grandes, como parlamentares em exercício
- proveniência determinística, pontuação de indícios e validação auditável
- interface plugável para fontes públicas
- interface plugável para agentes, com adaptador compatível com OpenAI voltado primeiro para Gemini
- plugins reais para TSE, CNPJ/QSA, PNCP, DataJud, DOU, Portal da Transparência, TransfereGov, TCU, Querido Diário, Câmara dos Deputados e busca contextual na web

## Início rápido

Os comandos em português são a interface preferencial. Os comandos em inglês continuam funcionando por compatibilidade.

```bash
python3 -m investigador inicializar
python3 -m investigador configurar
source .investigador/env.sh

python3 -m investigador projeto criar nome-projeto --title "Prefeitura do Rio"
python3 -m investigador projeto adicionar-alvo nome-projeto person JOAO-001 --title "João da Silva" \
  --alias "Joao da Silva" \
  --meta territory_ids='["3304557"]' \
  --meta election_year=2024 \
  --meta tribunal_aliases='["api_publica_tse"]'

python3 -m investigador projeto avancar nome-projeto
python3 -m investigador dossie gerar nome-projeto
python3 -m investigador validar
```

Você também pode usar o alias de módulo em inglês:

```bash
python3 -m investigator init
```

O script auxiliar continua disponível:

```bash
bash scripts/setup-investigador.sh
```

## Publicação segura

Este repositório público deve ser tratado como **source-only**.

- dados gerados são locais e não acompanham o repositório publicado
- segredos nunca devem ser commitados nem distribuídos
- exemplos e fixtures do projeto são ilustrativos e não imputam culpa a pessoas reais
- publique sempre a partir de um clone limpo, nunca da pasta operacional usada em investigações

Antes de empacotar, subir ou compartilhar o repositório, rode:

```bash
python3 -m investigador validar --modo-publicacao
```

Essa auditoria falha se encontrar segredos, áreas locais geradas como `projects/`, `registry/`, `portfolios/` e `.investigador/`, ou fixtures que ainda remetam a casos reais.

## Modelo da área de trabalho

Estrutura principal:

- `registry/people`, `registry/organizations`, `registry/laws`, `registry/hypotheses`
- `projects/<slug>/project.md`
- `projects/<slug>/targets`, `tasks`, `evidence`, `runs`, `dossiers`
- `portfolios/<slug>/...`
- `.investigador/cache/index.json`

As notas são arquivos Markdown com metadados estruturados em TOML para manter o projeto leve e auditável.

Metadados extras podem ser informados com `--meta chave=valor`. Exemplos comuns:

- `cnpj="12345678000190"`
- `cpf="12345678901"`
- `territory_ids=["3304557"]`
- `municipality_ibge="3304557"`
- `election_year=2024`
- `tribunal_aliases=["api_publica_tse"]`
- `modalidades_contratacao=[8]`
- `date_from="2026-01-01"`
- `date_to="2026-03-27"`

Para enriquecer um alvo depois da primeira sincronização:

```bash
python3 -m investigador projeto atualizar-alvo nome-projeto JOAO-001 \
  --alias "João da Silva Neto" \
  --meta election_year=2022 \
  --meta tribunal_aliases='["api_publica_tse","api_publica_tjrj"]'
```

Para regenerar os corpos Markdown sem bater novamente na rede:

```bash
python3 -m investigador projeto atualizar nome-projeto
```

Para ver por que um caso ainda está raso ou não foi promovido:

```bash
python3 -m investigador projeto diagnosticar nome-projeto
```

## Fluxo supervisionado por projeto

O comando principal é:

```bash
python3 -m investigador projeto avancar <projeto>
```

Esse ciclo faz, nesta ordem:

1. coleta de fatos amplos
2. verificação de lacunas de metadados
3. execução apenas das fontes oficiais prontas para consulta
4. busca contextual na web
5. geração determinística de hipóteses, revisão por agentes, materialização
6. montagem do dossiê e validação

Para rodar apenas o censo raso:

```bash
python3 -m investigador projeto avancar <projeto> --mode baseline
```

No modo `baseline`, o sistema:

- coleta fatos amplos e contexto
- materializa organizações ligadas a partir de `proposed_entities`
- recalcula métricas e reescreve as notas legíveis
- não roda o papel cético
- não gera dossiê
- não cria hipóteses nem leis novas

## Modo portfólio

Para populações grandes, como deputados em exercício:

```bash
python3 -m investigador portfolio criar deputados --population deputados-current --scope federal,state
python3 -m investigador portfolio sincronizar-roster deputados
python3 -m investigador portfolio reparar deputados --scope federal --batch-size 25 --resume
python3 -m investigador portfolio ciclo deputados --scope federal --only-status active_validated --max-projects 25 --provider gemini --sync-roster skip
python3 -m investigador portfolio ranking deputados
```

O que o portfólio faz:

- cria um projeto por parlamentar
- semeia cada projeto com um alvo canônico
- roda uma fila `baseline -> aprofundamento`
- gera alertas entre projetos
- produz ranking, checkpoint e artefatos de execução em `portfolios/<slug>/runs/`

Comandos úteis:

```bash
python3 -m investigador portfolio diagnosticar deputados --limit 20
python3 -m investigador portfolio estado deputados
python3 -m investigador portfolio executar deputados --loop --scope federal --only-status active_validated --max-projects 25 --sleep-seconds 300 --provider gemini --sync-roster auto
```

Comportamentos importantes:

- fontes estaduais de roster falham em modo fechado quando não conseguem identificar deputados reais com confiança
- projetos estaduais não herdam plugins exclusivos da Câmara
- o caminho quente federal agora é `camara-profile` + `camara-expenses` primeiro
- evidência só de identidade não promove caso por si só
- hipóteses e leis só surgem no fluxo aprofundado
- membros `failed_roster`, `inactive_roster` e `provisional_roster` ficam fora da fila ativa

### Recuperação incremental recomendada

Para um portfólio antigo que ficou travado:

```bash
python3 -m investigador portfolio sincronizar-roster deputados
python3 -m investigador portfolio reparar deputados --scope federal --batch-size 25 --resume
python3 -m investigador portfolio ciclo deputados --scope federal --only-status active_validated --max-projects 25 --provider gemini --sync-roster skip
python3 -m investigador portfolio diagnosticar deputados --limit 20
python3 -m investigador portfolio executar deputados --loop --scope federal --only-status active_validated --max-projects 25 --sleep-seconds 300 --provider gemini --sync-roster auto
```

## Provedores de agente

O MVP inclui dois provedores:

- `mock`: determinístico, offline e seguro para testes
- `gemini`: adaptador HTTP compatível com OpenAI voltado para Gemini

Configuração típica do Gemini:

```bash
export INVESTIGADOR_AGENT_PROVIDER=gemini
export INVESTIGADOR_GEMINI_API_KEY="sua-chave"
export INVESTIGADOR_GEMINI_BASE_URL="https://generativelanguage.googleapis.com/v1beta/openai"
export INVESTIGADOR_GEMINI_MODEL="gemini-3.1-flash-lite-preview"
```

Sugerimos usar o Geminin 3.1 Flash Lite por ser um modelo muito capaz a custo muito baixo, mas o código poderia ser facilmente modificado para usar outros modelos, ou mesmo modelos locais. 

Sem credenciais, o provider Gemini cai em `dry-run`, para que o restante do fluxo continue utilizável offline.

## Credenciais e configuração

A forma mais fácil de preparar a máquina é o assistente guiado:

```bash
python3 -m investigador configurar
source .investigador/env.sh
```

Ele:

- verifica Python e SSL
- tenta executar o assistente de certificados do macOS quando você permitir
- pede segredos um por um
- configura os padrões da busca contextual na web
- permite informar um bundle CA manual se o HTTPS continuar falhando
- grava `.investigador/env.sh`

O aplicativo continua lendo credenciais por variáveis de ambiente. O assistente apenas cria um arquivo reutilizável para exportá-las.

### Gemini

Como obter:

1. abra [Google AI Studio API Keys](https://aistudio.google.com/app/apikey) ou a [documentação oficial de chave da API Gemini](https://ai.google.dev/gemini-api/docs/api-key)
2. autentique-se com uma conta Google
3. crie uma chave
4. copie o valor

Como configurar:

```bash
export INVESTIGADOR_AGENT_PROVIDER=gemini
export INVESTIGADOR_GEMINI_API_KEY="sua-chave"
export INVESTIGADOR_GEMINI_BASE_URL="https://generativelanguage.googleapis.com/v1beta/openai"
export INVESTIGADOR_GEMINI_MODEL="gemini-3.1-flash-lite-preview"
```

Documentação útil:

- [Guia oficial de chave de API](https://ai.google.dev/gemini-api/docs/api-key)
- [Compatibilidade OpenAI para Gemini](https://ai.google.dev/gemini-api/docs/openai)

### Portal da Transparência

Como obter o token:

1. abra a página oficial [API de Dados](https://portaldatransparencia.gov.br/api-de-dados/)
2. use o cadastro oficial em [Cadastro da API](https://portaldatransparencia.gov.br/api-de-dados/cadastrar-email)
3. autentique-se com Gov.br
4. receba o token no e-mail cadastrado

Como configurar:

```bash
export INVESTIGADOR_PORTAL_API_KEY="seu-token"
```

Referências:

- [Visão geral da API](https://portaldatransparencia.gov.br/api-de-dados/)
- [Cadastro](https://portaldatransparencia.gov.br/api-de-dados/cadastrar-email)
- [Exemplos de uso](https://portaldatransparencia.gov.br/pagina-interna/603579-api-de-dados-exemplos-de-uso)

### DataJud

Como obter a chave pública:

1. abra [Acesso à API pública do DataJud](https://datajud-wiki.cnj.jus.br/api-publica/acesso/)
2. copie o valor atual publicado no cabeçalho `Authorization: APIKey ...`

Como configurar:

```bash
export INVESTIGADOR_DATAJUD_API_KEY="valor-da-chave-sem-o-prefixo-Authorization"
```

Quase sempre você também vai precisar informar aliases de tribunais:

```bash
python3 -m investigador projeto adicionar-alvo nome-projeto person JOAO-001 \
  --title "João da Silva" \
  --meta tribunal_aliases='["api_publica_tse","api_publica_tjrj"]'
```

Referências:

- [Acesso](https://datajud-wiki.cnj.jus.br/api-publica/acesso/)
- [Endpoints e aliases](https://datajud-wiki.cnj.jus.br/api-publica/endpoints/)
- [Visão geral](https://www.cnj.jus.br/sistemas/datajud/api-publica/)

### DOU / INLABS

O plugin do DOU usa downloads XML/ZIP do INLABS. O CLI atual não faz login automaticamente; ele espera um valor de `Cookie` já autenticado.

Ponto de entrada oficial:

- [INLABS](https://inlabs.in.gov.br/)

Como preparar:

1. entre no INLABS pelo navegador
2. abra as ferramentas de desenvolvedor
3. encontre uma requisição para `inlabs.in.gov.br`
4. copie o cabeçalho `Cookie`
5. exporte em `INVESTIGADOR_DOU_COOKIE`

```bash
export INVESTIGADOR_DOU_COOKIE='cookies.iakim=...; outro_cookie=...'
```

Observação:

- esse passo manual é uma adaptação ao comportamento atual do CLI, não uma instrução oficial do INLABS

### SSL / certificados

Se `source sync` falhar com `CERTIFICATE_VERIFY_FAILED`, os motivos mais comuns são:

- o assistente de certificados do Python no macOS não foi concluído
- há proxy, VPN ou ferramenta de segurança reassinando HTTPS
- seu shell ainda não carregou `SSL_CERT_FILE`

Fluxo sugerido:

1. rode `python3 -m investigador configurar` novamente
2. permita o assistente de certificados do macOS, se aparecer
3. se o teste HTTPS ainda falhar, informe o caminho do bundle CA da sua empresa
4. recarregue:

```bash
source .investigador/env.sh
```

5. confirme:

```bash
echo "$SSL_CERT_FILE"
python3 -c "import os; print(os.environ.get('SSL_CERT_FILE'))"
```

### CNPJ / QSA

O backend padrão atual usa a [BrasilAPI para CNPJ](https://brasilapi.com.br/docs#tag/CNPJ) e não exige chave nesta integração.

```bash
export INVESTIGADOR_CNPJ_BASE_URL="https://brasilapi.com.br/api/cnpj/v1"
```

## Plugins de fonte

### Busca contextual na web

`web-search` é um conector secundário. Ele:

- ajuda a descobrir cronologia e contexto público
- sugere URLs oficiais para aprofundamento
- registra consulta, domínio, veículo, data de publicação e método de obtenção

Ele nunca eleva sozinho a prioridade do caso.

Configuração padrão:

```bash
export INVESTIGADOR_WEB_SEARCH_PROVIDER=duckduckgo_html
export INVESTIGADOR_WEB_SEARCH_URL=https://html.duckduckgo.com/html/
```

Uso direto:

```bash
python3 -m investigador fonte sincronizar <projeto> --plugin web-search
python3 -m investigador projeto atualizar <projeto>
```

Uso dentro do ciclo supervisionado:

```bash
python3 -m investigador projeto avancar <projeto>
```

O que ele grava:

- evidências em `projects/<slug>/evidence/`
- resumo normalizado em `projects/<slug>/runs/sync-web-search.json`
- artefatos crus em `projects/<slug>/runs/artifacts/web-search/`

### Fontes oficiais principais

- `tse`: descoberta e download de datasets públicos do TSE
- `cnpj-qsa`: consulta CNPJ/QSA
- `pncp`: PCA e contratações públicas
- `datajud`: consulta pública do DataJud
- `dou`: publicações oficiais via INLABS
- `portal-transparencia`: API oficial do Portal da Transparência
- `transferegov`: relações de transferências especiais
- `tcu`: páginas e arquivos públicos do TCU
- `querido-diario`: API pública do Querido Diário
- `camara-profile`, `camara-expenses`, `camara-organs`: dados oficiais da Câmara dos Deputados

O motor de sincronização prioriza fatos amplos antes de consultas mais estreitas. Na prática:

- `tse`, `cnpj-qsa` e `dou` entram primeiro quando fazem parte do pedido
- metadados promovidos nesses passos passam a alimentar consultas posteriores
- `camara-profile` conta como evidência de identidade/contexto
- `camara-expenses` conta como candidato a sinal investigativo

### Fontes de roster

O portfólio usa fontes de roster separadas antes dos plugins de evidência:

- `camara-roster` para deputados federais atuais
- `assembleia-<uf>-roster` para deputados estaduais atuais

As fontes estaduais são propositalmente estritas. O parser rejeita:

- notícias
- páginas genéricas do site
- sistemas internos
- leis e regimentos
- comissões
- outras páginas que não pareçam perfis reais de parlamentares

Se um estado não funcionar bem no seu ambiente, sobrescreva a URL oficial:

```bash
export INVESTIGADOR_ROSTER_RJ_URL="https://www.alerj.rj.gov.br/Deputados"
export INVESTIGADOR_ROSTER_SP_URL="https://www.al.sp.gov.br/deputados/"
```

Para a Câmara:

```bash
export INVESTIGADOR_CAMARA_ROSTER_URL="https://dadosabertos.camara.leg.br/api/v2/deputados"
```

## Loop manual do operador

Hoje existem dois estilos principais de uso:

1. loop manual do operador
2. loop supervisionado com `projeto avancar`

O loop manual ainda é útil quando você quer inspecionar cada etapa:

```bash
python3 -m investigador fonte sincronizar <projeto> --plugin tse --plugin cnpj-qsa --plugin dou
python3 -m investigador projeto atualizar-alvo <projeto> <alvo> --meta ...
python3 -m investigador fonte sincronizar <projeto> --plugin pncp --plugin datajud --plugin portal-transparencia --plugin transferegov --plugin tcu --plugin querido-diario
python3 -m investigador projeto atualizar <projeto>
python3 -m investigador agente executar <projeto> --role entity_resolver
python3 -m investigador agente executar <projeto> --role collector_analyst
python3 -m investigador agente executar <projeto> --role skeptic
python3 -m investigador dossie gerar <projeto>
python3 -m investigador validar
```

Regra operacional importante:

- plugins de fonte coletam prova e pistas estruturadas/contextuais
- agentes revisam, contestam, conectam e criticam o que foi coletado
- busca contextual é sempre secundária

Fluxo recomendado:

1. cadastre o alvo com o melhor metadado que você já possui
2. rode fatos amplos primeiro
3. inspecione o resumo de sincronização e a nota do alvo
4. complete o que faltar com `projeto atualizar-alvo`
5. rode fontes mais estreitas
6. use `web-search` depois que a identidade já estiver melhor resolvida
7. regenere as notas com `projeto atualizar` quando quiser revisar o estado legível

## Verificando sua configuração

Checagens básicas:

```bash
env | grep INVESTIGADOR_
test -f .investigador/env.sh && sed -n '1,40p' .investigador/env.sh
python3 -m investigador validar
python3 -m investigador validar --modo-publicacao
```

Testes simples por fonte:

```bash
python3 -m investigador fonte sincronizar nome-projeto --plugin tse
python3 -m investigador fonte sincronizar nome-projeto --plugin datajud
python3 -m investigador fonte sincronizar nome-projeto --plugin portal-transparencia
python3 -m investigador fonte sincronizar nome-projeto --plugin dou
python3 -m investigador fonte sincronizar nome-projeto --plugin web-search
python3 -m investigador projeto avancar nome-projeto
```

O que inspecionar depois:

- `projects/<slug>/evidence/`
- `projects/<slug>/runs/sync-<plugin>.json`
- `projects/<slug>/runs/artifacts/<plugin>/`
- `projects/<slug>/targets/*.md`
- `registry/*/*.md`

Se um plugin não retornar evidências:

- confirme se a chave, token ou cookie está configurado
- veja se o alvo tem metadados suficientes
- leia o resumo impresso pelo CLI
- consulte `next_queries` dentro do `sync-<plugin>.json`

## Testes

```bash
python3 -m unittest discover -s tests -v
```
