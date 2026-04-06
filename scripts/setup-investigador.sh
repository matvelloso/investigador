#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

echo "Executando o assistente de configuração do Investigador (SSL, credenciais e padrões de busca contextual na web)..."
python3 -m investigador setup "$@"
echo ""
echo "Próximo passo: source .investigador/env.sh"
