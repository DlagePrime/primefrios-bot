# (arquivo completo já corrigido com PYTHONPATH)

import argparse
import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional


CURRENT_FILE = Path(__file__).resolve()
SYNC_DIR = CURRENT_FILE.parent
APP_DIR = SYNC_DIR.parent
PROJECT_ROOT = APP_DIR.parent
LOGS_DIR = PROJECT_ROOT / "logs"
SYNC_LOGS_DIR = LOGS_DIR / "sync_orchestrator"

SYNC_LOGS_DIR.mkdir(parents=True, exist_ok=True)


MODULES_ORDER = [
    "clientes_recomendacoes",
    "vendedores_clientes",
    "contas_correntes",
    "produtos",
    "tabelas_preco",
    "locais_estoque",
    "estoque_produtos",
    "categorias",
    "condicoes_pagamento",
    "titulos_receber",
    "pedidos",
]

MODULES_MAP = {
    "clientes_recomendacoes": {"file": "sync_clientes_recomendacoes.py", "label": "Clientes Recomendações"},
    "vendedores_clientes": {"file": "sync_vendedores_clientes.py", "label": "Vendedores x Clientes"},
    "contas_correntes": {"file": "sync_contas_correntes.py", "label": "Contas Correntes"},
    "produtos": {"file": "sync_produtos.py", "label": "Produtos"},
    "tabelas_preco": {"file": "sync_tabelas_preco.py", "label": "Tabelas de Preço"},
    "locais_estoque": {"file": "sync_locais_estoque.py", "label": "Locais de Estoque"},
    "estoque_produtos": {"file": "sync_estoque_produtos.py", "label": "Estoque Produtos"},
    "categorias": {"file": "sync_categorias.py", "label": "Categorias"},
    "condicoes_pagamento": {"file": "sync_condicoes_pagamento.py", "label": "Condições de Pagamento"},
    "titulos_receber": {"file": "sync_titulos_receber.py", "label": "Títulos a Receber"},
    "pedidos": {"file": "sync_pedidos.py", "label": "Pedidos"},
}


@dataclass
class ModuleResult:
    module: str
    label: str
    status: str
    duration_seconds: float
    return_code: int
    log_file: str
    stderr_preview: str


def now_stamp():
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def run_module(module_name, batch_stamp, python_exec):
    info = MODULES_MAP[module_name]
    script_path = SYNC_DIR / info["file"]
    log_file = SYNC_LOGS_DIR / f"{batch_stamp}__{module_name}.log"

    start = time.time()

    # 🔥 CORREÇÃO PRINCIPAL AQUI
    env = os.environ.copy()
    env["PYTHONPATH"] = str(PROJECT_ROOT)

    process = subprocess.run(
        [python_exec, str(script_path)],
        cwd=str(PROJECT_ROOT),
        capture_output=True,
        text=True,
        env=env
    )

    duration = time.time() - start

    log_file.write_text(
        f"STDOUT:\n{process.stdout}\n\nSTDERR:\n{process.stderr}",
        encoding="utf-8"
    )

    return ModuleResult(
        module=module_name,
        label=info["label"],
        status="success" if process.returncode == 0 else "error",
        duration_seconds=round(duration, 2),
        return_code=process.returncode,
        log_file=str(log_file),
        stderr_preview=process.stderr[:300]
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--all", action="store_true")
    args = parser.parse_args()

    if not args.all:
        print("Use --all")
        return

    python_exec = sys.executable
    batch = now_stamp()

    results = []

    print("\n=== INICIANDO SYNC ===\n")

    for i, module in enumerate(MODULES_ORDER, start=1):
        print(f"[{i}/{len(MODULES_ORDER)}] {MODULES_MAP[module]['label']}")

        result = run_module(module, batch, python_exec)
        results.append(result)

        print(f"-> {result.status} ({result.duration_seconds}s)")

    summary = {
        "batch": batch,
        "results": [asdict(r) for r in results]
    }

    print("\n=== RESUMO ===")
    print(json.dumps(summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()