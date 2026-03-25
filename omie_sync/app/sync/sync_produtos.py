import os
import time
from datetime import datetime
from dotenv import load_dotenv
import requests
from psycopg2.extras import Json

from app.db.connection import get_connection

load_dotenv()

OMIE_APP_KEY = os.getenv("OMIE_APP_KEY")
OMIE_APP_SECRET = os.getenv("OMIE_APP_SECRET")

OMIE_URL = "https://app.omie.com.br/api/v1/geral/produtos/"


def omie_post(call: str, param: list) -> dict:
    payload = {
        "call": call,
        "app_key": OMIE_APP_KEY,
        "app_secret": OMIE_APP_SECRET,
        "param": param,
    }

    response = requests.post(OMIE_URL, json=payload, timeout=60)

    try:
        data = response.json()
    except Exception:
        data = {"raw_text": response.text}

    # 🔥 tratamento de bloqueio do Omie
    if isinstance(data, dict) and "REDUNDANT" in str(data):
        print("Aguardando 10 segundos (bloqueio Omie)...")
        time.sleep(10)
        return omie_post(call, param)

    if response.status_code >= 400:
        raise Exception(f"Erro HTTP Omie {response.status_code}: {data}")

    if isinstance(data, dict) and data.get("faultstring"):
        raise Exception(f"Erro Omie: {data.get('faultstring')}")

    return data


def fetch_all_produtos() -> list:
    pagina = 1
    registros = []

    while True:
        data = omie_post(
            "ListarProdutos",
            [{
                "pagina": pagina,
                "registros_por_pagina": 100,
                "filtrar_apenas_omiepdv": "N"
            }]
        )

        lista = data.get("produto_servico_cadastro", []) if isinstance(data, dict) else []
        registros.extend(lista)

        total_paginas = data.get("total_de_paginas", pagina)
        if pagina >= total_paginas:
            break

        pagina += 1
        time.sleep(1)  # evita bloqueio

    return registros


def upsert_produto(conn, prod: dict) -> None:
    codigo = prod.get("codigo_produto")
    descricao = prod.get("descricao")

    if not codigo or not descricao:
        return

    ativo = prod.get("inativo", "N") != "S"

    with conn.cursor() as cur:
        cur.execute(
            """
            insert into omie_core.produtos (
                codigo_produto_omie,
                codigo_produto_integracao,
                descricao,
                unidade,
                ncm,
                cfop_padrao,
                ativo,
                payload_json,
                created_at,
                updated_at
            )
            values (%s, %s, %s, %s, %s, %s, %s, %s, now(), now())
            on conflict (codigo_produto_omie)
            do update set
                descricao = excluded.descricao,
                unidade = excluded.unidade,
                ncm = excluded.ncm,
                cfop_padrao = excluded.cfop_padrao,
                ativo = excluded.ativo,
                payload_json = excluded.payload_json,
                updated_at = now()
            """,
            (
                int(codigo),
                prod.get("codigo_produto_integracao"),
                descricao,
                prod.get("unidade"),
                prod.get("ncm"),
                prod.get("cfop"),
                ativo,
                Json(prod),
            ),
        )


def run():
    started_at = datetime.now()
    conn = get_connection()

    try:
        produtos = fetch_all_produtos()

        for prod in produtos:
            upsert_produto(conn, prod)

        conn.commit()

        print(f"Produtos sincronizados: {len(produtos)}")
        print(f"Início: {started_at} | Fim: {datetime.now()}")

    except Exception as e:
        conn.rollback()
        print(f"Erro na sincronização: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    run()