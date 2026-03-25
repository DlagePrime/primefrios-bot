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

URL_TABELAS = "https://app.omie.com.br/api/v1/produtos/tabelaprecos/"


def omie_post(call: str, param: list) -> dict:
    payload = {
        "call": call,
        "app_key": OMIE_APP_KEY,
        "app_secret": OMIE_APP_SECRET,
        "param": param,
    }

    response = requests.post(URL_TABELAS, json=payload, timeout=60)

    try:
        data = response.json()
    except Exception:
        data = {"raw_text": response.text}

    if isinstance(data, dict) and "REDUNDANT" in str(data):
        print("Aguardando 10s (bloqueio Omie)...")
        time.sleep(10)
        return omie_post(call, param)

    if response.status_code >= 400:
        raise Exception(f"Erro HTTP Omie {response.status_code}: {data}")

    if isinstance(data, dict) and data.get("faultstring"):
        raise Exception(f"Erro Omie: {data.get('faultstring')}")

    return data


def fetch_tabelas() -> list:
    data = omie_post(
        "ListarTabelasPreco",
        [{
            "nPagina": 1,
            "nRegPorPagina": 100
        }]
    )
    return data.get("listaTabelasPreco", []) if isinstance(data, dict) else []


def fetch_itens(ncod_tab_preco: int) -> list:
    data = omie_post(
        "ListarTabelaItens",
        [{
            "nPagina": 1,
            "nRegPorPagina": 500,
            "nCodTabPreco": int(ncod_tab_preco)
        }]
    )
    return data.get("itensTabela", []) if isinstance(data, dict) else []


def upsert_tabela(conn, tabela: dict) -> None:
    ncod = tabela.get("nCodTabPreco")
    nome = tabela.get("cNome")
    codigo_tabela_preco = tabela.get("cCodigo")

    if not ncod or not nome:
        return

    ativa = tabela.get("cAtiva", "S") == "S"

    with conn.cursor() as cur:
        cur.execute(
            """
            insert into omie_core.tabelas_preco (
                ncod_tab_preco,
                nome_tabela,
                ativa,
                payload_json,
                codigo_tabela_preco,
                created_at,
                updated_at
            )
            values (%s, %s, %s, %s, %s, now(), now())
            on conflict (ncod_tab_preco)
            do update set
                nome_tabela = excluded.nome_tabela,
                ativa = excluded.ativa,
                payload_json = excluded.payload_json,
                codigo_tabela_preco = excluded.codigo_tabela_preco,
                updated_at = now()
            """,
            (
                int(ncod),
                nome,
                ativa,
                Json(tabela),
                codigo_tabela_preco,
            ),
        )


def upsert_item(conn, ncod_tab_preco: int, item: dict) -> None:
    cod_prod = item.get("nCodProd")
    valor = item.get("nValorTabela")
    desconto = item.get("nPercDesconto")

    if not cod_prod:
        return

    with conn.cursor() as cur:
        cur.execute(
            """
            insert into omie_core.tabelas_preco_itens (
                ncod_tab_preco,
                codigo_produto_omie,
                valor_unitario,
                percentual_desconto,
                payload_json,
                created_at,
                updated_at
            )
            values (%s, %s, %s, %s, %s, now(), now())
            on conflict (ncod_tab_preco, codigo_produto_omie)
            do update set
                valor_unitario = excluded.valor_unitario,
                percentual_desconto = excluded.percentual_desconto,
                payload_json = excluded.payload_json,
                updated_at = now()
            """,
            (
                int(ncod_tab_preco),
                int(cod_prod),
                valor,
                desconto,
                Json(item),
            ),
        )


def run():
    conn = get_connection()
    inicio = datetime.now()

    try:
        tabelas = fetch_tabelas()

        for tabela in tabelas:
            ncod = tabela.get("nCodTabPreco")
            if not ncod:
                continue

            upsert_tabela(conn, tabela)

            itens = fetch_itens(ncod)
            for item in itens:
                upsert_item(conn, ncod, item)

            time.sleep(1)

        conn.commit()

        print(f"Tabelas sincronizadas: {len(tabelas)}")
        print(f"Início: {inicio} | Fim: {datetime.now()}")

    except Exception as e:
        conn.rollback()
        print("Erro:", e)
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    run()