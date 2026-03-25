import os
from datetime import datetime
from dotenv import load_dotenv
import requests
from psycopg2.extras import Json

from app.db.connection import get_connection

load_dotenv()

OMIE_APP_KEY = os.getenv("OMIE_APP_KEY")
OMIE_APP_SECRET = os.getenv("OMIE_APP_SECRET")

OMIE_URL = "https://app.omie.com.br/api/v1/geral/contacorrente/"


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

    if response.status_code >= 400:
        raise Exception(f"Erro HTTP Omie {response.status_code}: {data}")

    if isinstance(data, dict) and data.get("faultstring"):
        raise Exception(f"Erro Omie: {data.get('faultstring')}")

    return data


def fetch_all_contas() -> list:
    pagina = 1
    registros = []

    while True:
        data = omie_post(
            "ListarContasCorrentes",
            [{
                "pagina": pagina,
                "registros_por_pagina": 50
            }]
        )

        lista = data.get("ListarContasCorrentes", []) if isinstance(data, dict) else []
        registros.extend(lista)

        total_paginas = data.get("total_de_paginas", pagina)
        if pagina >= total_paginas:
            break

        pagina += 1

    return registros


def upsert_conta(conn, conta: dict) -> None:
    ncod = conta.get("nCodCC")
    descricao = conta.get("descricao")

    if not ncod or not descricao:
        return

    ativo = conta.get("inativo", "N") != "S"

    with conn.cursor() as cur:
        cur.execute(
            """
            insert into omie_core.contas_correntes (
                ncod_cc,
                descricao,
                ativo,
                payload_json,
                created_at,
                updated_at
            )
            values (%s, %s, %s, %s, now(), now())
            on conflict (ncod_cc)
            do update set
                descricao = excluded.descricao,
                ativo = excluded.ativo,
                payload_json = excluded.payload_json,
                updated_at = now()
            """,
            (
                int(ncod),
                descricao,
                ativo,
                Json(conta),
            ),
        )


def run():
    started_at = datetime.now()
    conn = get_connection()

    try:
        contas = fetch_all_contas()

        for conta in contas:
            upsert_conta(conn, conta)

        conn.commit()

        print(f"Contas correntes sincronizadas: {len(contas)}")
        print(f"Início: {started_at} | Fim: {datetime.now()}")

    except Exception as e:
        conn.rollback()
        print(f"Erro na sincronização: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    run()