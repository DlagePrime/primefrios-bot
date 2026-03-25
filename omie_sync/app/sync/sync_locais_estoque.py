import os
import requests
from dotenv import load_dotenv
from psycopg2.extras import Json

from app.db.connection import get_connection

load_dotenv()

OMIE_APP_KEY = os.getenv("OMIE_APP_KEY")
OMIE_APP_SECRET = os.getenv("OMIE_APP_SECRET")

URL = "https://app.omie.com.br/api/v1/estoque/local/"


def to_bool_omie(value):
    return value in ("S", "s", True)


def listar_locais():
    payload = {
        "call": "ListarLocaisEstoque",
        "app_key": OMIE_APP_KEY,
        "app_secret": OMIE_APP_SECRET,
        "param": [{
            "nPagina": 1,
            "nRegPorPagina": 50
        }]
    }

    response = requests.post(URL, json=payload)
    response.raise_for_status()
    return response.json()


def sync_locais_estoque():
    print("🔄 Sincronizando locais de estoque...")

    conn = get_connection()
    cur = conn.cursor()

    try:
        data = listar_locais()
        locais = data.get("locaisEncontrados", []) or []

        total = 0

        for local in locais:
            nome = local.get("descricao")

            # 🔥 regra: ignorar Pastelito
            if nome and "pastelito" in nome.lower():
                continue

            codigo_local = local.get("codigo_local_estoque")

            cur.execute("""
                insert into omie_core.locais_estoque (
                    codigo_local_estoque_omie,
                    nome_local_estoque,
                    inativo,
                    payload_json
                )
                values (%s, %s, %s, %s)
                on conflict (codigo_local_estoque_omie)
                do update set
                    nome_local_estoque = excluded.nome_local_estoque,
                    inativo = excluded.inativo,
                    payload_json = excluded.payload_json,
                    updated_at = now()
            """, (
                codigo_local,
                nome,
                to_bool_omie(local.get("inativo")),
                Json(local)
            ))

            total += 1

        conn.commit()
        print(f"✅ {total} locais sincronizados.")

    finally:
        conn.close()


if __name__ == "__main__":
    sync_locais_estoque()