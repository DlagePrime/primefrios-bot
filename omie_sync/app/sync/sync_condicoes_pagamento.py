import os
from datetime import datetime
from dotenv import load_dotenv
import requests
from psycopg2.extras import Json

from app.db.connection import get_connection

load_dotenv()

OMIE_APP_KEY = os.getenv("OMIE_APP_KEY")
OMIE_APP_SECRET = os.getenv("OMIE_APP_SECRET")

OMIE_URL = "https://app.omie.com.br/api/v1/produtos/formaspagvendas/"


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


def fetch_all_condicoes() -> list:
    pagina = 1
    registros = []

    while True:
        data = omie_post(
            "ListarFormasPagVendas",
            [{
                "pagina": pagina,
                "registros_por_pagina": 50
            }]
        )

        lista = data.get("cadastros", []) if isinstance(data, dict) else []
        registros.extend(lista)

        total_paginas = data.get("total_de_paginas", pagina)
        if pagina >= total_paginas:
            break

        pagina += 1

    return registros


def upsert_condicao(conn, cond: dict) -> None:
    codigo = cond.get("cCodigo")
    descricao = cond.get("cDescricao")

    if not codigo or not descricao:
        return

    numero_parcelas = cond.get("nQtdeParc")
    prazo_dias = cond.get("nDiasParc")

    with conn.cursor() as cur:
        cur.execute(
            """
            insert into omie_core.condicoes_pagamento (
                codigo_parcela_omie,
                descricao,
                descricao_curta,
                numero_parcelas_padrao,
                prazo_total_dias,
                detalhamento_parcelas_json,
                ativa,
                created_at,
                updated_at
            )
            values (%s, %s, %s, %s, %s, %s, %s, now(), now())
            on conflict (codigo_parcela_omie)
            do update set
                descricao = excluded.descricao,
                descricao_curta = excluded.descricao_curta,
                numero_parcelas_padrao = excluded.numero_parcelas_padrao,
                prazo_total_dias = excluded.prazo_total_dias,
                detalhamento_parcelas_json = excluded.detalhamento_parcelas_json,
                ativa = excluded.ativa,
                updated_at = now()
            """,
            (
                str(codigo),
                descricao,
                descricao,
                int(numero_parcelas) if numero_parcelas is not None else None,
                int(prazo_dias) if prazo_dias is not None else None,
                Json(cond),
                True,
            ),
        )


def run():
    started_at = datetime.now()
    conn = get_connection()

    try:
        condicoes = fetch_all_condicoes()

        for cond in condicoes:
            upsert_condicao(conn, cond)

        conn.commit()

        print(f"Condições sincronizadas: {len(condicoes)}")
        print(f"Início: {started_at} | Fim: {datetime.now()}")

    except Exception as e:
        conn.rollback()
        print(f"Erro na sincronização: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    run()