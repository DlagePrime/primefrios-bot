import os
import sys
import time
from datetime import datetime
from pathlib import Path

import requests
from dotenv import load_dotenv
from psycopg2.extras import Json

CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.db.connection import get_connection

load_dotenv()

OMIE_APP_KEY = os.getenv("OMIE_APP_KEY")
OMIE_APP_SECRET = os.getenv("OMIE_APP_SECRET")

OMIE_URL = "https://app.omie.com.br/api/v1/financas/contareceber/"


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


def parse_date(value):
    if not value:
        return None
    try:
        if "/" in str(value):
            dia, mes, ano = str(value).split("/")
            return f"{ano}-{mes}-{dia}"
        return str(value)
    except Exception:
        return None


def to_float(value):
    if value in (None, "", "null"):
        return 0.0
    try:
        return float(str(value).replace(".", "").replace(",", "."))
    except Exception:
        try:
            return float(value)
        except Exception:
            return 0.0


def fetch_titulos():
    pagina = 1
    registros = []

    while True:
        data = omie_post(
            "ListarContasReceber",
            [{
                "pagina": pagina,
                "registros_por_pagina": 100,
                "apenas_importado_api": "N"
            }]
        )

        lista = data.get("conta_receber_cadastro", [])
        registros.extend(lista)

        total_paginas = data.get("total_de_paginas", pagina)
        if pagina >= total_paginas:
            break

        pagina += 1
        time.sleep(1)

    return registros


def map_status(titulo):
    if parse_date(titulo.get("data_pagamento")):
        return "PAGO"
    if titulo.get("cancelado") == "S":
        return "CANCELADO"
    return "ABERTO"


def extrair_numero_parcela(titulo):
    """
    Usa o campo numero_parcela do payload Omie:
    001/001 -> 1
    001/002 -> 1
    002/002 -> 2
    """
    numero_parcela = titulo.get("numero_parcela")

    if not numero_parcela:
        return None

    numero_parcela = str(numero_parcela).strip()

    if "/" in numero_parcela:
        primeira_parte = numero_parcela.split("/")[0].strip()
    else:
        primeira_parte = numero_parcela

    try:
        return str(int(primeira_parte))
    except ValueError:
        return None


def to_int(value):
    if value in (None, "", "null"):
        return None
    try:
        return int(str(value).strip())
    except Exception:
        return None


def buscar_dados_cliente(cur, codigo_cliente_omie):
    """
    Busca dados do cliente já sincronizados para enriquecer o título.
    """
    if not codigo_cliente_omie:
        return None, None, None, None

    cur.execute(
        """
        select
            nome_fantasia,
            whatsapp,
            codigo_vendedor_padrao_omie,
            nome_vendedor_padrao_snapshot
        from omie_core.clientes
        where codigo_cliente_omie = %s
        limit 1
        """,
        (int(codigo_cliente_omie),),
    )

    row = cur.fetchone()
    if not row:
        return None, None, None, None

    nome_fantasia, whatsapp, codigo_vendedor_padrao_omie, nome_vendedor_padrao_snapshot = row
    return (
        nome_fantasia,
        whatsapp,
        codigo_vendedor_padrao_omie,
        nome_vendedor_padrao_snapshot,
    )


def buscar_nome_vendedor(cur, codigo_vendedor_omie):
    if not codigo_vendedor_omie:
        return None

    cur.execute(
        """
        select nome_vendedor
        from omie_core.vendedores
        where codigo_vendedor_omie = %s
        limit 1
        """,
        (int(codigo_vendedor_omie),),
    )

    row = cur.fetchone()
    if not row:
        return None

    return row[0]


def extrair_codigo_vendedor_titulo(titulo: dict):
    """
    Tenta identificar o vendedor no próprio payload do título.
    Mantém fallback no cliente se não vier nada aqui.
    """
    candidatos = [
        titulo.get("codigo_vendedor"),
        titulo.get("codVend"),
        titulo.get("codigo_vendedor_omie"),
    ]

    info = titulo.get("info") or {}
    candidatos.extend(
        [
            info.get("codigo_vendedor"),
            info.get("codVend"),
        ]
    )

    for valor in candidatos:
        codigo = to_int(valor)
        if codigo:
            return codigo

    return None


def upsert_titulo(conn, t):
    codigo_lancamento = t.get("codigo_lancamento_omie")

    if not codigo_lancamento:
        return

    valor_documento = to_float(t.get("valor_documento"))
    valor_pago = to_float(t.get("valor_pago"))
    valor_aberto = valor_documento - valor_pago
    codigo_cliente_omie = t.get("codigo_cliente_fornecedor")
    parcela = extrair_numero_parcela(t)

    with conn.cursor() as cur:
        (
            nome_fantasia_cliente,
            whatsapp_cliente,
            codigo_vendedor_cliente,
            nome_vendedor_cliente,
        ) = buscar_dados_cliente(cur, codigo_cliente_omie)

        codigo_vendedor_omie = extrair_codigo_vendedor_titulo(t)

        if not codigo_vendedor_omie:
            codigo_vendedor_omie = to_int(codigo_vendedor_cliente)

        nome_vendedor_snapshot = None
        if codigo_vendedor_omie:
            nome_vendedor_snapshot = buscar_nome_vendedor(cur, codigo_vendedor_omie)

        if not nome_vendedor_snapshot:
            nome_vendedor_snapshot = nome_vendedor_cliente

        cur.execute(
            """
            insert into omie_core.titulos_receber (
                codigo_titulo_omie,
                codigo_lancamento_omie,
                codigo_cliente_omie,
                nome_fantasia_cliente,
                whatsapp_cliente,
                codigo_vendedor_omie,
                nome_vendedor_snapshot,
                parcela,
                valor_original,
                valor_aberto,
                data_emissao,
                data_vencimento,
                data_pagamento,
                status,
                ncod_cc,
                codigo_categoria,
                payload_json,
                created_at,
                updated_at
            )
            values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now(),now())
            on conflict (codigo_lancamento_omie)
            do update set
                codigo_titulo_omie = excluded.codigo_titulo_omie,
                codigo_cliente_omie = excluded.codigo_cliente_omie,
                nome_fantasia_cliente = excluded.nome_fantasia_cliente,
                whatsapp_cliente = excluded.whatsapp_cliente,
                codigo_vendedor_omie = excluded.codigo_vendedor_omie,
                nome_vendedor_snapshot = excluded.nome_vendedor_snapshot,
                parcela = excluded.parcela,
                valor_original = excluded.valor_original,
                valor_aberto = excluded.valor_aberto,
                data_emissao = excluded.data_emissao,
                data_vencimento = excluded.data_vencimento,
                data_pagamento = excluded.data_pagamento,
                status = excluded.status,
                ncod_cc = excluded.ncod_cc,
                codigo_categoria = excluded.codigo_categoria,
                payload_json = excluded.payload_json,
                updated_at = now()
            """,
            (
                t.get("codigo_lancamento_omie"),
                t.get("codigo_lancamento_omie"),
                codigo_cliente_omie,
                nome_fantasia_cliente,
                whatsapp_cliente,
                codigo_vendedor_omie,
                nome_vendedor_snapshot,
                parcela,
                valor_documento,
                valor_aberto,
                parse_date(t.get("data_emissao")),
                parse_date(t.get("data_vencimento")),
                parse_date(t.get("data_pagamento")),
                map_status(t),
                t.get("codigo_conta_corrente"),
                t.get("codigo_categoria"),
                Json(t),
            ),
        )


def run():
    conn = get_connection()
    inicio = datetime.now()

    try:
        titulos = fetch_titulos()

        for t in titulos:
            upsert_titulo(conn, t)

        conn.commit()

        print(f"Títulos sincronizados: {len(titulos)}")
        print(f"Início: {inicio} | Fim: {datetime.now()}")

    except Exception as e:
        conn.rollback()
        print("Erro:", e)
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    run()