import os
from datetime import datetime
from dotenv import load_dotenv
import requests
from psycopg2.extras import Json

from app.db.connection import get_connection

load_dotenv()

OMIE_APP_KEY = os.getenv("OMIE_APP_KEY")
OMIE_APP_SECRET = os.getenv("OMIE_APP_SECRET")

OMIE_CLIENTES_URL = "https://app.omie.com.br/api/v1/geral/clientes/"
OMIE_VENDEDORES_URL = "https://app.omie.com.br/api/v1/geral/vendedores/"


def omie_post(url: str, call: str, param: list) -> dict:
    payload = {
        "call": call,
        "app_key": OMIE_APP_KEY,
        "app_secret": OMIE_APP_SECRET,
        "param": param,
    }

    response = requests.post(url, json=payload, timeout=60)
    data = response.json()

    if isinstance(data, dict) and data.get("faultstring"):
        raise Exception(data["faultstring"])

    return data


def fetch_all_vendedores() -> list:
    pagina = 1
    registros = []

    while True:
        data = omie_post(
            OMIE_VENDEDORES_URL,
            "ListarVendedores",
            [{
                "pagina": pagina,
                "registros_por_pagina": 100,
                "apenas_importado_api": "N"
            }]
        )

        lista = data.get("cadastro", []) if isinstance(data, dict) else []
        registros.extend(lista)

        total_paginas = data.get("total_de_paginas", pagina)
        if pagina >= total_paginas:
            break

        pagina += 1

    return registros


def fetch_all_clientes() -> list:
    pagina = 1
    registros = []

    while True:
        data = omie_post(
            OMIE_CLIENTES_URL,
            "ListarClientes",
            [{
                "pagina": pagina,
                "registros_por_pagina": 100,
                "apenas_importado_api": "N"
            }]
        )

        lista = data.get("clientes_cadastro", []) if isinstance(data, dict) else []
        registros.extend(lista)

        total_paginas = data.get("total_de_paginas", pagina)
        if pagina >= total_paginas:
            break

        pagina += 1

    return registros


def upsert_vendedor(conn, vendedor: dict) -> None:
    codigo_vendedor = vendedor.get("codigo")
    nome_vendedor = vendedor.get("nome")

    if not codigo_vendedor or not nome_vendedor:
        return

    ativo = vendedor.get("inativo", "N") != "S"

    with conn.cursor() as cur:
        cur.execute(
            """
            insert into omie_core.vendedores (
                codigo_vendedor_omie,
                nome_vendedor,
                ativo,
                created_at,
                updated_at
            )
            values (%s, %s, %s, now(), now())
            on conflict (codigo_vendedor_omie)
            do update set
                nome_vendedor = excluded.nome_vendedor,
                ativo = excluded.ativo,
                updated_at = now()
            """,
            (
                int(codigo_vendedor),
                nome_vendedor,
                ativo,
            ),
        )


def format_telefone(cliente: dict) -> str | None:
    ddd = cliente.get("telefone1_ddd")
    numero = cliente.get("telefone1_numero")

    if ddd and numero:
        return f"({ddd}) {numero}"

    return None


def format_whatsapp(cliente: dict) -> str | None:
    ddd = cliente.get("telefone1_ddd")
    numero = cliente.get("telefone1_numero")

    if ddd and numero:
        return f"55{ddd}{numero}"

    return None


def upsert_cliente(conn, cliente: dict) -> None:
    codigo_cliente = cliente.get("codigo_cliente_omie") or cliente.get("codigo_cliente")
    razao_social = cliente.get("razao_social") or cliente.get("nome_fantasia") or "SEM_NOME"

    if not codigo_cliente:
        return

    telefone_principal = format_telefone(cliente)
    whatsapp = format_whatsapp(cliente)

    with conn.cursor() as cur:
        cur.execute(
            """
            insert into omie_core.clientes (
                codigo_cliente_omie,
                cnpj_cpf,
                razao_social,
                nome_fantasia,
                email_principal,
                telefone_principal,
                whatsapp,
                ativo,
                payload_json,
                created_at,
                updated_at
            )
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now())
            on conflict (codigo_cliente_omie)
            do update set
                cnpj_cpf = excluded.cnpj_cpf,
                razao_social = excluded.razao_social,
                nome_fantasia = excluded.nome_fantasia,
                email_principal = excluded.email_principal,
                telefone_principal = excluded.telefone_principal,
                whatsapp = excluded.whatsapp,
                ativo = excluded.ativo,
                payload_json = excluded.payload_json,
                updated_at = now()
            """,
            (
                int(codigo_cliente),
                cliente.get("cnpj_cpf"),
                razao_social,
                cliente.get("nome_fantasia"),
                cliente.get("email"),
                telefone_principal,
                whatsapp,
                cliente.get("inativo", "N") != "S",
                Json(cliente),
            ),
        )


def run() -> None:
    started_at = datetime.now()
    conn = get_connection()

    try:
        vendedores = fetch_all_vendedores()
        for vendedor in vendedores:
            upsert_vendedor(conn, vendedor)
        conn.commit()
        print(f"Vendedores sincronizados: {len(vendedores)}")

        clientes = fetch_all_clientes()
        for cliente in clientes:
            upsert_cliente(conn, cliente)
        conn.commit()
        print(f"Clientes sincronizados: {len(clientes)}")

        finished_at = datetime.now()
        print(f"Sincronização concluída. Início: {started_at} | Fim: {finished_at}")

    except Exception as e:
        conn.rollback()
        print("Erro:", e)
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    run()