import os
import requests
from datetime import datetime
from dotenv import load_dotenv
from psycopg2.extras import Json

from app.db.connection import get_connection

load_dotenv()

OMIE_APP_KEY = os.getenv("OMIE_APP_KEY")
OMIE_APP_SECRET = os.getenv("OMIE_APP_SECRET")

URL = "https://app.omie.com.br/api/v1/estoque/consulta/"


def listar_estoque():
    payload = {
        "call": "ListarPosEstoque",
        "app_key": OMIE_APP_KEY,
        "app_secret": OMIE_APP_SECRET,
        "param": [{
            "nPagina": 1,
            "nRegPorPagina": 500,
            "dDataPosicao": datetime.now().strftime("%d/%m/%Y"),
            "cExibeTodos": "N",
            "lista_local_estoque": "10708535697,10793042942,11024975135"
        }]
    }

    response = requests.post(URL, json=payload)
    response.raise_for_status()
    return response.json()


def buscar_nome_local(cur, codigo_local):
    cur.execute("""
        select nome_local_estoque
        from omie_core.locais_estoque
        where codigo_local_estoque_omie = %s
        limit 1
    """, (codigo_local,))
    
    row = cur.fetchone()
    return row[0] if row else None


def sync_estoque():
    print("🔄 Sincronizando estoque...")

    conn = get_connection()
    cur = conn.cursor()

    try:
        data = listar_estoque()
        produtos = data.get("produtos", []) or []

        total = 0

        for item in produtos:
            codigo_produto = item.get("nCodProd")
            codigo_local = item.get("codigo_local_estoque")

            if not codigo_produto or not codigo_local:
                continue

            descricao = item.get("cDescricao")
            codigo_integracao = item.get("cCodInt")
            quantidade = item.get("nSaldo", 0)

            nome_local = buscar_nome_local(cur, codigo_local)

            cur.execute("""
                insert into omie_core.estoque_produtos (
                    codigo_produto_omie,
                    codigo_produto_integracao,
                    codigo_local_estoque_omie,
                    descricao_produto_snapshot,
                    local_estoque_snapshot,
                    quantidade_disponivel,
                    payload_json
                )
                values (%s, %s, %s, %s, %s, %s, %s)
                on conflict (codigo_produto_omie, codigo_local_estoque_omie)
                do update set
                    descricao_produto_snapshot = excluded.descricao_produto_snapshot,
                    local_estoque_snapshot = excluded.local_estoque_snapshot,
                    quantidade_disponivel = excluded.quantidade_disponivel,
                    payload_json = excluded.payload_json,
                    updated_at = now()
            """, (
                codigo_produto,
                codigo_integracao,
                codigo_local,
                descricao,
                nome_local,
                quantidade,
                Json(item)
            ))

            total += 1

        conn.commit()
        print(f"✅ {total} registros de estoque sincronizados.")

    finally:
        conn.close()


if __name__ == "__main__":
    sync_estoque()