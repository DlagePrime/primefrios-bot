import os
import json
import requests
from decimal import Decimal
from dotenv import load_dotenv
from psycopg2.extras import Json

from app.db.connection import get_connection

load_dotenv()

OMIE_APP_KEY = os.getenv("OMIE_APP_KEY")
OMIE_APP_SECRET = os.getenv("OMIE_APP_SECRET")

URL_PEDIDOS = "https://app.omie.com.br/api/v1/produtos/pedido/"


def decimal_to_float(value):
    if isinstance(value, Decimal):
        return float(value)
    return value


def parse_date_to_brazilian(value):
    if not value:
        return ""
    parts = str(value).split("-")
    if len(parts) == 3:
        return f"{parts[2]}/{parts[1]}/{parts[0]}"
    return str(value)


def buscar_pedidos_prontos(cur):
    cur.execute("""
        select
            id,
            codigo_pedido_omie,
            numero_pedido,
            codigo_cliente_omie,
            nome_fantasia_cliente_snapshot,
            codigo_vendedor_omie,
            nome_vendedor_snapshot,
            codigo_tabela_preco,
            ncod_tabela_preco,
            nome_tabela_preco_snapshot,
            codigo_condicao_pagamento,
            descricao_condicao_pagamento_snapshot,
            numero_parcelas,
            data_emissao,
            data_previsao_entrega,
            valor_produtos,
            valor_desconto,
            valor_total,
            status_pedido,
            etapa,
            pedido_bloqueado,
            codigo_pedido_integracao,
            codigo_cenario_impostos,
            cenario_fiscal_snapshot,
            codigo_empresa_omie,
            codigo_conta_corrente_omie,
            codigo_categoria_pedido,
            origem_pedido,
            quantidade_itens,
            cidade_cliente_snapshot,
            uf_cliente_snapshot,
            status_integracao,
            data_envio_omie,
            erro_integracao
        from omie_core.pedidos
        where status_integracao = 'Pronto para envio'
        order by created_at asc
    """)
    return cur.fetchall()


def buscar_itens_pedido(cur, pedido_id):
    cur.execute("""
        select
            sequencia,
            codigo_produto_omie,
            descricao_produto_snapshot,
            unidade,
            quantidade,
            valor_unitario,
            percentual_desconto,
            valor_total_item,
            codigo_item_omie,
            cfop,
            codigo_local_estoque_omie,
            local_estoque_snapshot,
            codigo_tabela_preco_item,
            tabela_preco_snapshot,
            valor_desconto_item,
            valor_mercadoria,
            codigo_categoria_item,
            codigo_cenario_impostos_item
        from omie_core.pedido_itens
        where pedido_id = %s
        order by sequencia
    """, (pedido_id,))
    return cur.fetchall()


def montar_codigo_item_integracao(id_local, sequencia):
    codigo = f"{str(id_local)[:20]}-{int(sequencia)}"
    return codigo[:30]


def montar_payload(pedido, itens):
    (
        id_local,
        codigo_pedido_omie,
        numero_pedido,
        codigo_cliente_omie,
        nome_fantasia_cliente_snapshot,
        codigo_vendedor_omie,
        nome_vendedor_snapshot,
        codigo_tabela_preco,
        ncod_tabela_preco,
        nome_tabela_preco_snapshot,
        codigo_condicao_pagamento,
        descricao_condicao_pagamento_snapshot,
        numero_parcelas,
        data_emissao,
        data_previsao_entrega,
        valor_produtos,
        valor_desconto,
        valor_total,
        status_pedido,
        etapa,
        pedido_bloqueado,
        codigo_pedido_integracao,
        codigo_cenario_impostos,
        cenario_fiscal_snapshot,
        codigo_empresa_omie,
        codigo_conta_corrente_omie,
        codigo_categoria_pedido,
        origem_pedido,
        quantidade_itens,
        cidade_cliente_snapshot,
        uf_cliente_snapshot,
        status_integracao,
        data_envio_omie,
        erro_integracao
    ) = pedido

    if not codigo_pedido_integracao:
        codigo_pedido_integracao = str(id_local)

    det = []

    for item in itens:
        (
            sequencia,
            codigo_produto_omie_item,
            descricao_produto_snapshot,
            unidade,
            quantidade,
            valor_unitario,
            percentual_desconto,
            valor_total_item,
            codigo_item_omie,
            cfop,
            codigo_local_estoque_omie,
            local_estoque_snapshot,
            codigo_tabela_preco_item,
            tabela_preco_snapshot,
            valor_desconto_item,
            valor_mercadoria,
            codigo_categoria_item,
            codigo_cenario_impostos_item
        ) = item

        codigo_item_integracao = montar_codigo_item_integracao(id_local, sequencia)

        det.append({
            "ide": {
                "codigo_item_integracao": codigo_item_integracao
            },
            "produto": {
                "codigo_produto": int(codigo_produto_omie_item),
                "quantidade": decimal_to_float(quantidade),
                "valor_unitario": decimal_to_float(valor_unitario),
                "codigo_tabela_preco": int(codigo_tabela_preco_item) if codigo_tabela_preco_item else 0,
                "cfop": cfop or "",
                "unidade": unidade or ""
            },
            "inf_adic": {
                "codigo_local_estoque": int(codigo_local_estoque_omie) if codigo_local_estoque_omie else 0,
                "codigo_categoria_item": codigo_categoria_item or codigo_categoria_pedido or "",
                "codigo_cenario_impostos_item": int(codigo_cenario_impostos_item) if codigo_cenario_impostos_item else int(codigo_cenario_impostos or 0)
            }
        })

    payload = {
        "call": "IncluirPedido",
        "app_key": OMIE_APP_KEY,
        "app_secret": OMIE_APP_SECRET,
        "param": [{
            "cabecalho": {
                "codigo_cliente": int(codigo_cliente_omie),
                "codigo_parcela": codigo_condicao_pagamento or "",
                "codigo_cenario_impostos": int(codigo_cenario_impostos) if codigo_cenario_impostos else 0,
                "codigo_empresa": int(codigo_empresa_omie) if codigo_empresa_omie else 0,
                "codigo_pedido_integracao": codigo_pedido_integracao,
                "data_previsao": parse_date_to_brazilian(data_previsao_entrega),
                "etapa": etapa or "",
                "numero_pedido": numero_pedido or ""
            },
            "informacoes_adicionais": {
                "codVend": int(codigo_vendedor_omie) if codigo_vendedor_omie else 0,
                "codigo_categoria": codigo_categoria_pedido or "",
                "codigo_conta_corrente": int(codigo_conta_corrente_omie) if codigo_conta_corrente_omie else 0
            },
            "lista_parcelas": {
                "parcela": []
            },
            "det": det
        }]
    }

    return payload


def enviar_para_omie(payload):
    response = requests.post(URL_PEDIDOS, json=payload, timeout=60)

    try:
        data = response.json()
    except Exception:
        data = {"raw_text": response.text}

    if response.status_code >= 400:
        raise Exception(json.dumps(data, ensure_ascii=False))

    return data


def extrair_retorno_omie(data):
    if not isinstance(data, dict):
        return None, None, None

    bloco = data
    if isinstance(data.get("pedido_venda_produto"), dict):
        bloco = data["pedido_venda_produto"]
    elif isinstance(data.get("pedido_venda_produto"), list) and data["pedido_venda_produto"]:
        bloco = data["pedido_venda_produto"][0]

    codigo_pedido_omie = (
        bloco.get("codigo_pedido")
        or bloco.get("codigo_pedido_omie")
        or bloco.get("nCodPedido")
    )

    numero_pedido = (
        bloco.get("numero_pedido")
        or bloco.get("cNumeroPedido")
    )

    etapa = bloco.get("etapa")

    return codigo_pedido_omie, numero_pedido, etapa


def atualizar_sucesso(cur, id_local, retorno_json, codigo_pedido_omie_retorno, numero_pedido_retorno, etapa_retorno):
    cur.execute("""
        update omie_core.pedidos
        set
            codigo_pedido_omie = coalesce(%s, codigo_pedido_omie),
            numero_pedido = coalesce(%s, numero_pedido),
            etapa = coalesce(%s, etapa),
            status_integracao = 'Enviado',
            data_envio_omie = now(),
            erro_integracao = null,
            payload_json = case
                when payload_json is null then %s
                else payload_json || %s
            end,
            updated_at = now()
        where id = %s
    """, (
        codigo_pedido_omie_retorno,
        numero_pedido_retorno,
        etapa_retorno,
        Json({"retorno_envio_omie": retorno_json}),
        Json({"retorno_envio_omie": retorno_json}),
        id_local
    ))

    if codigo_pedido_omie_retorno:
        cur.execute("""
            update omie_core.pedido_itens
            set
                codigo_pedido_omie = %s,
                updated_at = now()
            where pedido_id = %s
        """, (
            codigo_pedido_omie_retorno,
            id_local
        ))


def atualizar_erro(cur, id_local, mensagem):
    cur.execute("""
        update omie_core.pedidos
        set
            status_integracao = 'Erro no envio',
            erro_integracao = %s,
            updated_at = now()
        where id = %s
    """, (mensagem, id_local))


def main():
    print("🔄 Enviando pedidos prontos para o Omie...")

    conn = get_connection()
    cur = conn.cursor()

    try:
        pedidos = buscar_pedidos_prontos(cur)

        enviados = 0
        erros = 0

        for pedido in pedidos:
            id_local = pedido[0]

            try:
                itens = buscar_itens_pedido(cur, id_local)

                if not itens:
                    raise Exception("Pedido sem itens para envio.")

                payload = montar_payload(pedido, itens)
                retorno = enviar_para_omie(payload)
                print(json.dumps(payload, indent=2, ensure_ascii=False, default=decimal_to_float))

                codigo_pedido_omie_retorno, numero_pedido_retorno, etapa_retorno = extrair_retorno_omie(retorno)

                atualizar_sucesso(
                    cur,
                    id_local,
                    retorno,
                    codigo_pedido_omie_retorno,
                    numero_pedido_retorno,
                    etapa_retorno
                )

                enviados += 1

            except Exception as e:
                atualizar_erro(cur, id_local, str(e))
                erros += 1

        conn.commit()
        print(f"✅ Enviados: {enviados} | ❌ Erros: {erros}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
