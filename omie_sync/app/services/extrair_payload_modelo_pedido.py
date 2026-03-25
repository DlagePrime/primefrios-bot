import json
from decimal import Decimal
from app.db.connection import get_connection


NUMERO_PEDIDO_MODELO = "4669"


def decimal_to_float(value):
    if isinstance(value, Decimal):
        return float(value)
    return value


def main():
    conn = get_connection()
    cur = conn.cursor()

    try:
        cur.execute("""
            select
                codigo_pedido_omie,
                numero_pedido,
                codigo_cliente_omie,
                nome_fantasia_cliente_snapshot,
                cidade_cliente_snapshot,
                uf_cliente_snapshot,
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
                quantidade_itens
            from omie_core.pedidos
            where numero_pedido = %s
            limit 1
        """, (NUMERO_PEDIDO_MODELO,))

        row = cur.fetchone()
        if not row:
            raise Exception(f"Pedido modelo {NUMERO_PEDIDO_MODELO} não encontrado.")

        pedido = {
            "codigo_pedido_omie": row[0],
            "numero_pedido": row[1],
            "codigo_cliente_omie": row[2],
            "nome_fantasia_cliente_snapshot": row[3],
            "cidade_cliente_snapshot": row[4],
            "uf_cliente_snapshot": row[5],
            "codigo_vendedor_omie": row[6],
            "nome_vendedor_snapshot": row[7],
            "codigo_tabela_preco": row[8],
            "ncod_tabela_preco": row[9],
            "nome_tabela_preco_snapshot": row[10],
            "codigo_condicao_pagamento": row[11],
            "descricao_condicao_pagamento_snapshot": row[12],
            "numero_parcelas": row[13],
            "data_emissao": str(row[14]) if row[14] else None,
            "data_previsao_entrega": str(row[15]) if row[15] else None,
            "valor_produtos": decimal_to_float(row[16]),
            "valor_desconto": decimal_to_float(row[17]),
            "valor_total": decimal_to_float(row[18]),
            "status_pedido": row[19],
            "etapa": row[20],
            "pedido_bloqueado": row[21],
            "codigo_pedido_integracao": row[22],
            "codigo_cenario_impostos": row[23],
            "cenario_fiscal_snapshot": row[24],
            "codigo_empresa_omie": row[25],
            "codigo_conta_corrente_omie": row[26],
            "codigo_categoria_pedido": row[27],
            "origem_pedido": row[28],
            "quantidade_itens": row[29],
        }

        cur.execute("""
            select
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
            where codigo_pedido_omie = %s
            order by sequencia
        """, (pedido["codigo_pedido_omie"],))

        itens = []
        for item in cur.fetchall():
            itens.append({
                "codigo_produto_omie": item[0],
                "descricao_produto_snapshot": item[1],
                "unidade": item[2],
                "quantidade": decimal_to_float(item[3]),
                "valor_unitario": decimal_to_float(item[4]),
                "percentual_desconto": decimal_to_float(item[5]),
                "valor_total_item": decimal_to_float(item[6]),
                "codigo_item_omie": item[7],
                "cfop": item[8],
                "codigo_local_estoque_omie": item[9],
                "local_estoque_snapshot": item[10],
                "codigo_tabela_preco_item": item[11],
                "tabela_preco_snapshot": item[12],
                "valor_desconto_item": decimal_to_float(item[13]),
                "valor_mercadoria": decimal_to_float(item[14]),
                "codigo_categoria_item": item[15],
                "codigo_cenario_impostos_item": item[16],
            })

        payload_modelo = {
            "pedido_modelo": pedido,
            "itens_modelo": itens
        }

        print(json.dumps(payload_modelo, indent=2, ensure_ascii=False))

    finally:
        conn.close()


if __name__ == "__main__":
    main()