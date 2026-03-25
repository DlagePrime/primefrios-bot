import uuid
from datetime import date
from psycopg2.extras import Json

from app.db.connection import get_connection


def buscar_cliente(cur, codigo_cliente_omie):
    cur.execute("""
        select
            nome_fantasia,
            nome_vendedor_padrao_snapshot,
            prazo_pagamento_padrao_descricao,
            nome_tabela_preco_padrao_snapshot,
            endereco_cidade,
            endereco_uf
        from omie_core.clientes
        where codigo_cliente_omie = %s
        limit 1
    """, (codigo_cliente_omie,))
    row = cur.fetchone()
    if not row:
        return None

    return {
        "nome_fantasia": row[0],
        "nome_vendedor_padrao_snapshot": row[1],
        "prazo_pagamento_padrao_descricao": row[2],
        "nome_tabela_preco_padrao_snapshot": row[3],
        "endereco_cidade": row[4],
        "endereco_uf": row[5],
    }


def buscar_vendedor(cur, codigo_vendedor_omie):
    cur.execute("""
        select nome_vendedor
        from omie_core.vendedores
        where codigo_vendedor_omie = %s
        limit 1
    """, (codigo_vendedor_omie,))
    row = cur.fetchone()
    return row[0] if row else None


def buscar_tabela_preco(cur, codigo_tabela_preco=None, ncod_tabela_preco=None):
    if ncod_tabela_preco:
        cur.execute("""
            select nome_tabela
            from omie_core.tabelas_preco
            where ncod_tab_preco = %s
            limit 1
        """, (ncod_tabela_preco,))
        row = cur.fetchone()
        if row:
            return row[0]

    if codigo_tabela_preco:
        cur.execute("""
            select nome_tabela
            from omie_core.tabelas_preco
            where codigo_tabela_preco = %s
            limit 1
        """, (str(codigo_tabela_preco),))
        row = cur.fetchone()
        if row:
            return row[0]

    return None


def buscar_condicao_pagamento(cur, codigo_condicao_pagamento):
    cur.execute("""
        select descricao
        from omie_core.condicoes_pagamento
        where codigo_parcela_omie = %s
        limit 1
    """, (codigo_condicao_pagamento,))
    row = cur.fetchone()
    return row[0] if row else None


def classificar_cenario_fiscal(cfop):
    if cfop in ("5.102", "6.102"):
        return "Pedido de Venda"
    if cfop in ("5.910", "6.910"):
        return "Bonificação"
    return None


def buscar_nome_local(cur, codigo_local_estoque_omie):
    cur.execute("""
        select nome_local_estoque
        from omie_core.locais_estoque
        where codigo_local_estoque_omie = %s
        limit 1
    """, (codigo_local_estoque_omie,))
    row = cur.fetchone()
    return row[0] if row else None


def buscar_descricao_produto(cur, codigo_produto_omie):
    cur.execute("""
        select descricao, unidade, codigo_produto_integracao
        from omie_core.produtos
        where codigo_produto_omie = %s
        limit 1
    """, (codigo_produto_omie,))
    row = cur.fetchone()
    if not row:
        return None

    return {
        "descricao": row[0],
        "unidade": row[1],
        "codigo_produto_integracao": row[2],
    }


def criar_pedido():
    conn = get_connection()
    cur = conn.cursor()

    try:
        pedido_id = str(uuid.uuid4())

        # =========================
        # DADOS DO PEDIDO (EDITÁVEL)
        # =========================
        codigo_cliente_omie = 10797617883
        codigo_vendedor_omie = 10850370001
        codigo_tabela_preco = "5030"
        ncod_tabela_preco = 10900870329
        codigo_condicao_pagamento = "A14"
        numero_parcelas = 1
        codigo_cenario_impostos = 10720457717
        codigo_empresa_omie = 10708535689
        codigo_conta_corrente_omie = 10708538258
        codigo_categoria_pedido = "1.01.01"
        etapa = "60"

        # =========================
        # ITENS DO PEDIDO (EDITÁVEL)
        # =========================
        itens = [
            {
                "codigo_produto_omie": 10708536154,
                "quantidade": 2,
                "valor_unitario": 28.90,
                "cfop": "5.102",
                "codigo_local_estoque_omie": 10708535697
            },
            {
                "codigo_produto_omie": 10708537462,
                "quantidade": 2,
                "valor_unitario": 43.75,
                "cfop": "5.102",
                "codigo_local_estoque_omie": 10708535697
            }
        ]

        # =========================
        # ENRIQUECIMENTO
        # =========================
        cliente = buscar_cliente(cur, codigo_cliente_omie)
        nome_fantasia_cliente_snapshot = cliente["nome_fantasia"] if cliente else None
        cidade_cliente_snapshot = cliente["endereco_cidade"] if cliente else None
        uf_cliente_snapshot = cliente["endereco_uf"] if cliente else None

        nome_vendedor_snapshot = buscar_vendedor(cur, codigo_vendedor_omie)
        if not nome_vendedor_snapshot and cliente:
            nome_vendedor_snapshot = cliente["nome_vendedor_padrao_snapshot"]

        nome_tabela_preco_snapshot = buscar_tabela_preco(cur, codigo_tabela_preco, ncod_tabela_preco)
        if not nome_tabela_preco_snapshot and cliente:
            nome_tabela_preco_snapshot = cliente["nome_tabela_preco_padrao_snapshot"]

        descricao_condicao_pagamento_snapshot = buscar_condicao_pagamento(cur, codigo_condicao_pagamento)
        if not descricao_condicao_pagamento_snapshot and cliente:
            descricao_condicao_pagamento_snapshot = cliente["prazo_pagamento_padrao_descricao"]

        cenario_fiscal_snapshot = None
        for item in itens:
            cenario_fiscal_snapshot = classificar_cenario_fiscal(item.get("cfop"))
            if cenario_fiscal_snapshot:
                break

        # =========================
        # CALCULA TOTAL
        # =========================
        total_produtos = 0
        for item in itens:
            total_produtos += item["quantidade"] * item["valor_unitario"]

        # =========================
        # INSERE PEDIDO
        # =========================
        cur.execute("""
            insert into omie_core.pedidos (
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
                payload_json,
                codigo_pedido_integracao,
                codigo_cenario_impostos,
                cenario_fiscal_snapshot,
                codigo_empresa_omie,
                codigo_conta_corrente_omie,
                codigo_categoria_pedido,
                cliente_snapshot,
                origem_pedido,
                quantidade_itens,
                cidade_cliente_snapshot,
                uf_cliente_snapshot,
                status_integracao,
                erro_integracao
            )
            values (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s
            )
        """, (
            pedido_id,
            None,
            None,
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
            date.today(),
            date.today(),
            total_produtos,
            0,
            total_produtos,
            None,
            etapa,
            False,
            Json({}),
            None,
            codigo_cenario_impostos,
            cenario_fiscal_snapshot,
            codigo_empresa_omie,
            codigo_conta_corrente_omie,
            codigo_categoria_pedido,
            nome_fantasia_cliente_snapshot,
            "API",
            len(itens),
            cidade_cliente_snapshot,
            uf_cliente_snapshot,
            "Pronto para envio",
            None
        ))

        # =========================
        # INSERE ITENS
        # =========================
        for idx, item in enumerate(itens, start=1):
            total_item = item["quantidade"] * item["valor_unitario"]

            produto = buscar_descricao_produto(cur, item["codigo_produto_omie"])
            descricao_produto = produto["descricao"] if produto else None
            unidade = produto["unidade"] if produto else None
            codigo_produto_integracao = produto["codigo_produto_integracao"] if produto else None
            local_estoque_snapshot = buscar_nome_local(cur, item["codigo_local_estoque_omie"])

            cur.execute("""
                insert into omie_core.pedido_itens (
                    pedido_id,
                    codigo_pedido_omie,
                    codigo_produto_omie,
                    descricao_produto_snapshot,
                    unidade,
                    sequencia,
                    quantidade,
                    valor_unitario,
                    percentual_desconto,
                    valor_total_item,
                    payload_json,
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
                )
                values (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s
                )
            """, (
                pedido_id,
                None,
                item["codigo_produto_omie"],
                descricao_produto,
                unidade,
                idx,
                item["quantidade"],
                item["valor_unitario"],
                0,
                total_item,
                Json({
                    "codigo_produto_integracao": codigo_produto_integracao
                }),
                None,
                item["cfop"],
                item["codigo_local_estoque_omie"],
                local_estoque_snapshot,
                ncod_tabela_preco,
                nome_tabela_preco_snapshot,
                0,
                total_item,
                codigo_categoria_pedido,
                codigo_cenario_impostos
            ))

        conn.commit()
        print(f"✅ Pedido criado com sucesso! ID interno: {pedido_id}")

    finally:
        conn.close()


if __name__ == "__main__":
    criar_pedido()