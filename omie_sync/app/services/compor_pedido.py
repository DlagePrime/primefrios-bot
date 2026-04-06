import uuid
from datetime import date
from decimal import Decimal
from typing import Any, Dict, List, Optional

from psycopg2.extras import Json

from app.db.connection import get_connection


TABELAS_PRECO = {
    "5030": {
        "requeijao premium": Decimal("42.00"),
        "cheddar premium": Decimal("43.75"),
        "cheddar tradicional": Decimal("31.60"),
        "doce leite bisnaga": Decimal("29.90"),
        "doce leite balde": Decimal("95.90"),
        "doce chocolate bisnaga": Decimal("33.90"),
        "doce chocolate balde": Decimal("110.90"),
        "cream cheese bisnaga": Decimal("28.90"),
        "cream cheese balde": Decimal("107.95"),
    },
    "5060": {
        "requeijao premium": Decimal("40.30"),
        "cheddar premium": Decimal("43.75"),
        "cheddar tradicional": Decimal("31.60"),
        "doce leite bisnaga": Decimal("29.90"),
        "doce leite balde": Decimal("95.50"),
        "doce chocolate bisnaga": Decimal("33.90"),
        "doce chocolate balde": Decimal("110.90"),
        "cream cheese bisnaga": Decimal("28.90"),
        "cream cheese balde": Decimal("107.90"),
    },
    "5070": {
        "requeijao premium": Decimal("39.90"),
        "cheddar premium": Decimal("43.75"),
        "cheddar tradicional": Decimal("31.60"),
        "doce leite bisnaga": Decimal("29.90"),
        "doce leite balde": Decimal("95.50"),
        "doce chocolate bisnaga": Decimal("33.90"),
        "doce chocolate balde": Decimal("110.90"),
        "cream cheese bisnaga": Decimal("28.90"),
        "cream cheese balde": Decimal("107.90"),
    },
}

MAPA_ALIASES = {
    "requeijao premium": ["requeijao premium", "requeijão premium", "req premium", "requeijao puro", "requeijão puro", "req puro", "50040"],
    "cheddar premium": ["cheddar premium", "req cheddar", "cheddar 1,5", "50041"],
    "cheddar tradicional": ["cheddar tradicional", "mistura cheddar", "cheddar tradicional 1,8"],
    "doce leite bisnaga": ["doce leite bisnaga", "doce bisnaga", "doce de leite bisnaga", "60010"],
    "doce leite balde": ["doce leite balde", "doce balde", "doce de leite balde", "60008"],
    "doce chocolate bisnaga": ["doce chocolate bisnaga", "doce de leite com chocolate bisnaga", "60002"],
    "doce chocolate balde": ["doce chocolate balde", "doce de leite com chocolate balde", "60004"],
    "cream cheese bisnaga": ["cream cheese bisnaga", "cream cheese 1,010", "180001"],
    "cream cheese balde": ["cream cheese balde", "cream cheese 3,6", "prd00001"],
}

EMBALAGEM_POR_CHAVE = {
    "requeijao premium": "UN",
    "cheddar premium": "UN",
    "cheddar tradicional": "UN",
    "doce leite bisnaga": "UN",
    "doce leite balde": "UN",
    "doce chocolate bisnaga": "UN",
    "doce chocolate balde": "UN",
    "cream cheese bisnaga": "UN",
    "cream cheese balde": "UN",
}

CAIXA_POR_CHAVE = {
    "requeijao premium": 10,
    "cheddar premium": 10,
    "cheddar tradicional": 8,
    "doce leite bisnaga": 12,
    "doce leite balde": 4,
    "doce chocolate bisnaga": 12,
    "doce chocolate balde": 4,
    "cream cheese bisnaga": 12,
    "cream cheese balde": 4,
}


def decimal_to_float(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    return value


def normalizar_texto(texto: str) -> str:
    if not texto:
        return ""
    return (
        texto.lower()
        .replace("ç", "c")
        .replace("ã", "a")
        .replace("á", "a")
        .replace("à", "a")
        .replace("â", "a")
        .replace("é", "e")
        .replace("ê", "e")
        .replace("í", "i")
        .replace("ó", "o")
        .replace("ô", "o")
        .replace("õ", "o")
        .replace("ú", "u")
        .strip()
    )


def to_decimal(value: Any, default: str = "0") -> Decimal:
    if isinstance(value, Decimal):
        return value
    if value in (None, "", "-"):
        return Decimal(default)

    texto = str(value).strip().replace("R$", "").replace(" ", "")
    texto = texto.replace(".", "").replace(",", ".") if "," in texto else texto

    try:
        return Decimal(texto)
    except Exception:
        return Decimal(default)


def to_int_or_none(value: Any) -> Optional[int]:
    if value in (None, "", "-"):
        return None
    try:
        return int(value)
    except Exception:
        try:
            return int(float(str(value).replace(",", ".")))
        except Exception:
            return None


def to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in ("1", "true", "verdadeiro", "sim", "yes")


def parse_date_or_today(value: Any) -> date:
    if not value:
        return date.today()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value))
    except Exception:
        return date.today()


def classificar_cenario_fiscal(uf_cliente: str, tipo_pedido: str) -> Dict[str, str]:
    uf = (uf_cliente or "").upper().strip()
    tipo = (tipo_pedido or "venda").lower().strip()

    if tipo == "bonificacao":
        return {
            "cfop": "5.910" if uf == "MG" else "6.910",
            "cenario_fiscal": "Bonificação",
        }

    return {
        "cfop": "5.102" if uf == "MG" else "6.102",
        "cenario_fiscal": "Pedido de Venda",
    }


def resolver_local_por_operacao(operacao_destino: str, uf_cliente: str) -> int:
    operacao = (operacao_destino or "").upper().strip()
    uf = (uf_cliente or "").upper().strip()

    if operacao in ("PI", "MA"):
        return 11024975135

    if uf in ("PI", "MA"):
        return 11024975135

    return 10708535697


def buscar_cliente_por_codigo(cur, codigo_cliente_omie: int) -> Optional[Dict[str, Any]]:
    cur.execute("""
        select
            codigo_cliente_omie,
            nome_fantasia,
            whatsapp,
            codigo_vendedor_padrao_omie,
            nome_vendedor_padrao_snapshot,
            codigo_condicao_pagamento_padrao_omie,
            prazo_pagamento_padrao_descricao,
            numero_parcelas_padrao,
            codigo_tabela_preco_padrao,
            ncod_tabela_preco_padrao,
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
        "codigo_cliente_omie": row[0],
        "nome_fantasia": row[1],
        "whatsapp": row[2],
        "codigo_vendedor_padrao_omie": row[3],
        "nome_vendedor_padrao_snapshot": row[4],
        "codigo_condicao_pagamento_padrao_omie": row[5],
        "prazo_pagamento_padrao_descricao": row[6],
        "numero_parcelas_padrao": row[7],
        "codigo_tabela_preco_padrao": row[8],
        "ncod_tabela_preco_padrao": row[9],
        "nome_tabela_preco_padrao_snapshot": row[10],
        "endereco_cidade": row[11],
        "endereco_uf": row[12],
    }


def buscar_cliente_por_whatsapp(cur, whatsapp: str) -> Optional[Dict[str, Any]]:
    if not whatsapp:
        return None

    numero = "".join(ch for ch in whatsapp if ch.isdigit())

    cur.execute("""
        select
            codigo_cliente_omie,
            nome_fantasia,
            whatsapp,
            codigo_vendedor_padrao_omie,
            nome_vendedor_padrao_snapshot,
            codigo_condicao_pagamento_padrao_omie,
            prazo_pagamento_padrao_descricao,
            numero_parcelas_padrao,
            codigo_tabela_preco_padrao,
            ncod_tabela_preco_padrao,
            nome_tabela_preco_padrao_snapshot,
            endereco_cidade,
            endereco_uf
        from omie_core.clientes
        where regexp_replace(coalesce(whatsapp, ''), '\D', '', 'g') = %s
        limit 1
    """, (numero,))
    row = cur.fetchone()
    if not row:
        return None

    return {
        "codigo_cliente_omie": row[0],
        "nome_fantasia": row[1],
        "whatsapp": row[2],
        "codigo_vendedor_padrao_omie": row[3],
        "nome_vendedor_padrao_snapshot": row[4],
        "codigo_condicao_pagamento_padrao_omie": row[5],
        "prazo_pagamento_padrao_descricao": row[6],
        "numero_parcelas_padrao": row[7],
        "codigo_tabela_preco_padrao": row[8],
        "ncod_tabela_preco_padrao": row[9],
        "nome_tabela_preco_padrao_snapshot": row[10],
        "endereco_cidade": row[11],
        "endereco_uf": row[12],
    }


def buscar_vendedor_nome(cur, codigo_vendedor_omie: int) -> Optional[str]:
    cur.execute("""
        select nome_vendedor
        from omie_core.vendedores
        where codigo_vendedor_omie = %s
        limit 1
    """, (codigo_vendedor_omie,))
    row = cur.fetchone()
    return row[0] if row else None


def buscar_tabela_nome(cur, ncod_tabela_preco: Optional[int], codigo_tabela_preco: Optional[str]) -> Optional[str]:
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
        """, (codigo_tabela_preco,))
        row = cur.fetchone()
        if row:
            return row[0]

    return None


def buscar_condicao_descricao(cur, codigo_condicao_pagamento: str) -> Optional[str]:
    cur.execute("""
        select descricao
        from omie_core.condicoes_pagamento
        where codigo_parcela_omie = %s
        limit 1
    """, (codigo_condicao_pagamento,))
    row = cur.fetchone()
    return row[0] if row else None


def buscar_nome_local(cur, codigo_local_estoque_omie: int) -> Optional[str]:
    cur.execute("""
        select nome_local_estoque
        from omie_core.locais_estoque
        where codigo_local_estoque_omie = %s
        limit 1
    """, (codigo_local_estoque_omie,))
    row = cur.fetchone()
    return row[0] if row else None


def resolver_chave_produto(texto_produto: str) -> Optional[str]:
    texto = normalizar_texto(texto_produto)

    for chave, aliases in MAPA_ALIASES.items():
        for alias in aliases:
            if alias in texto:
                return chave

    return None


def buscar_produto_por_texto(cur, texto_produto: str) -> Optional[Dict[str, Any]]:
    chave = resolver_chave_produto(texto_produto)
    if not chave:
        return None

    aliases = MAPA_ALIASES[chave]

    for alias in aliases:
        cur.execute("""
            select
                codigo_produto_omie,
                descricao,
                unidade,
                codigo_produto_integracao
            from omie_core.produtos
            where lower(descricao) like %s
               or lower(coalesce(codigo_produto_integracao, '')) = %s
            limit 1
        """, (f"%{normalizar_texto(alias)}%", normalizar_texto(alias)))
        row = cur.fetchone()
        if row:
            return {
                "chave": chave,
                "codigo_produto_omie": row[0],
                "descricao": row[1],
                "unidade": row[2],
                "codigo_produto_integracao": row[3],
            }

    return None


def buscar_produto_por_codigo(cur, codigo_produto_omie: int) -> Optional[Dict[str, Any]]:
    cur.execute("""
        select
            codigo_produto_omie,
            descricao,
            unidade,
            codigo_produto_integracao
        from omie_core.produtos
        where codigo_produto_omie = %s
        limit 1
    """, (codigo_produto_omie,))
    row = cur.fetchone()
    if not row:
        return None

    return {
        "codigo_produto_omie": row[0],
        "descricao": row[1],
        "unidade": row[2],
        "codigo_produto_integracao": row[3],
    }


def converter_quantidade(chave_produto: str, quantidade: Any, unidade: str) -> Decimal:
    qtd = Decimal(str(quantidade or 0))
    und = (unidade or "UN").upper().strip()

    if und == "CX":
        fator = CAIXA_POR_CHAVE.get(chave_produto, 1)
        return qtd * Decimal(str(fator))

    return qtd


def resolver_preco_unitario(codigo_tabela_preco: str, chave_produto: str) -> Decimal:
    tabela = TABELAS_PRECO.get(str(codigo_tabela_preco))
    if not tabela:
        raise Exception(f"Tabela de preço {codigo_tabela_preco} não mapeada no motor de regras.")

    preco = tabela.get(chave_produto)
    if preco is None:
        raise Exception(f"Produto '{chave_produto}' sem preço na tabela {codigo_tabela_preco}.")

    return preco


def validar_estoque(cur, codigo_produto_omie: int, codigo_local_estoque_omie: int, quantidade: Decimal):
    cur.execute("""
        select quantidade_disponivel
        from omie_core.estoque_produtos
        where codigo_produto_omie = %s
          and codigo_local_estoque_omie = %s
        limit 1
    """, (codigo_produto_omie, codigo_local_estoque_omie))
    row = cur.fetchone()

    if not row:
        raise Exception(
            f"Estoque não encontrado para produto {codigo_produto_omie} no local {codigo_local_estoque_omie}."
        )

    saldo = Decimal(str(row[0] or 0))
    if saldo < quantidade:
        raise Exception(
            f"Estoque insuficiente para produto {codigo_produto_omie} no local {codigo_local_estoque_omie}. Solicitado {quantidade}, disponível {saldo}."
        )


def compor_pedido(payload: Dict[str, Any]) -> Dict[str, Any]:
    conn = get_connection()
    cur = conn.cursor()

    try:
        pedido_extraido = payload.get("pedido_extraido") or {}
        payload_envio_omie = payload.get("payload_envio_omie") or {}

        modo_payload_pronto = bool(payload_envio_omie)

        if modo_payload_pronto:
            pedido_pronto = payload_envio_omie.get("pedido") or {}
            itens_entrada = payload_envio_omie.get("itens") or []
            codigo_cliente_omie = pedido_pronto.get("codigo_cliente_omie")
        else:
            pedido_pronto = {}
            itens_entrada = pedido_extraido.get("itens") or []
            codigo_cliente_omie = pedido_extraido.get("codigo_cliente_omie")

        if not itens_entrada:
            raise Exception("Nenhum item foi informado para compor o pedido.")

        remotejid = payload.get("remotejid") or ""
        whatsapp_numero = "".join(ch for ch in str(remotejid) if ch.isdigit())

        cliente = None
        if codigo_cliente_omie:
            cliente = buscar_cliente_por_codigo(cur, int(codigo_cliente_omie))
        if not cliente and whatsapp_numero:
            cliente = buscar_cliente_por_whatsapp(cur, whatsapp_numero)

        if not cliente:
            raise Exception("Cliente não encontrado pelo código nem pelo WhatsApp.")

        codigo_cliente_omie = cliente["codigo_cliente_omie"]
        nome_fantasia_cliente_snapshot = cliente["nome_fantasia"]
        cidade_cliente_snapshot = cliente["endereco_cidade"]
        uf_cliente_snapshot = cliente["endereco_uf"]

        if modo_payload_pronto:
            codigo_vendedor_omie = to_int_or_none(pedido_pronto.get("codigo_vendedor_omie")) or cliente["codigo_vendedor_padrao_omie"]
            codigo_condicao_pagamento = str(pedido_pronto.get("codigo_condicao_pagamento") or cliente["codigo_condicao_pagamento_padrao_omie"] or "")
            numero_parcelas = to_int_or_none(pedido_pronto.get("numero_parcelas")) or cliente["numero_parcelas_padrao"] or 1
            codigo_tabela_preco = str(pedido_pronto.get("codigo_tabela_preco") or cliente["codigo_tabela_preco_padrao"] or "5030")
            ncod_tabela_preco = to_int_or_none(pedido_pronto.get("ncod_tabela_preco")) or cliente["ncod_tabela_preco_padrao"]

            codigo_cenario_impostos = to_int_or_none(pedido_pronto.get("codigo_cenario_impostos")) or 10720457717
            tipo_pedido = "bonificacao" if codigo_cenario_impostos == 10907366523 else "venda"
            operacao_destino = pedido_pronto.get("operacao_destino") or "AUTO"

            codigo_empresa_omie = to_int_or_none(pedido_pronto.get("codigo_empresa_omie")) or 10708535689
            codigo_conta_corrente_omie = to_int_or_none(pedido_pronto.get("codigo_conta_corrente_omie")) or 10708538258
            codigo_categoria_pedido = str(pedido_pronto.get("codigo_categoria_pedido") or "1.01.01")
            etapa = str(pedido_pronto.get("etapa") or "10")
            pedido_bloqueado = to_bool(pedido_pronto.get("pedido_bloqueado"))
            codigo_pedido_integracao = str(pedido_pronto.get("codigo_pedido_integracao") or str(uuid.uuid4()))
            data_emissao = parse_date_or_today(pedido_pronto.get("data_emissao"))
            data_previsao_entrega = parse_date_or_today(pedido_pronto.get("data_previsao_entrega"))
        else:
            codigo_vendedor_omie = cliente["codigo_vendedor_padrao_omie"]
            codigo_condicao_pagamento = cliente["codigo_condicao_pagamento_padrao_omie"]
            numero_parcelas = cliente["numero_parcelas_padrao"] or 1
            codigo_tabela_preco = cliente["codigo_tabela_preco_padrao"] or "5030"
            ncod_tabela_preco = cliente["ncod_tabela_preco_padrao"]

            tipo_pedido = (pedido_extraido.get("tipo_pedido") or "venda").lower().strip()
            operacao_destino = pedido_extraido.get("operacao_destino") or "AUTO"

            if tipo_pedido == "bonificacao":
                codigo_cenario_impostos = 10907366523
            else:
                codigo_cenario_impostos = 10720457717

            codigo_empresa_omie = 10708535689
            codigo_conta_corrente_omie = 10708538258
            codigo_categoria_pedido = "1.01.01"
            etapa = "10"
            pedido_bloqueado = False
            codigo_pedido_integracao = str(uuid.uuid4())
            data_emissao = date.today()
            data_previsao_entrega = date.today()

        nome_vendedor_snapshot = buscar_vendedor_nome(cur, codigo_vendedor_omie) or cliente["nome_vendedor_padrao_snapshot"]
        descricao_condicao_pagamento_snapshot = buscar_condicao_descricao(cur, codigo_condicao_pagamento) or cliente["prazo_pagamento_padrao_descricao"]
        nome_tabela_preco_snapshot = buscar_tabela_nome(cur, ncod_tabela_preco, codigo_tabela_preco) or cliente["nome_tabela_preco_padrao_snapshot"]

        regra_fiscal = classificar_cenario_fiscal(uf_cliente_snapshot, tipo_pedido)
        cenario_fiscal_snapshot = regra_fiscal["cenario_fiscal"]

        codigo_local_estoque_padrao = resolver_local_por_operacao(operacao_destino, uf_cliente_snapshot)
        local_estoque_padrao_snapshot = buscar_nome_local(cur, codigo_local_estoque_padrao)

        pedido_id = str(uuid.uuid4())

        itens_resolvidos: List[Dict[str, Any]] = []
        total_produtos = Decimal("0")

        for idx, item in enumerate(itens_entrada, start=1):
            if modo_payload_pronto:
                codigo_produto_omie_item = to_int_or_none(item.get("codigo_produto_omie"))
                if not codigo_produto_omie_item:
                    raise Exception(f"Item {idx} sem codigo_produto_omie.")

                produto = buscar_produto_por_codigo(cur, codigo_produto_omie_item)
                if not produto:
                    raise Exception(f"Produto não encontrado pelo código: {codigo_produto_omie_item}")

                quantidade_final = to_decimal(item.get("quantidade"))
                valor_unitario = to_decimal(item.get("valor_unitario"))
                percentual_desconto = to_decimal(item.get("percentual_desconto"))
                valor_desconto_item = to_decimal(item.get("valor_desconto_item"))
                valor_total_item = to_decimal(item.get("valor_total_item"))
                valor_mercadoria = to_decimal(item.get("valor_mercadoria"))

                if valor_total_item == 0:
                    valor_total_item = quantidade_final * valor_unitario

                if valor_mercadoria == 0:
                    valor_mercadoria = valor_total_item

                codigo_local_estoque_omie = to_int_or_none(item.get("codigo_local_estoque_omie")) or codigo_local_estoque_padrao
                local_estoque_snapshot = buscar_nome_local(cur, codigo_local_estoque_omie) or local_estoque_padrao_snapshot

                validar_estoque(cur, codigo_produto_omie_item, codigo_local_estoque_omie, quantidade_final)

                total_produtos += valor_total_item

                itens_resolvidos.append({
                    "sequencia": idx,
                    "codigo_produto_omie": codigo_produto_omie_item,
                    "descricao_produto_snapshot": produto["descricao"],
                    "unidade": item.get("unidade") or produto["unidade"] or "UN",
                    "quantidade": quantidade_final,
                    "valor_unitario": valor_unitario,
                    "percentual_desconto": percentual_desconto,
                    "valor_total_item": valor_total_item,
                    "cfop": item.get("cfop") or regra_fiscal["cfop"],
                    "codigo_local_estoque_omie": codigo_local_estoque_omie,
                    "local_estoque_snapshot": local_estoque_snapshot,
                    "codigo_tabela_preco_item": to_int_or_none(item.get("codigo_tabela_preco_item")) or ncod_tabela_preco or int(codigo_tabela_preco),
                    "tabela_preco_snapshot": nome_tabela_preco_snapshot,
                    "valor_desconto_item": valor_desconto_item,
                    "valor_mercadoria": valor_mercadoria,
                    "codigo_categoria_item": item.get("codigo_categoria_item") or codigo_categoria_pedido,
                    "codigo_cenario_impostos_item": to_int_or_none(item.get("codigo_cenario_impostos_item")) or codigo_cenario_impostos,
                    "payload_json": {
                        "fonte": "payload_envio_omie",
                        "item_original": item,
                        "codigo_produto_integracao": produto["codigo_produto_integracao"],
                    }
                })
            else:
                produto_texto = item.get("produto_texto") or ""
                quantidade_raw = item.get("quantidade") or 0
                unidade_raw = item.get("unidade") or "UN"

                produto = buscar_produto_por_texto(cur, produto_texto)
                if not produto:
                    raise Exception(f"Produto não encontrado a partir do texto: {produto_texto}")

                chave_produto = produto["chave"]
                quantidade_final = converter_quantidade(chave_produto, quantidade_raw, unidade_raw)
                valor_unitario = resolver_preco_unitario(codigo_tabela_preco, chave_produto)

                validar_estoque(cur, produto["codigo_produto_omie"], codigo_local_estoque_padrao, quantidade_final)

                valor_total_item = quantidade_final * valor_unitario
                total_produtos += valor_total_item

                itens_resolvidos.append({
                    "sequencia": idx,
                    "codigo_produto_omie": produto["codigo_produto_omie"],
                    "descricao_produto_snapshot": produto["descricao"],
                    "unidade": produto["unidade"] or EMBALAGEM_POR_CHAVE.get(chave_produto, "UN"),
                    "quantidade": quantidade_final,
                    "valor_unitario": valor_unitario,
                    "percentual_desconto": Decimal("0"),
                    "valor_total_item": valor_total_item,
                    "cfop": regra_fiscal["cfop"],
                    "codigo_local_estoque_omie": codigo_local_estoque_padrao,
                    "local_estoque_snapshot": local_estoque_padrao_snapshot,
                    "codigo_tabela_preco_item": ncod_tabela_preco or int(codigo_tabela_preco),
                    "tabela_preco_snapshot": nome_tabela_preco_snapshot,
                    "valor_desconto_item": Decimal("0"),
                    "valor_mercadoria": valor_total_item,
                    "codigo_categoria_item": codigo_categoria_pedido,
                    "codigo_cenario_impostos_item": codigo_cenario_impostos,
                    "payload_json": {
                        "produto_texto_original": produto_texto,
                        "unidade_original": unidade_raw,
                        "quantidade_original": quantidade_raw,
                        "codigo_produto_integracao": produto["codigo_produto_integracao"],
                    }
                })

        if modo_payload_pronto:
            valor_produtos_pedido = to_decimal(pedido_pronto.get("valor_produtos"))
            valor_desconto_pedido = to_decimal(pedido_pronto.get("valor_desconto"))
            valor_total_pedido = to_decimal(pedido_pronto.get("valor_total"))

            if valor_produtos_pedido == 0:
                valor_produtos_pedido = total_produtos

            if valor_total_pedido == 0:
                valor_total_pedido = valor_produtos_pedido - valor_desconto_pedido

            status_integracao = "Pronto para envio"
            origem_pedido = pedido_pronto.get("origem_pedido") or "N8N"
        else:
            confirmado = bool(pedido_extraido.get("confirmado"))
            status_integracao = "Pronto para envio" if confirmado else "Aguardando confirmação"
            valor_produtos_pedido = total_produtos
            valor_desconto_pedido = Decimal("0")
            valor_total_pedido = total_produtos
            origem_pedido = "API"

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
            data_emissao,
            data_previsao_entrega,
            valor_produtos_pedido,
            valor_desconto_pedido,
            valor_total_pedido,
            None,
            etapa,
            pedido_bloqueado,
            Json({
                "fonte": "payload_envio_omie" if modo_payload_pronto else "n8n_ia",
                "entrada_original": payload,
            }),
            codigo_pedido_integracao,
            codigo_cenario_impostos,
            cenario_fiscal_snapshot,
            codigo_empresa_omie,
            codigo_conta_corrente_omie,
            codigo_categoria_pedido,
            nome_fantasia_cliente_snapshot,
            origem_pedido,
            len(itens_resolvidos),
            cidade_cliente_snapshot,
            uf_cliente_snapshot,
            status_integracao,
            None
        ))

        for item in itens_resolvidos:
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
                item["descricao_produto_snapshot"],
                item["unidade"],
                item["sequencia"],
                item["quantidade"],
                item["valor_unitario"],
                item["percentual_desconto"],
                item["valor_total_item"],
                Json(item["payload_json"]),
                None,
                item["cfop"],
                item["codigo_local_estoque_omie"],
                item["local_estoque_snapshot"],
                item["codigo_tabela_preco_item"],
                item["tabela_preco_snapshot"],
                item["valor_desconto_item"],
                item["valor_mercadoria"],
                item["codigo_categoria_item"],
                item["codigo_cenario_impostos_item"],
            ))

        conn.commit()

        resumo_itens = []
        for item in itens_resolvidos:
            resumo_itens.append({
                "codigo_produto_omie": item["codigo_produto_omie"],
                "descricao": item["descricao_produto_snapshot"],
                "quantidade": decimal_to_float(item["quantidade"]),
                "valor_unitario": decimal_to_float(item["valor_unitario"]),
                "valor_total_item": decimal_to_float(item["valor_total_item"]),
                "cfop": item["cfop"],
                "local": item["local_estoque_snapshot"],
            })

        return {
            "ok": True,
            "modo": "payload_envio_omie" if modo_payload_pronto else "pedido_extraido",
            "pedido_id": pedido_id,
            "status_integracao": status_integracao,
            "codigo_cliente_omie": codigo_cliente_omie,
            "cliente": nome_fantasia_cliente_snapshot,
            "cidade": cidade_cliente_snapshot,
            "uf": uf_cliente_snapshot,
            "codigo_tabela_preco": codigo_tabela_preco,
            "ncod_tabela_preco": ncod_tabela_preco,
            "nome_tabela_preco_snapshot": nome_tabela_preco_snapshot,
            "codigo_condicao_pagamento": codigo_condicao_pagamento,
            "descricao_condicao_pagamento_snapshot": descricao_condicao_pagamento_snapshot,
            "codigo_vendedor_omie": codigo_vendedor_omie,
            "nome_vendedor_snapshot": nome_vendedor_snapshot,
            "codigo_cenario_impostos": codigo_cenario_impostos,
            "cenario_fiscal_snapshot": cenario_fiscal_snapshot,
            "codigo_local_estoque_omie_padrao": codigo_local_estoque_padrao,
            "local_estoque_padrao_snapshot": local_estoque_padrao_snapshot,
            "valor_total": decimal_to_float(valor_total_pedido),
            "itens": resumo_itens,
        }

    finally:
        conn.close()
