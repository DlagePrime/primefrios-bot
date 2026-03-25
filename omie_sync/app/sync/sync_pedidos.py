import os
import sys
from datetime import datetime, timedelta
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

URL_PEDIDOS = "https://app.omie.com.br/api/v1/produtos/pedido/"


def listar_pedidos(pagina=1, registros_por_pagina=100):
    payload = {
        "call": "ListarPedidos",
        "app_key": OMIE_APP_KEY,
        "app_secret": OMIE_APP_SECRET,
        "param": [{
            "pagina": pagina,
            "registros_por_pagina": registros_por_pagina,
            "apenas_importado_api": "N"
        }]
    }

    response = requests.post(URL_PEDIDOS, json=payload, timeout=60)
    response.raise_for_status()
    return response.json()


def parse_date(value):
    if not value:
        return None
    try:
        if "/" in value:
            dia, mes, ano = value.split("/")
            return f"{ano}-{mes}-{dia}"
        return value
    except Exception:
        return None


def parse_datetime_omie(data_str, hora_str=None):
    data_iso = parse_date(data_str)
    if not data_iso:
        return None

    hora_str = (hora_str or "00:00:00").strip()
    if len(hora_str) == 5:
        hora_str = f"{hora_str}:00"

    try:
        return datetime.strptime(f"{data_iso} {hora_str}", "%Y-%m-%d %H:%M:%S")
    except Exception:
        try:
            return datetime.strptime(data_iso, "%Y-%m-%d")
        except Exception:
            return None


def to_bool_omie(value):
    return value in ("S", "s", True)


def buscar_cliente(cur, codigo_cliente_omie):
    if not codigo_cliente_omie:
        return None

    cur.execute("""
        select
            nome_fantasia,
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
        "nome_fantasia": row[0],
        "codigo_vendedor_padrao_omie": row[1],
        "nome_vendedor_padrao_snapshot": row[2],
        "codigo_condicao_pagamento_padrao_omie": row[3],
        "prazo_pagamento_padrao_descricao": row[4],
        "numero_parcelas_padrao": row[5],
        "codigo_tabela_preco_padrao": row[6],
        "ncod_tabela_preco_padrao": row[7],
        "nome_tabela_preco_padrao_snapshot": row[8],
        "endereco_cidade": row[9],
        "endereco_uf": row[10],
    }


def buscar_condicao_pagamento(cur, codigo):
    if not codigo:
        return None

    cur.execute("""
        select
            descricao,
            numero_parcelas_padrao
        from omie_core.condicoes_pagamento
        where codigo_parcela_omie = %s
        limit 1
    """, (codigo,))

    row = cur.fetchone()
    if not row:
        return None

    return {
        "descricao": row[0],
        "numero_parcelas_padrao": row[1],
    }


def buscar_tabela_preco_por_codigo(cur, codigo):
    if codigo is None:
        return None

    cur.execute("""
        select
            codigo_tabela_preco,
            ncod_tab_preco,
            nome_tabela
        from omie_core.tabelas_preco
        where codigo_tabela_preco = %s
        limit 1
    """, (str(codigo),))

    row = cur.fetchone()
    if not row:
        return None

    return {
        "codigo_tabela_preco": row[0],
        "ncod_tab_preco": row[1],
        "nome_tabela": row[2],
    }


def buscar_tabela_preco_por_ncod(cur, codigo):
    if not codigo:
        return None

    cur.execute("""
        select
            codigo_tabela_preco,
            ncod_tab_preco,
            nome_tabela
        from omie_core.tabelas_preco
        where ncod_tab_preco = %s
        limit 1
    """, (codigo,))

    row = cur.fetchone()
    if not row:
        return None

    return {
        "codigo_tabela_preco": row[0],
        "ncod_tab_preco": row[1],
        "nome_tabela": row[2],
    }


def classificar_cenario_fiscal(cfop):
    if cfop in ("5.102", "6.102"):
        return "Pedido de Venda"
    if cfop in ("5.910", "6.910"):
        return "Bonificação"
    return None


def obter_datetimes_relevantes_pedido(pedido):
    info_cadastro = pedido.get("infoCadastro", {}) or {}

    datetimes = [
        parse_datetime_omie(info_cadastro.get("dInc"), info_cadastro.get("hInc")),
        parse_datetime_omie(info_cadastro.get("dAlt"), info_cadastro.get("hAlt")),
        parse_datetime_omie(info_cadastro.get("dFat"), info_cadastro.get("hFat")),
    ]

    return [dt for dt in datetimes if dt is not None]


def pedido_esta_na_janela(pedido, data_hora_minima):
    datetimes_relevantes = obter_datetimes_relevantes_pedido(pedido)
    if not datetimes_relevantes:
        return False
    return any(dt >= data_hora_minima for dt in datetimes_relevantes)


def pedido_eh_antigo(pedido, data_hora_minima):
    datetimes_relevantes = obter_datetimes_relevantes_pedido(pedido)
    if not datetimes_relevantes:
        return True
    return all(dt < data_hora_minima for dt in datetimes_relevantes)


def salvar_pedido(conn, pedido):
    cur = conn.cursor()

    cabecalho = pedido.get("cabecalho", {}) or {}
    info_adicionais = pedido.get("informacoes_adicionais", {}) or {}
    total_pedido = pedido.get("total_pedido", {}) or {}

    codigo_pedido = cabecalho.get("codigo_pedido")
    if not codigo_pedido:
        print("⚠️ Pedido ignorado sem codigo_pedido")
        return False

    numero_pedido = str(cabecalho.get("numero_pedido")) if cabecalho.get("numero_pedido") is not None else None
    codigo_cliente = cabecalho.get("codigo_cliente")
    codigo_vendedor = info_adicionais.get("codVend")

    codigo_condicao_pagamento = cabecalho.get("codigo_parcela")
    numero_parcelas = cabecalho.get("qtde_parcelas")

    data_emissao = parse_date((pedido.get("infoCadastro", {}) or {}).get("dInc"))
    data_previsao_entrega = parse_date(cabecalho.get("data_previsao"))

    valor_produtos = total_pedido.get("valor_mercadorias", 0) or 0
    valor_desconto = total_pedido.get("valor_descontos", 0) or 0
    valor_total = total_pedido.get("valor_total_pedido", 0) or 0

    info_cadastro = pedido.get("infoCadastro", {}) or {}
    if info_cadastro.get("cancelado") == "S":
        status_pedido = "CANCELADO"
    elif info_cadastro.get("faturado") == "S":
        status_pedido = "FATURADO"
    elif cabecalho.get("encerrado") == "S":
        status_pedido = "ENCERRADO"
    else:
        status_pedido = "ABERTO"

    etapa = cabecalho.get("etapa")
    pedido_bloqueado = to_bool_omie(cabecalho.get("bloqueado"))

    codigo_pedido_integracao = cabecalho.get("codigo_pedido_integracao")
    codigo_cenario_impostos = cabecalho.get("codigo_cenario_impostos")
    codigo_empresa_omie = cabecalho.get("codigo_empresa")
    codigo_conta_corrente_omie = info_adicionais.get("codigo_conta_corrente")
    codigo_categoria_pedido = info_adicionais.get("codigo_categoria")
    origem_pedido = cabecalho.get("origem_pedido")
    quantidade_itens = cabecalho.get("quantidade_itens")

    cliente = buscar_cliente(cur, codigo_cliente)

    nome_fantasia_cliente_snapshot = None
    cidade_cliente_snapshot = None
    uf_cliente_snapshot = None
    nome_vendedor_snapshot = None

    if cliente:
        nome_fantasia_cliente_snapshot = cliente["nome_fantasia"]
        cidade_cliente_snapshot = cliente["endereco_cidade"]
        uf_cliente_snapshot = cliente["endereco_uf"]

        if not codigo_vendedor or int(codigo_vendedor) == 0:
            codigo_vendedor = cliente["codigo_vendedor_padrao_omie"]

        nome_vendedor_snapshot = cliente["nome_vendedor_padrao_snapshot"]

    descricao_condicao_pagamento_snapshot = None
    condicao = buscar_condicao_pagamento(cur, codigo_condicao_pagamento)

    if condicao:
        descricao_condicao_pagamento_snapshot = condicao["descricao"]
        if not numero_parcelas:
            numero_parcelas = condicao["numero_parcelas_padrao"]
    elif cliente:
        descricao_condicao_pagamento_snapshot = cliente["prazo_pagamento_padrao_descricao"]
        if not numero_parcelas:
            numero_parcelas = cliente["numero_parcelas_padrao"]

    codigo_tabela_preco = None
    ncod_tabela_preco = None
    nome_tabela_preco_snapshot = None
    cenario_fiscal_snapshot = None

    for item in pedido.get("det", []) or []:
        produto = item.get("produto", {}) or {}

        codigo_tabela_preco_item = produto.get("codigo_tabela_preco")
        cfop_item = produto.get("cfop")

        if cenario_fiscal_snapshot is None and cfop_item:
            cenario_fiscal_snapshot = classificar_cenario_fiscal(cfop_item)

        if codigo_tabela_preco_item is None:
            continue

        if isinstance(codigo_tabela_preco_item, int) and codigo_tabela_preco_item > 999999:
            ncod_tabela_preco = codigo_tabela_preco_item
            tabela = buscar_tabela_preco_por_ncod(cur, codigo_tabela_preco_item)
            if tabela:
                codigo_tabela_preco = tabela["codigo_tabela_preco"]
                nome_tabela_preco_snapshot = tabela["nome_tabela"]
            else:
                codigo_tabela_preco = str(codigo_tabela_preco_item)
            break

        tabela = buscar_tabela_preco_por_codigo(cur, codigo_tabela_preco_item)
        if tabela:
            codigo_tabela_preco = str(codigo_tabela_preco_item)
            ncod_tabela_preco = tabela["ncod_tab_preco"]
            nome_tabela_preco_snapshot = tabela["nome_tabela"]
        else:
            codigo_tabela_preco = str(codigo_tabela_preco_item)
        break

    if not codigo_tabela_preco and cliente:
        codigo_tabela_preco = cliente["codigo_tabela_preco_padrao"]
        ncod_tabela_preco = cliente["ncod_tabela_preco_padrao"]
        nome_tabela_preco_snapshot = cliente["nome_tabela_preco_padrao_snapshot"]

    status_integracao = "Omie"

    cur.execute("""
        insert into omie_core.pedidos (
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
            status_integracao,
            codigo_pedido_integracao,
            codigo_cenario_impostos,
            cenario_fiscal_snapshot,
            codigo_empresa_omie,
            codigo_conta_corrente_omie,
            codigo_categoria_pedido,
            origem_pedido,
            quantidade_itens,
            payload_json
        )
        values (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s
        )
        on conflict (codigo_pedido_omie)
        do update set
            numero_pedido = excluded.numero_pedido,
            codigo_cliente_omie = excluded.codigo_cliente_omie,
            nome_fantasia_cliente_snapshot = excluded.nome_fantasia_cliente_snapshot,
            cidade_cliente_snapshot = excluded.cidade_cliente_snapshot,
            uf_cliente_snapshot = excluded.uf_cliente_snapshot,
            codigo_vendedor_omie = excluded.codigo_vendedor_omie,
            nome_vendedor_snapshot = excluded.nome_vendedor_snapshot,
            codigo_tabela_preco = excluded.codigo_tabela_preco,
            ncod_tabela_preco = excluded.ncod_tabela_preco,
            nome_tabela_preco_snapshot = excluded.nome_tabela_preco_snapshot,
            codigo_condicao_pagamento = excluded.codigo_condicao_pagamento,
            descricao_condicao_pagamento_snapshot = excluded.descricao_condicao_pagamento_snapshot,
            numero_parcelas = excluded.numero_parcelas,
            data_emissao = excluded.data_emissao,
            data_previsao_entrega = excluded.data_previsao_entrega,
            valor_produtos = excluded.valor_produtos,
            valor_desconto = excluded.valor_desconto,
            valor_total = excluded.valor_total,
            status_pedido = excluded.status_pedido,
            etapa = excluded.etapa,
            pedido_bloqueado = excluded.pedido_bloqueado,
            status_integracao = excluded.status_integracao,
            codigo_pedido_integracao = excluded.codigo_pedido_integracao,
            codigo_cenario_impostos = excluded.codigo_cenario_impostos,
            cenario_fiscal_snapshot = excluded.cenario_fiscal_snapshot,
            codigo_empresa_omie = excluded.codigo_empresa_omie,
            codigo_conta_corrente_omie = excluded.codigo_conta_corrente_omie,
            codigo_categoria_pedido = excluded.codigo_categoria_pedido,
            origem_pedido = excluded.origem_pedido,
            quantidade_itens = excluded.quantidade_itens,
            payload_json = excluded.payload_json,
            updated_at = now()
    """, (
        codigo_pedido,
        numero_pedido,
        codigo_cliente,
        nome_fantasia_cliente_snapshot,
        cidade_cliente_snapshot,
        uf_cliente_snapshot,
        codigo_vendedor,
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
        status_integracao,
        codigo_pedido_integracao,
        codigo_cenario_impostos,
        cenario_fiscal_snapshot,
        codigo_empresa_omie,
        codigo_conta_corrente_omie,
        codigo_categoria_pedido,
        origem_pedido,
        quantidade_itens,
        Json(pedido)
    ))

    for i, item in enumerate(pedido.get("det", []) or [], start=1):
        produto = item.get("produto", {}) or {}
        ide = item.get("ide", {}) or {}
        inf_adic = item.get("inf_adic", {}) or {}

        codigo_produto = produto.get("codigo_produto")
        if not codigo_produto:
            continue

        descricao_produto = produto.get("descricao")
        unidade = produto.get("unidade")
        quantidade = produto.get("quantidade", 0) or 0
        valor_unitario = produto.get("valor_unitario", 0) or 0
        percentual_desconto = produto.get("percentual_desconto", 0) or 0
        valor_total_item = produto.get("valor_total", 0) or 0

        codigo_item_omie = ide.get("codigo_item")
        sequencia = codigo_item_omie or i
        cfop = produto.get("cfop")
        codigo_local_estoque_omie = inf_adic.get("codigo_local_estoque")
        local_estoque_snapshot = None

        codigo_tabela_preco_item = produto.get("codigo_tabela_preco")
        tabela_preco_snapshot = None

        if isinstance(codigo_tabela_preco_item, int) and codigo_tabela_preco_item > 999999:
            tabela_item = buscar_tabela_preco_por_ncod(cur, codigo_tabela_preco_item)
            if tabela_item:
                tabela_preco_snapshot = tabela_item["nome_tabela"]
        else:
            tabela_item = buscar_tabela_preco_por_codigo(cur, codigo_tabela_preco_item)
            if tabela_item:
                tabela_preco_snapshot = tabela_item["nome_tabela"]

        valor_desconto_item = produto.get("valor_desconto", 0) or 0
        valor_mercadoria = produto.get("valor_mercadoria", 0) or 0
        codigo_categoria_item = inf_adic.get("codigo_categoria_item")
        codigo_cenario_impostos_item = inf_adic.get("codigo_cenario_impostos_item")

        cur.execute("""
            insert into omie_core.pedido_itens (
                codigo_pedido_omie,
                codigo_produto_omie,
                descricao_produto_snapshot,
                unidade,
                sequencia,
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
                codigo_cenario_impostos_item,
                payload_json
            )
            values (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            on conflict (codigo_pedido_omie, codigo_produto_omie, sequencia)
            do update set
                descricao_produto_snapshot = excluded.descricao_produto_snapshot,
                unidade = excluded.unidade,
                quantidade = excluded.quantidade,
                valor_unitario = excluded.valor_unitario,
                percentual_desconto = excluded.percentual_desconto,
                valor_total_item = excluded.valor_total_item,
                codigo_item_omie = excluded.codigo_item_omie,
                cfop = excluded.cfop,
                codigo_local_estoque_omie = excluded.codigo_local_estoque_omie,
                local_estoque_snapshot = excluded.local_estoque_snapshot,
                codigo_tabela_preco_item = excluded.codigo_tabela_preco_item,
                tabela_preco_snapshot = excluded.tabela_preco_snapshot,
                valor_desconto_item = excluded.valor_desconto_item,
                valor_mercadoria = excluded.valor_mercadoria,
                codigo_categoria_item = excluded.codigo_categoria_item,
                codigo_cenario_impostos_item = excluded.codigo_cenario_impostos_item,
                payload_json = excluded.payload_json,
                updated_at = now()
        """, (
            codigo_pedido,
            codigo_produto,
            descricao_produto,
            unidade,
            sequencia,
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
            codigo_cenario_impostos_item,
            Json(item)
        ))

    return True


def sync_pedidos():
    inicio_sync = datetime.now()
    data_hora_minima = inicio_sync - timedelta(hours=36)

    print("🔄 Sincronizando pedidos das últimas 36 horas...")

    conn = get_connection()
    total_paginas = 0
    total_paginas_lidas = 0
    total_lidos = 0
    total_na_janela = 0
    total_salvos = 0

    try:
        primeira_pagina = listar_pedidos(pagina=1, registros_por_pagina=100)
        total_paginas = primeira_pagina.get("total_de_paginas", 1)

        paginas_antigas_consecutivas = 0

        for pagina in range(total_paginas, 0, -1):
            data = listar_pedidos(pagina=pagina, registros_por_pagina=100)
            total_paginas_lidas += 1

            pedidos = data.get("pedido_venda_produto", []) or []
            if not pedidos:
                continue

            pagina_teve_pedido_na_janela = False

            for pedido in pedidos:
                total_lidos += 1

                if pedido_esta_na_janela(pedido, data_hora_minima=data_hora_minima):
                    pagina_teve_pedido_na_janela = True
                    total_na_janela += 1

                    if salvar_pedido(conn, pedido):
                        total_salvos += 1

            if pagina_teve_pedido_na_janela:
                paginas_antigas_consecutivas = 0
            else:
                if all(pedido_eh_antigo(pedido, data_hora_minima=data_hora_minima) for pedido in pedidos):
                    paginas_antigas_consecutivas += 1
                else:
                    paginas_antigas_consecutivas = 0

            if paginas_antigas_consecutivas >= 2:
                break

        conn.commit()
        print(f"⏱️ Início da sincronização: {inicio_sync.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"🪟 Janela de referência: {data_hora_minima.strftime('%Y-%m-%d %H:%M:%S')} até {inicio_sync.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"📄 Total de páginas no Omie: {total_paginas}")
        print(f"📖 Páginas efetivamente lidas: {total_paginas_lidas}")
        print(f"📦 Pedidos lidos: {total_lidos}")
        print(f"🎯 Pedidos na janela encontrados: {total_na_janela}")
        print(f"✅ Pedidos sincronizados: {total_salvos}")

    finally:
        conn.close()


if __name__ == "__main__":
    sync_pedidos()