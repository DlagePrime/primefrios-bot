import os
from datetime import datetime

import requests
from dotenv import load_dotenv

from app.db.connection import get_connection

load_dotenv()

OMIE_APP_KEY = os.getenv("OMIE_APP_KEY")
OMIE_APP_SECRET = os.getenv("OMIE_APP_SECRET")

OMIE_URL = "https://app.omie.com.br/api/v1/geral/clientes/"


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


def fetch_clientes_completo() -> list:
    pagina = 1
    registros = []

    while True:
        data = omie_post(
            "ListarClientes",
            [{
                "pagina": pagina,
                "registros_por_pagina": 100,
                "apenas_importado_api": "N"
            }]
        )

        lista = data.get("clientes_cadastro", [])
        registros.extend(lista)

        total_paginas = data.get("total_de_paginas", pagina)
        if pagina >= total_paginas:
            break

        pagina += 1

    return registros


def to_int(value):
    if value in (None, "", "null"):
        return None
    try:
        return int(str(value).strip())
    except Exception:
        return None


def to_bool_omie_sn(value):
    """
    Omie costuma mandar S/N.
    Retorna:
    - True para S
    - False para N
    - None se vazio
    """
    if value is None:
        return None

    value = str(value).strip().upper()

    if value == "S":
        return True
    if value == "N":
        return False

    return None


def limpar_texto(value):
    if value is None:
        return None

    value = str(value).strip()

    if not value:
        return None

    return value.rstrip(",").strip() or None


def limpar_cep(value):
    value = limpar_texto(value)
    if not value:
        return None

    digits = "".join(ch for ch in value if ch.isdigit())
    return digits or None


def montar_endereco_completo(
    rua: str | None,
    numero: str | None,
    complemento: str | None,
    bairro: str | None,
    cidade: str | None,
    uf: str | None,
    cep: str | None,
) -> str | None:
    linha1_partes = [p for p in [rua, numero] if p]
    linha2_partes = [p for p in [complemento, bairro] if p]

    cidade_uf = None
    if cidade and uf:
        cidade_uf = f"{cidade} - {uf}"
    elif cidade:
        cidade_uf = cidade
    elif uf:
        cidade_uf = uf

    linha3_partes = [p for p in [cidade_uf, cep] if p]

    partes = []
    if linha1_partes:
        partes.append(", ".join(linha1_partes))
    if linha2_partes:
        partes.append(" - ".join(linha2_partes))
    if linha3_partes:
        partes.append(" | ".join(linha3_partes))

    if not partes:
        return None

    return " | ".join(partes)


def buscar_dados_condicao(cur, codigo_condicao: str):
    """
    Busca a condição na tabela sincronizada de condicoes_pagamento
    usando o código Omie.
    """
    if not codigo_condicao:
        return None, None

    cur.execute(
        """
        select
            descricao,
            numero_parcelas_padrao
        from omie_core.condicoes_pagamento
        where codigo_parcela_omie = %s
        limit 1
        """,
        (codigo_condicao,),
    )

    row = cur.fetchone()
    if not row:
        return None, None

    descricao, numero_parcelas = row
    return descricao, numero_parcelas


def buscar_nome_vendedor(cur, codigo_vendedor):
    """
    Busca o nome do vendedor na tabela sincronizada de vendedores
    usando o código Omie.
    """
    if not codigo_vendedor:
        return None

    cur.execute(
        """
        select nome_vendedor
        from omie_core.vendedores
        where codigo_vendedor_omie = %s
        limit 1
        """,
        (codigo_vendedor,),
    )

    row = cur.fetchone()
    if not row:
        return None

    return row[0]


def determinar_codigo_tabela_preco_por_uf(uf: str | None) -> str | None:
    """
    Regra de tabela padrão por estado:
    - MG -> 5030
    - PI/MA -> 5020
    """
    uf = limpar_texto(uf)
    if not uf:
        return None

    uf = uf.upper()

    if uf == "MG":
        return "5030"

    if uf in ("PI", "MA"):
        return "5020"

    return None


def buscar_dados_tabela_preco(cur, codigo_tabela_preco: str):
    """
    Busca ncod e nome da tabela de preço a partir do código da tabela.
    """
    if not codigo_tabela_preco:
        return None, None

    cur.execute(
        """
        select
            ncod_tab_preco,
            nome_tabela
        from omie_core.tabelas_preco
        where codigo_tabela_preco = %s
        order by ativa desc, updated_at desc nulls last
        limit 1
        """,
        (codigo_tabela_preco,),
    )

    row = cur.fetchone()
    if not row:
        return None, None

    ncod_tab_preco, nome_tabela = row
    return ncod_tab_preco, nome_tabela


def extrair_recomendacoes(cliente: dict) -> dict:
    """
    Lê os dados do local correto da resposta da Omie
    e passa a preencher também os campos estruturados de endereço.
    """
    recomendacoes = cliente.get("recomendacoes") or {}

    codigo_cliente = cliente.get("codigo_cliente_omie") or cliente.get("codigo_cliente")
    codigo_vendedor = recomendacoes.get("codigo_vendedor")
    codigo_condicao = recomendacoes.get("numero_parcelas")
    codigo_transportadora = recomendacoes.get("codigo_transportadora")
    email_fatura = recomendacoes.get("email_fatura")
    gerar_boletos = recomendacoes.get("gerar_boletos")

    endereco_rua = limpar_texto(cliente.get("endereco"))
    endereco_numero = limpar_texto(cliente.get("endereco_numero"))
    endereco_complemento = limpar_texto(cliente.get("complemento"))
    endereco_bairro = limpar_texto(cliente.get("bairro"))
    endereco_cidade = limpar_texto(cliente.get("cidade"))
    endereco_uf = limpar_texto(cliente.get("estado"))
    endereco_cep = limpar_cep(cliente.get("cep"))

    endereco_completo = montar_endereco_completo(
        rua=endereco_rua,
        numero=endereco_numero,
        complemento=endereco_complemento,
        bairro=endereco_bairro,
        cidade=endereco_cidade,
        uf=endereco_uf,
        cep=endereco_cep,
    )

    email_fatura_limpo = limpar_texto(email_fatura)
    if not email_fatura_limpo:
        email_fatura_limpo = limpar_texto(cliente.get("email"))

    return {
        "codigo_cliente_omie": to_int(codigo_cliente),
        "codigo_vendedor_padrao_omie": to_int(codigo_vendedor),
        "codigo_condicao_pagamento_padrao_omie": limpar_texto(codigo_condicao),
        "codigo_transportadora_padrao_omie": to_int(codigo_transportadora),
        "email_fatura": email_fatura_limpo,
        "gerar_boletos": to_bool_omie_sn(gerar_boletos),
        "endereco_rua": endereco_rua,
        "endereco_numero": endereco_numero,
        "endereco_complemento": endereco_complemento,
        "endereco_bairro": endereco_bairro,
        "endereco_cidade": endereco_cidade,
        "endereco_uf": endereco_uf,
        "endereco_cep": endereco_cep,
        "endereco_completo": endereco_completo,
    }


def atualizar_recomendacoes(conn, cliente: dict):
    dados = extrair_recomendacoes(cliente)
    codigo_cliente_omie = dados["codigo_cliente_omie"]

    if not codigo_cliente_omie:
        return False

    with conn.cursor() as cur:
        descricao_prazo = None
        numero_parcelas_padrao = None
        nome_vendedor = None

        codigo_condicao = dados["codigo_condicao_pagamento_padrao_omie"]
        if codigo_condicao:
            descricao_prazo, numero_parcelas_padrao = buscar_dados_condicao(cur, codigo_condicao)

        codigo_vendedor = dados["codigo_vendedor_padrao_omie"]
        if codigo_vendedor:
            nome_vendedor = buscar_nome_vendedor(cur, codigo_vendedor)

        codigo_tabela_preco_padrao = determinar_codigo_tabela_preco_por_uf(dados["endereco_uf"])
        ncod_tabela_preco_padrao = None
        nome_tabela_preco_padrao_snapshot = None

        if codigo_tabela_preco_padrao:
            (
                ncod_tabela_preco_padrao,
                nome_tabela_preco_padrao_snapshot,
            ) = buscar_dados_tabela_preco(cur, codigo_tabela_preco_padrao)

        cur.execute(
            """
            update omie_core.clientes
            set
                codigo_vendedor_padrao_omie = %s,
                nome_vendedor_padrao_snapshot = %s,
                codigo_condicao_pagamento_padrao_omie = %s,
                prazo_pagamento_padrao_descricao = %s,
                numero_parcelas_padrao = %s,
                codigo_transportadora_padrao_omie = %s,
                email_fatura = %s,
                gerar_boletos = %s,
                endereco_rua = %s,
                endereco_numero = %s,
                endereco_complemento = %s,
                endereco_bairro = %s,
                endereco_cidade = %s,
                endereco_uf = %s,
                endereco_cep = %s,
                endereco_completo = %s,
                codigo_tabela_preco_padrao = %s,
                ncod_tabela_preco_padrao = %s,
                nome_tabela_preco_padrao_snapshot = %s,
                updated_at = now()
            where codigo_cliente_omie = %s
            """,
            (
                dados["codigo_vendedor_padrao_omie"],
                nome_vendedor,
                dados["codigo_condicao_pagamento_padrao_omie"],
                descricao_prazo,
                numero_parcelas_padrao,
                dados["codigo_transportadora_padrao_omie"],
                dados["email_fatura"],
                dados["gerar_boletos"],
                dados["endereco_rua"],
                dados["endereco_numero"],
                dados["endereco_complemento"],
                dados["endereco_bairro"],
                dados["endereco_cidade"],
                dados["endereco_uf"],
                dados["endereco_cep"],
                dados["endereco_completo"],
                codigo_tabela_preco_padrao,
                ncod_tabela_preco_padrao,
                nome_tabela_preco_padrao_snapshot,
                codigo_cliente_omie,
            ),
        )

        return cur.rowcount > 0


def run():
    inicio = datetime.now()
    conn = get_connection()

    try:
        clientes = fetch_clientes_completo()

        total = 0
        atualizados = 0

        for cliente in clientes:
            total += 1
            if atualizar_recomendacoes(conn, cliente):
                atualizados += 1

        conn.commit()

        print(f"Clientes lidos da Omie: {total}")
        print(f"Clientes atualizados: {atualizados}")
        print(f"Início: {inicio} | Fim: {datetime.now()}")

    except Exception as e:
        conn.rollback()
        print(f"Erro na sincronização: {e}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    run()