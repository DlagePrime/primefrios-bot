from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import List, Optional, Union

from app.services.compor_pedido import compor_pedido
from app.services.enviar_pedidos_omie import main as enviar_pedidos_omie


router = APIRouter(prefix="/pedido", tags=["Pedidos"])


class ItemExtraido(BaseModel):
    produto_texto: str
    quantidade: float
    unidade: str = "UN"


class PedidoExtraido(BaseModel):
    confirmado: bool = False
    tipo_pedido: str = "venda"
    usar_prazo_padrao: bool = True
    usar_tabela_padrao: bool = True
    operacao_destino: str = "AUTO"
    codigo_cliente_omie: Optional[int] = None
    itens: List[ItemExtraido] = Field(default_factory=list)
    observacoes: str = ""


class ItemPayloadOmie(BaseModel):
    codigo_produto_omie: int
    unidade: str = "UN"
    quantidade: float
    valor_unitario: float
    percentual_desconto: float = 0
    valor_total_item: float
    cfop: str
    codigo_local_estoque_omie: Optional[int] = None
    codigo_tabela_preco_item: Optional[int] = None
    valor_desconto_item: float = 0
    valor_mercadoria: float
    codigo_categoria_item: Optional[str] = None
    codigo_cenario_impostos_item: Optional[int] = None


class PedidoPayloadOmie(BaseModel):
    codigo_cliente_omie: int
    codigo_vendedor_omie: Optional[int] = None
    codigo_tabela_preco: Optional[str] = None
    ncod_tabela_preco: Optional[int] = None
    codigo_condicao_pagamento: Optional[str] = None
    numero_parcelas: Optional[int] = None
    data_emissao: Optional[str] = None
    data_previsao_entrega: Optional[str] = None
    valor_produtos: float
    valor_desconto: float = 0
    valor_total: float
    etapa: Optional[str] = None
    pedido_bloqueado: bool = False
    codigo_pedido_integracao: Optional[str] = ""
    codigo_cenario_impostos: Optional[int] = None
    codigo_empresa_omie: Optional[int] = None
    codigo_conta_corrente_omie: Optional[int] = None
    codigo_categoria_pedido: Optional[str] = None
    origem_pedido: Optional[str] = None
    quantidade_itens: Optional[int] = None


class PayloadEnvioOmie(BaseModel):
    pedido: PedidoPayloadOmie
    itens: List[ItemPayloadOmie] = Field(default_factory=list)


class ComporPedidoRequest(BaseModel):
    instancia: Optional[str] = None
    remotejid: Optional[str] = None
    pushname: Optional[str] = None
    mensagem_original: Optional[str] = None
    pedido_extraido: Optional[PedidoExtraido] = None
    payload_envio_omie: Optional[PayloadEnvioOmie] = None


@router.post("/compor")
def compor_pedido_endpoint(payload: ComporPedidoRequest):
    try:
        dados = payload.model_dump()

        if not dados.get("pedido_extraido") and not dados.get("payload_envio_omie"):
            raise HTTPException(
                status_code=400,
                detail="Envie 'pedido_extraido' ou 'payload_envio_omie'."
            )

        resultado = compor_pedido(dados)
        return resultado

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
        
@router.post("/enviar")
def enviar_pedido_endpoint():
    try:
        enviar_pedidos_omie()
        return {"ok": True, "mensagem": "Envio de pedidos executado com sucesso."}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
