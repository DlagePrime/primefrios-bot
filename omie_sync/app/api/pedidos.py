from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import Any, Dict, List, Optional

from app.services.compor_pedido import compor_pedido


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


class ComporPedidoRequest(BaseModel):
    instancia: Optional[str] = None
    remotejid: Optional[str] = None
    pushname: Optional[str] = None
    mensagem_original: Optional[str] = None
    pedido_extraido: PedidoExtraido


@router.post("/compor")
def compor_pedido_endpoint(payload: ComporPedidoRequest):
    try:
        resultado = compor_pedido(payload.model_dump())
        return resultado
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))