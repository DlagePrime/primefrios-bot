from fastapi import FastAPI
from app.api.pedidos import router as pedidos_router

app = FastAPI(
    title="Omie AI Assistant API",
    version="1.0.0"
)

app.include_router(pedidos_router)


@app.get("/")
def root():
    return {"status": "ok", "message": "API rodando"}