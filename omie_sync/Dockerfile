FROM python:3.12-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# dependências do sistema
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# copiar requirements primeiro (cache)
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# copiar projeto
COPY . .

# expor porta da API
EXPOSE 8001

# iniciar API
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8001"]