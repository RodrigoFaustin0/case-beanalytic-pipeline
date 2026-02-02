# Base Python Slim 
FROM python:3.11-slim-bookworm

# Atualiza o sistema
RUN apt-get update && apt-get install -y --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Instala dependências do Python primeiro
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Instala as bibliotecas de sistema E o Chromium
RUN playwright install --with-deps chromium

# Instala o pytest para testes
RUN pip install pytest

# Copia o restante dos arquivos
COPY . .

CMD ["python", "run_pipeline.py"]