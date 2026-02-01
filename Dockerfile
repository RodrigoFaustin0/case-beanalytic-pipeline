# base Python Slim (Leve e estável)
FROM python:3.11-slim-bookworm

# 2. Atualiza o sistema
RUN apt-get update && apt-get install -y --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 3. Instala dependências do Python primeiro
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 4. A SOLUÇÃO: Instala as bibliotecas de sistema E o Chromium
# O '--with-deps' baixa exatamente o que o seu erro listou
RUN playwright install --with-deps chromium

# 5. Copia o restante do seu código
COPY . .

CMD ["python", "run_pipeline.py"]