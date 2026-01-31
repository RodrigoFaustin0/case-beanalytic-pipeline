FROM python:3.11-slim

# Diretório de trabalho dentro do container
WORKDIR /app

# Copia dependências
COPY requirements.txt .

# Instala dependências Python + navegador do Playwright
RUN pip install --no-cache-dir -r requirements.txt \
    && playwright install chromium

# Copia o restante do projeto
COPY . .

# Comando padrão do container
CMD ["python", "run_pipeline.py"]
