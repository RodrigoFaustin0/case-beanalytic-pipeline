FROM apache/airflow:2.10.3

# Instalar a biblioteca Python (como usuário Airflow)
USER airflow
RUN pip install --no-cache-dir playwright

# Instalar dependências do sistema Linux (como Root)
USER root
RUN /home/airflow/.local/bin/playwright install-deps chromium

# Instalar os requisitos do projeto
COPY requirements.txt /requirements.txt
COPY requirements-dev.txt /requirements-dev.txt

# Baixar o navegador Chromium (como usuário Airflow)
USER airflow

RUN pip install --no-cache-dir -r /requirements.txt -r /requirements-dev.txt
RUN playwright install chromium