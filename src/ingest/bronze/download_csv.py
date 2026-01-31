from playwright.sync_api import sync_playwright
import os

BASE_URL = "https://dados.pbh.gov.br/group/mobilidade-urbana"
OUTPUT_DIR = "data/bronze"

os.makedirs(OUTPUT_DIR, exist_ok=True)

def baixar_ultimo_csv(page, nome_arquivo):
    """
    Abre o último recurso da lista e baixa o CSV
    """
    ultimo_li = page.locator("li.resource-item").last

    # abre dropdown
    ultimo_li.locator("a.dropdown-toggle").click()

    with page.expect_download() as download_info:
        ultimo_li.locator("a:has-text('Baixar')").click()

    download = download_info.value
    caminho = f"{OUTPUT_DIR}/{nome_arquivo}"
    download.save_as(caminho)

    print(f"Arquivo salvo em: {caminho}")


with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    context = browser.new_context(accept_downloads=True)
    page = context.new_page()

    # =========================
    # DATASET 1 — MCO
    # =========================
    print("Acessando Mobilidade Urbana...")
    page.goto(BASE_URL)

    print("Entrando no MCO...")
    page.get_by_role(
        "link",
        name="Mapa de Controle Operacional (MCO) Consolidado",
        exact=True
    ).first.click()

    print("Baixando CSV do MCO...")
    baixar_ultimo_csv(page, "mco_consolidado.csv")

    # =========================
    # DATASET 2 — Tempo Real Ônibus
    # =========================
    print("Voltando para a página inicial...")
    page.goto(BASE_URL)

    print("Entrando em Tempo Real Ônibus - Coordenada atualizada...")
    page.get_by_role(
        "link",
        name="Tempo Real Ônibus - Coordenada atualizada",
        exact=True
    ).first.click()

    print("Baixando CSV de Tempo Real Ônibus...")
    baixar_ultimo_csv(page, "onibus_tempo_real.csv")

    browser.close()
