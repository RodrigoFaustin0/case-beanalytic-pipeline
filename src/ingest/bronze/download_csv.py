from playwright.sync_api import sync_playwright
import os

BASE_URL = "https://dados.pbh.gov.br/group/mobilidade-urbana"
OUTPUT_DIR = "data/bronze/mobilidade_bh"

os.makedirs(OUTPUT_DIR, exist_ok=True)

def download_resource(page, target_locator, nome_arquivo):
    """
    Recebe um locator (li), abre o menu e faz o download.
    """
    # abre o dropdown 'EXPLORAR' do item 
    target_locator.locator("a.dropdown-toggle").click()

    # clica em 'Baixar' e aguarda o evento de download
    with page.expect_download() as download_info:
        target_locator.locator("a:has-text('Baixar')").click()

    download = download_info.value
    caminho = os.path.join(OUTPUT_DIR, nome_arquivo)
    download.save_as(caminho)
    print(f"sucesso: {nome_arquivo} salvo em {OUTPUT_DIR}")

def run_bronze_ingestion():
    """Roda o processo de ingestão dos datasets da mobilidade urbana de BH."""
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()

        # define um timeout maior 
        page.set_default_timeout(2*60*1000)
        
        # Acessa a página principal
        print(f"Acessando {BASE_URL}...")
        page.goto(BASE_URL)

        # ===================================
        # DATASET 1 — MCO (Último da lista)
        # ==================================
        print("Acessando MCO...")
        page.get_by_role("link",name="Mapa de Controle Operacional (MCO) Consolidado",exact=True).first.click()
        
        # Pega o último li.resource-item da página (o mais recente)
        target_mco = page.locator("li.resource-item").last
        download_resource(page, target_mco, "mco_consolidado.csv")

        # ============================================
        # DATASET 2 — Tempo Real (Filtrado por texto)
        # ============================================
        print("Voltando para a página inicial...")
        page.goto(BASE_URL)
        
        print("Entrando em Tempo Real Ônibus - Coordenada atualizada...")
        page.get_by_role("link",name="Tempo Real Ônibus - Coordenada atualizada",exact=True).first.click()
        
        # Pega o li.resource-item que contém o texto exato 'ARQUIVO CSV'
        target_tempo_real = page.locator("li.resource-item").filter(has_text="ARQUIVO CSV")
        download_resource(page, target_tempo_real, "onibus_tempo_real.csv")

        browser.close()

