from src.ingest.bronze.download_csv import run_bronze_ingestion
from src.transform.silver.clean_data import run_silver
from src.transform.gold.build_dimensions import main as run_gold_dimensions
from src.transform.gold.build_facts import main as run_gold_facts



def main():
    print("\n*******************************")
    print("Iniciando a pipeline BeAnalytic")
    print("*******************************")

    print("\n-----------------------")
    print("     CAMADA BRONZE     ")
    print("-----------------------\n")
    run_bronze_ingestion()
    
    print("\n-----------------------")
    print("     CAMADA SILVER     ")
    print("-----------------------\n")
    run_silver()

    print("\n-----------------------")
    print("     CAMADA GOLD     ")
    print("-----------------------\n")
    run_gold_dimensions()
    run_gold_facts()

    print("\n*******************************")
    print("Pipeline finalizado com sucesso")
    print("*******************************")


if __name__ == "__main__":
    main()
