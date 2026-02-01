from src.ingest.bronze.download_csv import run_bronze_ingestion
from src.transform.silver.clean_data import run_silver



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

    print("\n*******************************")
    print("Pipeline finalizado com sucesso")
    print("*******************************")

if __name__ == "__main__":
    main()
