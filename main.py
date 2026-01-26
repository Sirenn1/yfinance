from bronze.ingestion import run_bronze
from silver.silver import run_silver

if __name__ == "__main__":
    run_bronze()
    print("Running bronze")
    run_silver()
    print("Running silver")
