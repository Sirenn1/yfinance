from bronze.ingestion import run_bronze
from silver.silver import run_silver
from gold.gold import run_gold

if __name__ == "__main__":
    print("Running bronze")
    run_bronze()
    print("Running silver")
    run_silver()
    print("Running gold")
    run_gold()
