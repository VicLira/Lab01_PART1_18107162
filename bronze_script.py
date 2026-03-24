
import os

from dotenv import load_dotenv
from utils.data_extract import extract
from utils.data_load import load


def handler():
    # ----------------------------------------
    # Handler que:
    # 1. Extract Kaggle CSV
    # 2. Load into BRONZE_DATA_PATH
    # 3. Report data_quality
    # ----------------------------------------
    DATASET_URL=os.getenv("DATASET_URL")
    
    print("INICIANDO EXTRAÇÃO: ")
    urls = extract(DATASET_URL)
    print("\nINCIANDO LOAD CSVs: ")
    files = load(urls)
    print(files)
    
if __name__ == '__main__':
    load_dotenv(override=True)
    handler()