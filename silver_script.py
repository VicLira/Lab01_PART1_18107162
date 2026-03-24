import time

from dotenv import load_dotenv
from utils.data_silver_transform import transform


def handler():
    # ----------------------------------------
    # Handler que:
    # 1. 
    # ----------------------------------------
    
    print("INICIANDO TRANSFORMAÇÃO BRONZE -> SILVER: ")
    start = time.perf_counter()
    
    transform()
    
    end = time.perf_counter()
    elapsed = end - start

    print(f"\n⏱ Tempo total de execução: {elapsed:.2f} segundos")
    
if __name__ == '__main__':
    load_dotenv(override=True)
    handler()