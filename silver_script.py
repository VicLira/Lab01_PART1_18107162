import time

from dotenv import load_dotenv
from utils.data_silver_transform import transform


def handler():
    # ----------------------------------------
    # Handler que:
    # 1. Lê dados da camada BRONZE
    # 2. Aplica transformações e limpeza
    # 3. Carrega dados na camada SILVER
    # 4. Reporta data_quality (ex: nulos, duplicidade, quantidade registros)
    # 5. Cria gráficos exploratórios
    # 6. Reporta tempo de execução
    # ----------------------------------------

    print("INICIANDO TRANSFORMAÇÃO BRONZE -> SILVER: ")
    start = time.perf_counter()

    # Executa transformações
    transform()

    end = time.perf_counter()
    elapsed = end - start

    print(f"\n⏱ Tempo total de execução: {elapsed:.2f} segundos")
    
if __name__ == '__main__':
    load_dotenv(override=True)
    handler()