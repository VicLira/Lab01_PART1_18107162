import time
from dotenv import load_dotenv

from utils.gold_schema import create_schema, get_pg_conn
from utils.gold_load import load_gold
from utils.gold_metrics import run_all_metrics


def handler():
    # ----------------------------------------
    # Handler que:
    # 1. Cria Star Schema
    # 2. Carrega dados Silver → Gold
    # 3. Executa métricas de negócio
    # ----------------------------------------

    print("INICIANDO PROCESSO GOLD (DW)")
    start = time.perf_counter()

    # Criar Schema
    conn = get_pg_conn()
    create_schema(conn)
    conn.close()

    # Carga
    load_gold()

    # Métricas
    run_all_metrics()

    end = time.perf_counter()
    elapsed = end - start

    print(f"\n⏱ Tempo total GOLD: {elapsed:.2f} segundos\n")


if __name__ == "__main__":
    load_dotenv(override=True)
    handler()