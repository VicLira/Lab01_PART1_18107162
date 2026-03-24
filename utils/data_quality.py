from dotenv import load_dotenv
import duckdb
import os 

load_dotenv()
BRONZE_DATA_PATH = os.getenv('BRONZE_DATA_PATH')

conn = duckdb.connect()

def verify():
    entities = [
            'aisles', 
            'departments', 
            'order_products__prior', 
            'order_products__train',
            'orders',
            'products'
    ]
    
    for entity in entities:
        execute_analysis(entity)
    
def execute_analysis(entity: str):
    path = f"{BRONZE_DATA_PATH}{entity}.csv"

    # Descobrir colunas dinamicamente
    columns_info = conn.execute(f"""
        DESCRIBE SELECT * FROM '{path}'
    """).fetchall()
    
    columns = [col[0] for col in columns_info]

    # Gerar query de nulos dinamicamente
    null_query = ",\n".join([
        f"SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) AS {col}_nulls"
        for col in columns
    ])

    # Nulls
    nulls = conn.execute(f"""
        SELECT 
            {null_query}
        FROM '{path}'
    """).fetchall()

    # Duplicatas (todas as colunas)
    col_list = ", ".join(columns)

    duplicates = conn.execute(f"""
        SELECT COUNT(*) - COUNT(DISTINCT ({col_list})) AS duplicate_rows 
        FROM '{path}'
    """).fetchall()

    # Tipos (DESCRIBE)
    dtypes_raw = conn.execute(f"""DESCRIBE SELECT * FROM '{path}'""").fetchall()
    dtypes = [(col[0], col[1]) for col in dtypes_raw]

    print(f"""
    {entity}:
        Nulos: {nulls}
        Duplicatas: {duplicates}
        Tipos de dados: {dtypes}
    """)



if __name__ == '__main__':
    verify()