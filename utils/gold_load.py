import os
import psycopg2
from io import StringIO
from dotenv import load_dotenv

from utils.duck_connections import close_conn, get_conn


# ==========================================================
# CONEXÕES
# ==========================================================

def get_pg_conn():
    print("\t- CONECTANDO AO POSTGRES")
    return psycopg2.connect(
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT"),
        database=os.getenv("PG_DATABASE"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
    )


# ==========================================================
# COPY OTIMIZADO
# ==========================================================

def copy_dataframe(conn, df, table):
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    cursor = conn.cursor()
    cursor.copy_expert(
        f"COPY {table} FROM STDIN WITH CSV",
        buffer
    )
    conn.commit()
    cursor.close()


# ==========================================================
# LOAD GOLD
# ==========================================================

def load_gold():
    print("INICIANDO CARGA GOLD")

    silver_path = os.getenv("SILVER_DATA_PATH") + "/combustiveis/**/*.parquet"

    duck, db_path = get_conn()
    pg = get_pg_conn()

    # LIMPA GOLD (FULL LOAD)
    print("\t- LIMPANDO TABELAS GOLD")

    cursor = pg.cursor()
    cursor.execute("""
        TRUNCATE fact_precos_combustivel,
                dim_data,
                dim_produto,
                dim_localidade,
                dim_posto
        RESTART IDENTITY CASCADE;
    """)
    pg.commit()
    cursor.close()
    
    # Lê todos Parquets
    df = duck.execute(f"""
        SELECT *
        FROM read_parquet('{silver_path}')
    """).fetchdf()

    print(f"\t- TOTAL REGISTROS SILVER: {len(df)}")

    # =============================
    # DIM_DATA
    # =============================
    dim_data = df[['data_coleta','ano','mes']].drop_duplicates()
    dim_data['trimestre'] = ((dim_data['mes'] - 1) // 3) + 1

    copy_dataframe(pg, dim_data, "dim_data(data,ano,mes,trimestre)")

    # =============================
    # DIM_PRODUTO
    # =============================
    dim_produto = df[['produto','unidade_medida']].drop_duplicates()
    copy_dataframe(pg, dim_produto, "dim_produto(produto,unidade_medida)")

    # =============================
    # DIM_LOCALIDADE
    # =============================
    dim_localidade = df[['regiao_sigla','estado_sigla','municipio']].drop_duplicates()
    copy_dataframe(pg, dim_localidade, "dim_localidade(regiao_sigla,estado_sigla,municipio)")

    # =============================
    # DIM_POSTO
    # =============================
    dim_posto = df[[
        'revenda','cnpj_revenda','bandeira',
        'nome_rua','numero_rua','complemento','bairro','cep'
    ]].drop_duplicates()

    copy_dataframe(pg, dim_posto,
        "dim_posto(revenda,cnpj_revenda,bandeira,nome_rua,numero_rua,complemento,bairro,cep)"
    )

    print("\t- DIMENSÕES CARREGADAS")

    # =============================
    # FATO
    # =============================
    print("\t- CRIANDO STAGING NO POSTGRES")

    cursor = pg.cursor()
    cursor.execute("""
    DROP TABLE IF EXISTS staging_silver;

    CREATE TABLE staging_silver (
        regiao_sigla TEXT,
        estado_sigla TEXT,
        municipio TEXT,
        revenda TEXT,
        cnpj_revenda TEXT,
        nome_rua TEXT,
        numero_rua TEXT,
        complemento TEXT,
        bairro TEXT,
        cep TEXT,
        produto TEXT,
        data_coleta DATE,
        valor_venda DOUBLE PRECISION,
        valor_compra DOUBLE PRECISION,
        unidade_medida TEXT,
        bandeira TEXT,
        ano TEXT,
        mes TEXT
    );
    """)
    pg.commit()
    cursor.close()

    copy_dataframe(pg, df, """
        staging_silver(
            regiao_sigla,
            estado_sigla,
            municipio,
            revenda,
            cnpj_revenda,
            nome_rua,
            numero_rua,
            complemento,
            bairro,
            cep,
            produto,
            data_coleta,
            valor_venda,
            valor_compra,
            unidade_medida,
            bandeira,
            ano,
            mes
        )
    """)
    
    print("\t- INSERINDO FATO")

    cursor = pg.cursor()
    cursor.execute("""
    INSERT INTO fact_precos_combustivel (
        data_id,
        produto_id,
        localidade_id,
        posto_id,
        valor_venda,
        valor_compra
    )
    SELECT
        d.data_id,
        p.produto_id,
        l.localidade_id,
        po.posto_id,
        s.valor_venda,
        s.valor_compra
    FROM staging_silver s
    JOIN dim_data d 
        ON s.data_coleta = d.data
    JOIN dim_produto p 
        ON s.produto = p.produto
    AND s.unidade_medida = p.unidade_medida
    JOIN dim_localidade l 
        ON s.regiao_sigla = l.regiao_sigla
    AND s.estado_sigla = l.estado_sigla
    AND s.municipio = l.municipio
    JOIN dim_posto po 
        ON s.cnpj_revenda = po.cnpj_revenda;
    """)
    pg.commit()
    cursor.close()
    
    close_conn(duck, db_path)

if __name__ == "__main__":
    load_dotenv()
    load_gold()