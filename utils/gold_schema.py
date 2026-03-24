import os
import psycopg2
from dotenv import load_dotenv


def get_pg_conn():
    print("\t- CONECTANDO AO POSTGRES")
    return psycopg2.connect(
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT"),
        database=os.getenv("PG_DATABASE"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
    )


def create_schema(conn):
    print("\t- CRIANDO STAR SCHEMA (GOLD)")

    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS dim_data (
        data_id SERIAL PRIMARY KEY,
        data DATE UNIQUE,
        ano INT,
        mes INT,
        trimestre INT
    );

    CREATE TABLE IF NOT EXISTS dim_produto (
        produto_id SERIAL PRIMARY KEY,
        produto TEXT,
        unidade_medida TEXT,
        UNIQUE(produto, unidade_medida)
    );

    CREATE TABLE IF NOT EXISTS dim_localidade (
        localidade_id SERIAL PRIMARY KEY,
        regiao_sigla TEXT,
        estado_sigla TEXT,
        municipio TEXT,
        UNIQUE(regiao_sigla, estado_sigla, municipio)
    );

    -- ==========================================================
    -- 🔹 DIM_POSTO (SCD TYPE 2)
    -- ==========================================================
    CREATE TABLE IF NOT EXISTS dim_posto (
        posto_id SERIAL PRIMARY KEY,
        cnpj_revenda TEXT,
        revenda TEXT,
        bandeira TEXT,
        nome_rua TEXT,
        numero_rua TEXT,
        complemento TEXT,
        bairro TEXT,
        cep TEXT,
        data_inicio DATE,
        data_fim DATE,
        ativo BOOLEAN DEFAULT TRUE
    );

    CREATE INDEX IF NOT EXISTS idx_dim_posto_cnpj
    ON dim_posto(cnpj_revenda);

    CREATE TABLE IF NOT EXISTS fact_precos_combustivel (
        fato_id BIGSERIAL PRIMARY KEY,
        data_id INT REFERENCES dim_data(data_id),
        produto_id INT REFERENCES dim_produto(produto_id),
        localidade_id INT REFERENCES dim_localidade(localidade_id),
        posto_id INT REFERENCES dim_posto(posto_id),
        valor_venda DOUBLE PRECISION,
        valor_compra DOUBLE PRECISION
    );
    """)

    conn.commit()
    cursor.close()


def main():
    load_dotenv()
    conn = get_pg_conn()
    create_schema(conn)
    conn.close()
    print("SCHEMA GOLD CRIADO COM SUCESSO\n")


if __name__ == "__main__":
    main()