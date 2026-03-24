import os
import re
import duckdb
import unicodedata
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
import matplotlib.pyplot as plt

from utils.duck_connections import close_conn, get_conn


# ==========================================================
# UNICODE FIX
# ==========================================================

def _fix_unicode_recursive(s: str, max_iter: int = 5) -> str:
    """
    Corrige múltiplas camadas de encoding incorreto.
    """
    if not isinstance(s, str):
        return s

    try:
        s = unicodedata.normalize("NFKC", s)
    except Exception:
        pass

    suspicious = re.compile(r'[ÃÂ]')

    for _ in range(max_iter):
        if not suspicious.search(s):
            break
        try:
            s = s.encode('latin1').decode('utf-8')
        except (UnicodeEncodeError, UnicodeDecodeError):
            break

    return s


def _fix_unicode_duckdb(s: str) -> str:
    return _fix_unicode_recursive(s)


# ==========================================================
# CONEXÃO E REGISTROS
# ==========================================================


def _register_functions(con: duckdb.DuckDBPyConnection) -> None:
    print("\t- REGISTRANDO FUNÇÕES AUXILIARES")

    con.create_function("FIX_UNICODE", _fix_unicode_duckdb)

    con.execute("""
        CREATE OR REPLACE MACRO SAFE_DATE(s) AS (
            CASE
                WHEN LENGTH(TRIM(s)) = 0 THEN NULL
                ELSE COALESCE(
                    TRY_CAST(TRY_STRPTIME(s, '%d/%m/%Y') AS DATE),
                    TRY_CAST(TRY_STRPTIME(s, '%Y-%m-%d') AS DATE)
                )
            END
        );
    """)


# ==========================================================
# BRONZE RAW
# ==========================================================

def _create_bronze_raw(con: duckdb.DuckDBPyConnection, path: str) -> None:
    print("\t- CRIANDO TABELA bronze_raw")

    con.execute(f"""
        CREATE OR REPLACE TABLE bronze_raw AS
        SELECT 
            NULLIF(SPLIT_PART(FIX_UNICODE(DS_LINHA),';',1), '')  AS regiao_sigla,
            NULLIF(SPLIT_PART(FIX_UNICODE(DS_LINHA),';',2), '')  AS estado_sigla,
            NULLIF(SPLIT_PART(FIX_UNICODE(DS_LINHA),';',3), '')  AS municipio,
            NULLIF(SPLIT_PART(FIX_UNICODE(DS_LINHA),';',4), '')  AS revenda,
            NULLIF(SPLIT_PART(FIX_UNICODE(DS_LINHA),';',5), '')  AS cnpj_revenda,
            NULLIF(SPLIT_PART(FIX_UNICODE(DS_LINHA),';',6), '')  AS nome_rua,
            NULLIF(SPLIT_PART(FIX_UNICODE(DS_LINHA),';',7), '')  AS numero_rua,
            NULLIF(SPLIT_PART(FIX_UNICODE(DS_LINHA),';',8), '')  AS complemento,
            NULLIF(SPLIT_PART(FIX_UNICODE(DS_LINHA),';',9), '')  AS bairro,
            NULLIF(SPLIT_PART(FIX_UNICODE(DS_LINHA),';',10), '') AS cep,
            NULLIF(SPLIT_PART(FIX_UNICODE(DS_LINHA),';',11), '') AS produto,
            NULLIF(SPLIT_PART(FIX_UNICODE(DS_LINHA),';',12), '') AS data_coleta,
            NULLIF(SPLIT_PART(FIX_UNICODE(DS_LINHA),';',13), '') AS valor_venda,
            NULLIF(SPLIT_PART(FIX_UNICODE(DS_LINHA),';',14), '') AS valor_compra,
            NULLIF(SPLIT_PART(FIX_UNICODE(DS_LINHA),';',15), '') AS unidade_medida,
            NULLIF(SPLIT_PART(FIX_UNICODE(DS_LINHA),';',16), '') AS bandeira
        FROM read_csv(
            '{path}',
            delim='^',
            columns={{'DS_LINHA':'VARCHAR'}},
            encoding='ISO8859_1'
        )
    """)
    
def _generate_plots(con: duckdb.DuckDBPyConnection, silver_root: str, report_path: str) -> None:
    print("\t- GERANDO GRÁFICOS")

    grafico_path = os.path.join(silver_root, "graficos")
    Path(grafico_path).mkdir(parents=True, exist_ok=True)

    # ===============================
    # 1️⃣ Histograma preço venda
    # ===============================
    df = con.execute("SELECT valor_venda FROM silver WHERE valor_venda IS NOT NULL").fetchdf()

    plt.figure()
    df["valor_venda"].hist(bins=50)
    plt.title("Distribuição do Preço de Venda")
    plt.xlabel("Preço")
    plt.ylabel("Frequência")
    plt.tight_layout()
    plt.savefig(f"{grafico_path}/01_hist_preco.png")
    plt.close()

    # ===============================
    # 2️⃣ Média por estado
    # ===============================
    df = con.execute("""
        SELECT estado_sigla, AVG(valor_venda) AS media
        FROM silver
        GROUP BY estado_sigla
        ORDER BY media DESC
    """).fetchdf()

    plt.figure()
    plt.bar(df["estado_sigla"], df["media"])
    plt.xticks(rotation=90)
    plt.title("Preço Médio por Estado")
    plt.tight_layout()
    plt.savefig(f"{grafico_path}/02_media_estado.png")
    plt.close()

    # ===============================
    # 3️⃣ Média por produto
    # ===============================
    df = con.execute("""
        SELECT produto, AVG(valor_venda) AS media
        FROM silver
        GROUP BY produto
        ORDER BY media DESC
    """).fetchdf()

    plt.figure()
    plt.bar(df["produto"], df["media"])
    plt.xticks(rotation=45)
    plt.title("Preço Médio por Produto")
    plt.tight_layout()
    plt.savefig(f"{grafico_path}/03_media_produto.png")
    plt.close()

    # ===============================
    # 4️⃣ Evolução média por ano
    # ===============================
    df = con.execute("""
        SELECT ano, AVG(valor_venda) AS media
        FROM silver
        GROUP BY ano
        ORDER BY ano
    """).fetchdf()

    plt.figure()
    plt.plot(df["ano"], df["media"])
    plt.title("Evolução Média de Preço por Ano")
    plt.tight_layout()
    plt.savefig(f"{grafico_path}/04_evolucao_ano.png")
    plt.close()

    # ===============================
    # 5️⃣ Top 10 bandeiras
    # ===============================
    df = con.execute("""
        SELECT bandeira, COUNT(*) AS total
        FROM silver
        GROUP BY bandeira
        ORDER BY total DESC
        LIMIT 10
    """).fetchdf()

    plt.figure()
    plt.bar(df["bandeira"], df["total"])
    plt.xticks(rotation=90)
    plt.title("Top 10 Bandeiras por Quantidade")
    plt.tight_layout()
    plt.savefig(f"{grafico_path}/05_top_bandeiras.png")
    plt.close()

    # ==========================================================
    # ADICIONA AO MARKDOWN EXISTENTE
    # ==========================================================

    with open(report_path, "a", encoding="utf-8") as f:
        f.write("\n# Gráficos Exploratórios\n\n")

        f.write("## 1️⃣ Distribuição do Preço de Venda\n")
        f.write("![Histograma](graficos/01_hist_preco.png)\n\n")

        f.write("## 2️⃣ Preço Médio por Estado\n")
        f.write("![Estado](graficos/02_media_estado.png)\n\n")

        f.write("## 3️⃣ Preço Médio por Produto\n")
        f.write("![Produto](graficos/03_media_produto.png)\n\n")

        f.write("## 4️⃣ Evolução Média por Ano\n")
        f.write("![Ano](graficos/04_evolucao_ano.png)\n\n")

        f.write("## 5️⃣ Top 10 Bandeiras\n")
        f.write("![Bandeiras](graficos/05_top_bandeiras.png)\n\n")

    print("\t- GRÁFICOS GERADOS COM SUCESSO")


# ==========================================================
# RELATÓRIO
# ==========================================================

def _generate_report(con: duckdb.DuckDBPyConnection, output_path: str) -> None:
    print("\t- GERANDO RELATÓRIO MARKDOWN")

    # Garante que o diretório exista
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    # ==========================================================
    # MÉTRICAS BÁSICAS
    # ==========================================================

    total_rows = con.execute(
        "SELECT COUNT(*) FROM bronze_raw"
    ).fetchone()[0]

    distinct_rows = con.execute("""
        SELECT COUNT(*)
        FROM (
            SELECT DISTINCT *
            FROM bronze_raw
        )
    """).fetchone()[0]

    duplicated_rows = total_rows - distinct_rows

    duplicated_percent = (
        (duplicated_rows / total_rows) * 100
        if total_rows > 0 else 0
    )

    schema = con.execute(
        "DESCRIBE bronze_raw"
    ).fetchdf()

    # ==========================================================
    # NULOS
    # ==========================================================

    nulls = con.execute("""
        SELECT
    """ + ",\n".join([
        f"COUNT(*) - COUNT({col}) AS {col}_nulls"
        for col in schema['column_name']
    ]) + """
        FROM bronze_raw
    """).fetchdf()

    # ==========================================================
    # GERA MARKDOWN
    # ==========================================================

    with open(output_path, "w", encoding="utf-8") as f:
        f.write("# Relatório Camada Silver (Pré-Tratamento)\n\n")

        f.write("## Total de Registros\n")
        f.write(f"{total_rows}\n\n")

        f.write("## Tipos de Colunas\n")
        f.write(schema.to_markdown(index=False))
        f.write("\n\n")

        f.write("## Contagem de Nulos\n")
        f.write(nulls.to_markdown(index=False))
        f.write("\n\n")

        f.write("## Duplicidade Geral\n")
        f.write(f"- Total de registros: {total_rows}\n")
        f.write(f"- Registros duplicados: {duplicated_rows}\n")
        f.write(f"- Percentual de duplicidade: {duplicated_percent:.2f}%\n\n")

        f.write("\n")

    print(f"\t- RELATÓRIO SALVO EM: {output_path}")


# ==========================================================
#  LIMPEZA E SILVER
# ==========================================================

def _create_silver(con: duckdb.DuckDBPyConnection) -> None:
    print("\t- APLICANDO LIMPEZA E PADRONIZAÇÃO (SILVER)")

    con.execute("""
        CREATE OR REPLACE TABLE silver AS
        WITH cleaned AS (
            SELECT
                UPPER(TRIM(regiao_sigla)) AS regiao_sigla,
                UPPER(TRIM(estado_sigla)) AS estado_sigla,
                UPPER(TRIM(municipio)) AS municipio,
                UPPER(TRIM(revenda)) AS revenda,
                REGEXP_REPLACE(cnpj_revenda,'\\D','','g') AS cnpj_revenda,
                UPPER(TRIM(nome_rua)) AS nome_rua,
                TRIM(numero_rua) AS numero_rua,
                UPPER(TRIM(complemento)) AS complemento,
                UPPER(TRIM(bairro)) AS bairro,
                REGEXP_REPLACE(cep,'\\D','','g') AS cep,
                UPPER(TRIM(produto)) AS produto,
                SAFE_DATE(data_coleta) AS data_coleta,
                TRY_CAST(REPLACE(REPLACE(valor_venda,'.',''),',','.') AS DOUBLE) AS valor_venda,
                TRY_CAST(REPLACE(REPLACE(valor_compra,'.',''),',','.') AS DOUBLE) AS valor_compra,
                UPPER(TRIM(unidade_medida)) AS unidade_medida,
                UPPER(TRIM(bandeira)) AS bandeira,
                EXTRACT(YEAR FROM SAFE_DATE(data_coleta)) AS ano,
                EXTRACT(MONTH FROM SAFE_DATE(data_coleta)) AS mes
            FROM bronze_raw
        ),
        hashed AS (
            SELECT *,
                HASH(
                    regiao_sigla || '|' ||
                    estado_sigla || '|' ||
                    municipio || '|' ||
                    revenda || '|' ||
                    cnpj_revenda || '|' ||
                    nome_rua || '|' ||
                    numero_rua || '|' ||
                    complemento || '|' ||
                    bairro || '|' ||
                    cep || '|' ||
                    produto || '|' ||
                    data_coleta || '|' ||
                    valor_venda || '|' ||
                    valor_compra || '|' ||
                    unidade_medida || '|' ||
                    bandeira
                ) AS row_hash
            FROM cleaned
        )
        SELECT 
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
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY row_hash) AS rn
            FROM hashed
        )
        WHERE rn = 1;
    """)


# ==========================================================
# PERSISTÊNCIA
# ==========================================================

def _persist_silver(con: duckdb.DuckDBPyConnection, silver_root: str) -> None:
    print("\t- SALVANDO PARQUET (PARTICIONADO POR ANO)")

    con.execute(f"""
        COPY silver
        TO '{silver_root}/combustiveis'
        (FORMAT PARQUET, OVERWRITE_OR_IGNORE, PARTITION_BY (ano))
    """)

    print("\t- SILVER SALVA COM SUCESSO")


# ==========================================================
# FUNÇÃO PRINCIPAL
# ==========================================================

def transform() -> None:
    """
    Pipeline completo Bronze -> Silver
    """

    bronze_root = os.path.abspath(os.getenv("BRONZE_DATA_PATH"))
    silver_root = os.path.abspath(os.getenv("SILVER_DATA_PATH"))

    path = os.path.join(bronze_root, "**", "*.csv")
    report_path = os.path.join(silver_root, "relatorio_silver.md")

    con, db_path = get_conn()

    try:
        _register_functions(con)
        _create_bronze_raw(con, path)
        _generate_report(con, report_path)
        _create_silver(con)
        _persist_silver(con, silver_root)
        _generate_plots(con, silver_root, report_path)

    finally:
        close_conn(con, db_path)
        print("PROCESSO FINALIZADO\n")
    
if __name__ == '__main__':
    load_dotenv(override=True)
    transform()