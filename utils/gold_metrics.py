import os
import psycopg2
from dotenv import load_dotenv


# ==========================================================
# 🔹 CONEXÃO
# ==========================================================

def get_conn():
    return psycopg2.connect(
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT"),
        database=os.getenv("PG_DATABASE"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
    )


def execute_query(title, query):
    print(f"\n{title}")

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(query)

    rows = cur.fetchall()
    for row in rows:
        print(row)

    conn.close()


# ==========================================================
# 🔹 MÉTRICAS DE NEGÓCIO
# ==========================================================

def metric_1_estado_maior_preco_medio():
    execute_query(
        "1️⃣ Estado com MAIOR preço médio de venda",
        """
        SELECT l.estado_sigla,
               ROUND(AVG(f.valor_venda)::numeric,2) AS preco_medio
        FROM fact_precos_combustivel f
        JOIN dim_localidade l ON f.localidade_id = l.localidade_id
        GROUP BY l.estado_sigla
        ORDER BY preco_medio DESC
        LIMIT 1;
        """
    )


def metric_2_estado_menor_preco_medio():
    execute_query(
        "2️⃣ Estado com MENOR preço médio de venda",
        """
        SELECT l.estado_sigla,
               ROUND(AVG(f.valor_venda)::numeric,2) AS preco_medio
        FROM fact_precos_combustivel f
        JOIN dim_localidade l ON f.localidade_id = l.localidade_id
        GROUP BY l.estado_sigla
        ORDER BY preco_medio ASC
        LIMIT 1;
        """
    )


def metric_3_produto_maior_variacao():
    execute_query(
        "3️⃣ Produto com maior variação histórica de preço",
        """
        SELECT p.produto,
               ROUND((MAX(f.valor_venda) - MIN(f.valor_venda))::numeric, 2) AS variacao
        FROM fact_precos_combustivel f
        JOIN dim_produto p ON f.produto_id = p.produto_id
        GROUP BY p.produto
        ORDER BY variacao DESC
        LIMIT 1;
        """
    )


def metric_4_media_por_regiao():
    execute_query(
        "4️⃣ Média de preço por região",
        """
        SELECT l.regiao_sigla,
               ROUND(AVG(f.valor_venda)::numeric,2) AS media_preco
        FROM fact_precos_combustivel f
        JOIN dim_localidade l ON f.localidade_id = l.localidade_id
        GROUP BY l.regiao_sigla
        ORDER BY media_preco DESC;
        """
    )


def metric_5_top_5_bandeiras_preco_medio():
    execute_query(
        "5️⃣ Top 5 bandeiras com maior preço médio",
        """
        SELECT po.bandeira,
               ROUND(AVG(f.valor_venda)::numeric,2) AS preco_medio
        FROM fact_precos_combustivel f
        JOIN dim_posto po ON f.posto_id = po.posto_id
        GROUP BY po.bandeira
        ORDER BY preco_medio DESC
        LIMIT 5;
        """
    )


def metric_6_margem_media_por_produto():
    execute_query(
        "7️⃣ Margem média (venda - compra) por produto",
        """
        SELECT p.produto,
               ROUND(AVG(f.valor_venda - f.valor_compra)::numeric,2) AS margem_media
        FROM fact_precos_combustivel f
        JOIN dim_produto p ON f.produto_id = p.produto_id
        GROUP BY p.produto
        ORDER BY margem_media DESC;
        """
    )


# ==========================================================
# 🔹 EXECUTAR TODAS
# ==========================================================

def run_all_metrics():
    print("\nEXECUTANDO MÉTRICAS GOLD")

    metric_1_estado_maior_preco_medio()
    metric_2_estado_menor_preco_medio()
    metric_3_produto_maior_variacao()
    metric_4_media_por_regiao()
    metric_5_top_5_bandeiras_preco_medio()
    metric_6_margem_media_por_produto()

    print("\nMÉTRICAS FINALIZADAS\n")