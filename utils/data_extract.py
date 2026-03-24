from typing import List


def _generate_dsas_urls(base_url: str) -> List[str]:
    """
    Padrão DSAS:
    - até 2020 → CSV
    - 2021+ → ZIP
    """
    urls = []
    
    urls.append(
        f"https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc/dsas/ca/precos-semestrais-ca.zip"
    )

    for year in range(2004, 2026):
        for month in range(1, 3):

            mm = f"{month:02d}"

            if year <= 2021:
                urls.append(
                    f"{base_url}/shpc/dsas/ca/ca-{year}-{mm}.csv"
                )
            else:
                urls.append(
                    f"{base_url}/shpc/dsas/ca/ca-{year}-{mm}.zip"
                )

    return urls


def _generate_dsas_extra_urls(base_url: str) -> List[str]:
    """
    Padrões adicionais DSAS:
    - ZIP semestral consolidado
    - GLP mensal
    """
    urls = []

    # -----------------------------
    # ZIP semestral consolidado CA
    # -----------------------------
    urls.append(
        f"{base_url}/shpc/dsas/ca/precos-semestrais-ca.zip"
    )

    # -----------------------------
    # GLP mensal
    # -----------------------------
    for year in range(2004, 2026):
        for month in range(1, 3):

            mm = f"{month:02d}"

            urls.append(
                f"{base_url}/shpc/dsas/glp/glp-{year}-{mm}.csv"
            )

    return urls


def _generate_dsan_urls(base_url: str) -> List[str]:
    """
    Padrões DSAN oficiais confirmados:
    - precos-diesel-gnv-YYYY-MM.csv
    - precos-gasolina-etanol-YYYY-MM.csv
    - precos-glp-YYYY-MM.csv
    - MM-dados-abertos-precos-gasolina-etanol.csv
    """

    urls = []
    
    # -----------------------------
    # Gasolina / Etanol
    # -----------------------------
    
    urls.append(
        f"{base_url}/shpc/dsan/2026/01-dados-abertos-precos-gasolina-etanol.csv"
    )
    
    for year in range(2022, 2023):
        for month in range(1, 3):
            mm = f"{month:02d}"
            
            # -----------------------------
            # Gasolina / Etanol
            # -----------------------------
            urls.append(
                f"{base_url}/shpc/dsan/{year}/precos-gasolina-etanol-{year}-{mm}.csv"
            )
            
            urls.append(
                f"{base_url}/shpc/dsan/{year}/dados-abertos-precos-{year}-{mm}-gasolina-etanol.csv"
            )
            

    for year in range(2021, 2027):
        for month in range(1, 13):

            mm = f"{month:02d}"

            # -----------------------------
            # Diesel / GNV
            # -----------------------------
            urls.append(
                f"{base_url}/shpc/dsan/{year}/precos-diesel-gnv-{year}-{mm}.csv"
            )
            # -----------------------------
            # Gasolina / Etanol
            # -----------------------------
            urls.append(
                f"{base_url}/shpc/dsan/{year}/precos-gasolina-etanol-{mm}.csv"
            )

            # -----------------------------
            # GLP
            # -----------------------------
            
            urls.append(
                f"{base_url}/shpc/dsan/{year}/precos-glp-{mm}.csv"
            )
        
        for month in range(1, 3):

            mm = f"{month:02d}"

            # -----------------------------
            # GLP
            # -----------------------------
            urls.append(
                f"{base_url}/shpc/dsan/{year}/precos-glp-{year}-{mm}.csv"
            )

    return urls


def _generate_qus_urls(base_url: str) -> List[str]:
    """
    Padrões QUS (dados mais recentes / agregados)
    """
    return [
        f"{base_url}/shpc/qus/ultimas-4-semanas-diesel-gnv.csv",
        f"{base_url}/shpc/qus/ultimas-4-semanas-gasolina-etanol.csv",
        f"{base_url}/shpc/qus/ultimas-4-semanas-glp.csv",
    ]


def _generate_metadata_urls(base_url: str) -> List[str]:
    """
    Metadados
    """
    return [
        f"{base_url}/shpc/metadados-serie-historica-precos-combustiveis-1.pdf"
    ]


def extract(DATASET_URL: str) -> List[str]:
    """
    Gera todas as URLs oficiais do dataset ANP.

    Estratégia:
    - Apenas padrões confirmados oficialmente
    - Sem geração de variações especulativas
    - Não valida existência (responsabilidade do load)
    """

    print("\t- GERANDO URLS (DSAS + DSAS EXTRA + DSAN + QUS + METADADOS)")

    urls = []

    urls.extend(_generate_dsas_urls(DATASET_URL))        # Mantido intacto
    urls.extend(_generate_dsas_extra_urls(DATASET_URL))  # GLP + semestral
    urls.extend(_generate_dsan_urls(DATASET_URL))        # Padrões oficiais
    urls.extend(_generate_qus_urls(DATASET_URL))
    urls.extend(_generate_metadata_urls(DATASET_URL))

    print(f"\t- TOTAL DE URLS GERADAS: {len(urls)}")
    print(f"\t- EXEMPLO: {urls[0]}")

    return urls