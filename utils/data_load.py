import os
from pathlib import Path
from typing import List, Optional
import zipfile
import requests
from time import sleep
from concurrent.futures import ThreadPoolExecutor, as_completed

# --------------------------------------------
# Sessão global (reutiliza conexão)
# --------------------------------------------
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/csv,application/zip,application/octet-stream,*/*",
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
    "Connection": "keep-alive",
})
MAX_WORKERS = 5


def _get_target_path() -> str:
    """
    Obtém e valida o diretório de destino a partir da variável de ambiente.
    """
    target = os.getenv("BRONZE_DATA_PATH")

    if not target:
        raise ValueError("BRONZE_DATA_PATH não definido")

    Path(target).mkdir(parents=True, exist_ok=True)

    return target


def _is_real_file(response: requests.Response, url: str) -> bool:
    """
    Garante que não estamos recebendo HTML mascarado.
    """

    if response.status_code != 200:
        return False

    content_type = response.headers.get("Content-Type", "").lower()

    # Se for HTML, provavelmente bloqueio
    if "text/html" in content_type:
        return False

    # Confirma coerência extensão ↔ tipo
    if url.endswith(".csv") and "csv" in content_type:
        return True

    if url.endswith(".zip") and (
        "zip" in content_type or "octet-stream" in content_type
    ):
        return True

    # fallback: aceita se não for html
    return "html" not in content_type


def _file_already_exists(path: str) -> bool:
    """
    Evita re-download de arquivos já existentes.
    """
    return os.path.exists(path) and os.path.getsize(path) > 0


def _save_file(response: requests.Response, path: str) -> None:
    """
    Salva o conteúdo com proteção contra corrupção.
    """
    try:
        with open(path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

    except Exception:
        if os.path.exists(path):
            os.remove(path)
        raise

def _extract_zip(path: str, target: str) -> List[str]:
    """
    Extrai arquivos de um ZIP e retorna lista de arquivos extraídos.
    """
    extracted_files = []

    try:
        with zipfile.ZipFile(path, 'r') as zip_ref:
            zip_ref.extractall(target)
            extracted_files = [
                os.path.join(target, name)
                for name in zip_ref.namelist()
            ]

        # 🔥 opcional: remove zip após extrair
        os.remove(path)

    except Exception as e:
        print(f"\t\t❌ erro ao extrair ZIP: {os.path.basename(path)}")
        return []

    return extracted_files

def _build_storage_path(url: str, target: str) -> str:
    """
    Constrói caminho preservando estrutura relevante da URL:
    - mantém pasta do ano (quando existir)
    - evita colisão de nomes iguais em anos diferentes
    """
    parts = url.split("/")

    # tenta identificar ano (dsan/YYYY/)
    try:
        dsan_index = parts.index("dsan")
        year = parts[dsan_index + 1]
        filename = parts[-1]

        year_path = os.path.join(target, "dsan", year)
        Path(year_path).mkdir(parents=True, exist_ok=True)

        return os.path.join(year_path, filename)

    except (ValueError, IndexError):
        # fallback padrão
        filename = parts[-1]
        return os.path.join(target, filename)
    
def _download_file(url: str, target: str, idx: int, total: int) -> Optional[List[str]]:
    """
    Download robusto:
    - Session persistente
    - Retry com backoff exponencial
    - Detecção de bloqueio por HTML
    - Evita colisão por estrutura
    """

    path = _build_storage_path(url, target)
    filename = os.path.basename(path)

    Path(os.path.dirname(path)).mkdir(parents=True, exist_ok=True)

    if _file_already_exists(path):
        print(f"\t\t[{idx}/{total}] ⏭️ já existe: {filename}")
        return [path]

    max_attempts = 5

    for attempt in range(max_attempts):
        try:
            response = SESSION.get(url, timeout=30, stream=True)

            if not _is_real_file(response, url):
                if attempt == 0:
                    print(f"\t\t[{idx}/{total}] ⚠️ bloqueado ou inexistente: {filename}")
                sleep(2 ** attempt)
                continue

            if attempt == 0:
                print(f"\t\t[{idx}/{total}] baixando: {filename}")

            _save_file(response, path)

            # Tratamento de ZIP
            if filename.endswith(".zip"):
                extracted = _extract_zip(path, os.path.dirname(path))
                sleep(0.5)  # pequena pausa anti-ban
                return extracted

            sleep(0.2)  # micro delay anti-rate-limit
            return [path]

        except requests.RequestException:
            sleep(2 ** attempt)

    print(f"\t\t❌ falha definitiva: {filename}")
    return None


def load(urls: List[str]) -> List[str]:
    """
    Função principal com paralelismo controlado e shutdown seguro.
    """
    target = _get_target_path()
    valid_files = []

    total = len(urls)
    print(f"\tEXTRAINDO {total} arquivos para pasta: {target}:")

    executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
    futures = []

    try:
        # 🔹 Submete tarefas
        for i, url in enumerate(urls, 1):
            futures.append(
                executor.submit(_download_file, url, target, i, total)
            )

        # 🔹 Coleta resultados
        for future in as_completed(futures):
            result = future.result()

            if result:
                valid_files.extend(result)

    except KeyboardInterrupt:
        print("\n\t⛔ Interrompido pelo usuário. Cancelando downloads...")

        # 🔥 Cancela tudo que ainda não começou
        for future in futures:
            future.cancel()

        executor.shutdown(wait=False)

        raise  # mantém comportamento padrão (encerra script)

    finally:
        executor.shutdown(wait=True)

    return valid_files