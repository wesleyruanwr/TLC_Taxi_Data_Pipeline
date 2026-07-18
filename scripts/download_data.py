import os
import sys
import requests

def download_month(year, month):
    month_str = f"{int(month):02d}"
    filename = f"yellow_tripdata_{year}-{month_str}.parquet"
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{filename}"
    

    base_dir = "/opt/airflow" if os.path.exists("/opt/airflow") else "" #garante que a pasta bronze esta criada
    dest_dir = os.path.join(base_dir, "data", "bronze")
    os.makedirs(dest_dir, exist_ok=True)
    
    dest_path = os.path.join(dest_dir, filename)
    tmp_path = dest_path + ".tmp"

    # o arquivo final so e publicado depis de baixar completamente
    if os.path.exists(dest_path) and os.path.getsize(dest_path) > 1000000:
        print(f"{filename} ja existe ignorando download")
        return dest_path

    print(f"comecando download de {url} para {dest_path}")
    try:
        # timeout para evitar a task travar indefinidamente
        with requests.get(url, stream=True, timeout=(15, 120)) as response:
            response.raise_for_status()
            expected = int(response.headers.get("Content-Length", 0))
            written = 0
            with open(tmp_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    f.write(chunk)
                    written += len(chunk)

        # verificacao de integridade do tamanh0o do arquivo
        if expected and written != expected:
            raise IOError(f"download incompleto de {filename}: {written} de {expected} bytes")
        if written < 1000000:
            raise IOError(f"arquivo muito pequeno para {filename}: {written} bytes")

        os.replace(tmp_path, dest_path)  # rename que so publica o arquivo integro
        print(f"download concluido: {dest_path} ({written} bytes)")
        return dest_path
    except Exception:
        # nao deixa arquivo parcial ou corrompido para tras
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        raise

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("uso: python download_data.py <ano> <mes>")
        sys.exit(1)
        
    y = sys.argv[1]
    m = sys.argv[2]
    download_month(y, m)
