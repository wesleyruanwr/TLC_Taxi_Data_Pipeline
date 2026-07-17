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
    
    if os.path.exists(dest_path) and os.path.getsize(dest_path) > 1000000:
        print(f"{filename} ja existe, ignorando download") #se existir ignora o download
        return dest_path
        
    print(f"comecando dowload de {url} para {dest_path}")
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        with open(dest_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"dowload concluido: {dest_path}")
        return dest_path
    else:
        print(f"erro ao baixar {url}: status {response.status_code}")
        response.raise_for_status()

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("uso: python download_data.py <ano> <mes>")
        sys.exit(1)
        
    y = sys.argv[1]
    m = sys.argv[2]
    download_month(y, m)
