import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, BooleanType

def process_silver(year, month):
    month_str = f"{int(month):02d}"
    filename = f"yellow_tripdata_{year}-{month_str}.parquet"
    bronze_path = os.path.join("/opt/airflow/data", "bronze", filename)
    
    if not os.path.exists(bronze_path):
        
        bronze_path = os.path.join("data", "bronze", filename) # exec local se não estiver no docker
        if not os.path.exists(bronze_path):
            raise FileNotFoundError(f"arquivo bronze nao encontrado: {bronze_path}")

    print(f"iniciando processamentoPySpark para o arquivo: {bronze_path}")

    spark = SparkSession.builder \
        .appName("TLC yellow taxi silver processing") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3") \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()

    df = spark.read.parquet(bronze_path)

    # colunas de data e competência mensal
    df = df.withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
    df = df.withColumn("pickup_year_month", F.date_format("tpep_pickup_datetime", "yyyyMM"))

    # calcular duração da viagem
    df = df.withColumn("trip_duration_minutes", 
                       (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60.0)



    # regras de qualidade e sinalizacoes

    # duração > 6h (360 min) = anomalia / distancia > 100 milhas = anomalia / datas inconsistentes (dropoff antes de pickup)
    # pagamento inválido (nao é 1 ou 2) = receita inválida ou tipo inválido / distancia negativa ou valor negativo
    # quant passageiros menor/igual a 0 ou maior que 8 / ID de tarifa fora das mapeadas pela TLC (1 a 6
    #local de embarque invalido (TLC Zone IDs vão de 1 a 265) / local de desembarque inválido (fora do intervalo 1 a 265)
    
    
    errors_expr = F.array([
        F.when(F.col("trip_duration_minutes") > 360, "anomaly_duration_gt_6h"),
        F.when(F.col("trip_distance") > 100, "anomaly_distance_gt_100mi"),
        F.when(~F.col("payment_type").isin(1, 2), "invalid_payment_type"),
        F.when(F.col("tpep_dropoff_datetime") <= F.col("tpep_pickup_datetime"), "invalid_dates_dropoff_before_pickup"),
        F.when(F.col("trip_distance") < 0, "negative_distance"),
        F.when(F.col("total_amount") < 0, "negative_total_amount"),
        F.when(F.col("passenger_count").isNull() | (F.col("passenger_count") <= 0) | (F.col("passenger_count") > 8), "invalid_passenger_count"),
        F.when(F.col("RatecodeID").isNull() | ~F.col("RatecodeID").between(1, 6), "invalid_ratecode_id"),
        F.when(F.col("PULocationID").isNull() | ~F.col("PULocationID").between(1, 265), "invalid_pickup_location"),
        F.when(F.col("DOLocationID").isNull() | ~F.col("DOLocationID").between(1, 265), "invalid_dropoff_location")
    ])
    
    # remover nulos do array de erros e concatenar
    df = df.withColumn("invalid_reasons_arr", F.array_remove(errors_expr, None))
    df = df.withColumn("invalid_reason", 
                       F.when(F.size("invalid_reasons_arr") > 0, F.array_join("invalid_reasons_arr", "; "))
                       .otherwise(F.lit(None).cast(StringType())))
    
    # se houver erro no array is_valid_trip = False
    df = df.withColumn("is_valid_trip", F.when(F.size("invalid_reasons_arr") > 0, False).otherwise(True))
    
    # drop coluna temp
    df = df.drop("invalid_reasons_arr")

    silver_df = df.select(
        "VendorID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "RatecodeID",
        "store_and_fwd_flag",
        "PULocationID",
        "DOLocationID",
        "payment_type",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "total_amount",
        "congestion_surcharge",
        "Airport_fee",
        "pickup_date",
        "pickup_year_month",
        "trip_duration_minutes",
        "is_valid_trip",
        "invalid_reason"
    )

    
    db_url = "jdbc:postgresql://postgres:5432/ny_taxi"
    db_properties = {
        "user": "postgres",
        "password": "postgres",
        "driver": "org.postgresql.Driver"
    }

    print("gravando dados no PostgreSQL (tabela: silver.trips)")
    
    try:
        silver_df.write \
            .mode("append") \
            .jdbc(url=db_url, table="silver.trips", properties=db_properties)
        print(f"processamento foiconcluido com sucesso para o lote {year}-{month_str}!")
    except Exception as e:
        print(f"erro ao gravar no PostgreSQL: {e}")
        raise e
    finally:
        spark.stop()

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Uso: python process_silver.py <ano> <mes>")
        sys.exit(1)
    
    y = sys.argv[1]
    m = sys.argv[2]
    process_silver(y, m)
