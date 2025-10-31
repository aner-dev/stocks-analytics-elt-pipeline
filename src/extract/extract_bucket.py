import polars as pl
from minio import Minio
import io
from minio_client import get_minio_client


def extract_bucket_data(bucket_name, object_name):
    client = get_minio_client()

    response = client.get_object(bucket_name, object_name)
    data_bytes = response.read()

    buffer = io.BytesIO(data_bytes)


df_raw = extraer_minio_a_polars("data-raw", "api_data/fecha.parquet")

if df_raw is not None:
    # Continuar con tu lógica de Transformación (T) usando Polars
    df_transformed = df_raw.with_columns(
        (pl.col("columna_original") * 10).alias("columna_calculada")
    )
    # ... y luego la Carga (L) a un destino final.
