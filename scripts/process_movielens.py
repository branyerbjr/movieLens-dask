# scripts/process_movielens.py
import dask.dataframe as dd
from dask.distributed import Client
import s3fs
import os

def main():
    # Conectar al clúster de Dask
    scheduler_address = os.getenv('SCHEDULER_ADDRESS', 'scheduler:8786')
    client = Client(scheduler_address)
    print("Conectado al clúster de Dask.")

    # Ruta al archivo de MovieLens en S3
    bucket_name = 'tu-bucket-movielens'
    movielens_file = 'movielens.csv'
    s3_path = f's3://{bucket_name}/{movielens_file}'

    # Leer los datos usando Dask
    df = dd.read_csv(s3_path, storage_options={'client_kwargs': {'endpoint_url': 'https://s3.amazonaws.com'}})

    # Procesamiento de ejemplo: calcular el promedio de calificaciones por película
    average_ratings = df.groupby('movieId')['rating'].mean()

    # Guardar los resultados de vuelta en S3
    result_path = f's3://{bucket_name}/average_ratings.csv'
    average_ratings.to_csv(result_path, single_file=True)

    print(f"Resultados guardados en {result_path}")

    # Cerrar la conexión del cliente
    client.close()

if __name__ == "__main__":
    main()
