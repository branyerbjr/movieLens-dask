# scripts/process_movielens.py
import dask.dataframe as dd
from dask.distributed import Client
import os

def main():
    # Conectar al clúster de Dask
    scheduler_address = os.getenv('SCHEDULER_ADDRESS', 'scheduler:8786')
    client = Client(scheduler_address)
    print("Conectado al clúster de Dask.")

    # Ruta al archivo de MovieLens en S3
    bucket_name = 'tecmovielens'
    movielens_file = 'genome-scores.csv'
    s3_path = f's3://{bucket_name}/ml-latest/{movielens_file}'

    # Leer los datos usando Dask
    print(f"Leyendo datos desde {s3_path}...")
    df = dd.read_csv(
        s3_path,
        storage_options={'client_kwargs': {'endpoint_url': 'https://s3.amazonaws.com'}},
        dtype={'movieId': 'int64', 'tagId': 'int64', 'relevance': 'float64'}
    )
    print("Datos leídos correctamente.")

    # 1. Calcular el promedio de relevancia por tagId
    print("Calculando el promedio de relevancia por tagId...")
    average_relevance_per_tag = df.groupby('tagId')['relevance'].mean().compute()
    average_relevance_per_tag = average_relevance_per_tag.reset_index().rename(columns={'relevance': 'average_relevance'})

    # 2. Calcular el promedio de relevancia por movieId
    print("Calculando el promedio de relevancia por movieId...")
    average_relevance_per_movie = df.groupby('movieId')['relevance'].mean().compute()
    average_relevance_per_movie = average_relevance_per_movie.reset_index().rename(columns={'relevance': 'average_relevance'})

    # 3. Obtener los Top 10 tags con mayor promedio de relevancia
    print("Identificando los Top 10 tags con mayor promedio de relevancia...")
    top_tags = average_relevance_per_tag.nlargest(10, 'average_relevance')

    # 4. Obtener las Top 10 películas con mayor promedio de relevancia
    print("Identificando las Top 10 películas con mayor promedio de relevancia...")
    top_movies = average_relevance_per_movie.nlargest(10, 'average_relevance')

    # Guardar los resultados de vuelta en S3
    print("Guardando los resultados en S3...")
    s3_client = 'https://s3.amazonaws.com'

    # Guardar promedio de relevancia por tagId
    result_path_tags = f's3://{bucket_name}/analysis/average_relevance_per_tag.csv'
    average_relevance_per_tag.to_csv(result_path_tags, single_file=True)
    print(f"Promedio de relevancia por tagId guardado en {result_path_tags}")

    # Guardar promedio de relevancia por movieId
    result_path_movies = f's3://{bucket_name}/analysis/average_relevance_per_movie.csv'
    average_relevance_per_movie.to_csv(result_path_movies, single_file=True)
    print(f"Promedio de relevancia por movieId guardado en {result_path_movies}")

    # Guardar Top 10 tags
    result_path_top_tags = f's3://{bucket_name}/analysis/top_10_tags.csv'
    top_tags.to_csv(result_path_top_tags, single_file=True)
    print(f"Top 10 tags guardado en {result_path_top_tags}")

    # Guardar Top 10 películas
    result_path_top_movies = f's3://{bucket_name}/analysis/top_10_movies.csv'
    top_movies.to_csv(result_path_top_movies, single_file=True)
    print(f"Top 10 películas guardado en {result_path_top_movies}")

    # Cerrar la conexión del cliente
    client.close()
    print("Conexión al clúster de Dask cerrada.")

if __name__ == "__main__":
    main()
