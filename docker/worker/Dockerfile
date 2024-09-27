# docker/worker/Dockerfile
FROM python:3.9-slim

# Actualizar pip y instalar Dask y dependencias
RUN pip install --upgrade pip
RUN pip install dask[distributed] s3fs boto3

# Establecer el directorio de trabajo
WORKDIR /app

# Comando para iniciar el worker (el scheduler se configurará mediante variables de entorno)
CMD ["dask-worker", "tcp://scheduler:8786"]