# docker/scheduler/Dockerfile
FROM python:3.9-slim

# Actualizar pip y instalar Dask y dependencias
RUN pip install --upgrade pip
RUN pip install dask[distributed] s3fs boto3

# Establecer el directorio de trabajo
WORKDIR /app

# Exponer los puertos necesarios para el scheduler
EXPOSE 8786 8787

# Comando para iniciar el scheduler
CMD ["dask-scheduler"]
