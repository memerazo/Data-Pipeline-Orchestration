#!/bin/bash

echo "Construyendo imagen de Spark..."
docker build -t custom-spark:latest -f Dockerfile.spark .

echo "Construyendo imagen de Airflow..."
docker build -t custom-airflow:latest -f Dockerfile.airflow .

echo "Imágenes construidas exitosamente"