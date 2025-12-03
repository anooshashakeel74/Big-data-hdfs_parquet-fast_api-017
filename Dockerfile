# Use an official Spark image
FROM apache/spark:3.5.2

USER root
WORKDIR /app
COPY . /app

RUN pip install --no-cache-dir fastapi uvicorn pandas hdfs "pyarrow<11" pyspark==3.5.2

EXPOSE 8000