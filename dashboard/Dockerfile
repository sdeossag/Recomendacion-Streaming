# Misma base que spark-master: Java 11 + Spark 3.5 + dependencias Iceberg vía ivy cache
FROM jupyter/pyspark-notebook:spark-3.5.0

USER root
WORKDIR /app

RUN pip install --no-cache-dir \
    streamlit \
    pymongo \
    plotly \
    matplotlib \
    pandas

ENV SPARK_HOME=/usr/local/spark \
    PYTHONPATH=/usr/local/spark/python:/usr/local/spark/python/lib/py4j-0.10.9.7-src.zip \
    RUNNING_IN_DOCKER=1 \
    SPARK_MASTER_URL=spark://spark-master:7077 \
    MINIO_ENDPOINT=http://minio:9000 \
    SPARK_CONF_DIR=/usr/local/spark/conf \
    PATH=/opt/conda/bin:/usr/local/spark/bin:${PATH}

EXPOSE 8501

USER ${NB_UID}

CMD ["streamlit", "run", "streamlit_app.py", "--server.port=8501", "--server.address=0.0.0.0"]
