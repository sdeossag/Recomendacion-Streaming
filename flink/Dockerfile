FROM flink:1.18-scala_2.12

# Instalar Python 3 y dependencias del sistema para PyFlink
USER root
RUN apt-get update && \
    apt-get install -y python3 python3-pip python3-dev wget && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

# Descargar el conector Kafka y ponerlo en el classpath de Flink
RUN wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.0.1-1.18/flink-sql-connector-kafka-3.0.1-1.18.jar

# Instalar dependencias de Python
COPY requirements_flink.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt kafka-python

# Regresar al usuario flink estándar
USER flink
