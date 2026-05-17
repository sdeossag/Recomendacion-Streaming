FROM python:3.9-slim

WORKDIR /app

# Instalar dependencias del sistema necesarias para compilar algunas librerias si fuera necesario
RUN apt-get update && apt-get install -y \
    build-essential \
    default-jre-headless \
    && rm -rf /var/lib/apt/lists/*

# Configurar Java para PySpark
ENV JAVA_HOME=/usr/lib/jvm/default-java

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Exponer el puerto de Streamlit
EXPOSE 8501

# Comando por defecto para correr la app
CMD ["streamlit", "run", "streamlit_app.py", "--server.port=8501", "--server.address=0.0.0.0"]
