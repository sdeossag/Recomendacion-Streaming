.PHONY: help up down stop status logs run-simulator run-flink run-spark-bronze run-spark-silver run-spark-gold run-spark-pipeline run-all

help:
	@echo "================================================================================"
	@echo "                   CineMetrics - Sistema Híbrido Recomendador                  "
	@echo "================================================================================"
	@echo "Comandos disponibles de Infraestructura:"
	@echo "  make up                  - Levanta todos los servicios Docker en segundo plano"
	@echo "  make stop                - Detiene los servicios sin eliminar los datos"
	@echo "  make down                - Detiene y elimina todos los contenedores y redes"
	@echo "  make status              - Muestra el estado actual de los contenedores"
	@echo "  make logs                - Muestra los logs en tiempo real (Ctrl+C para salir)"
	@echo ""
	@echo "Procesamiento de Streaming (Tiempo Real):"
	@echo "  make run-flink           - Ejecuta el Job de Streaming de PyFlink en flink-jobmanager"
	@echo "  make run-simulator       - Copia y corre el simulador de eventos en tiempo real"
	@echo ""
	@echo "Pipeline Batch (Spark Lakehouse Iceberg):"
	@echo "  make run-spark-bronze    - Ejecuta el script de ingesta a la capa Bronze"
	@echo "  make run-spark-silver    - Ejecuta el script de limpieza/enriquecimiento a Silver"
	@echo "  make run-spark-gold      - Ejecuta el entrenamiento ALS de ML y agregación analítica"
	@echo "  make run-spark-pipeline  - Corre el pipeline batch completo (Bronze -> Silver -> Gold)"
	@echo ""
	@echo "Automatización Completa (Bonificación +1 punto):"
	@echo "  make run-all             - Levanta todo, inicia Flink y corre el pipeline batch"
	@echo "================================================================================"

up:
	docker compose up -d

stop:
	docker compose stop

down:
	docker compose down

status:
	docker compose ps

logs:
	docker compose logs -f

run-flink:
	docker exec -it flink-jobmanager flink run -d -py /opt/flink/jobs/flink_streaming_job.py

run-simulator:
	cp event_simulator.py spark/jobs/
	docker exec -it spark-master bash -c "pip install kafka-python && KAFKA_BOOTSTRAP_SERVERS=kafka:9092 python3 /home/jovyan/jobs/event_simulator.py"

run-spark-bronze:
	docker exec -it spark-master spark-submit /home/jovyan/jobs/ingestion_bronze.py

run-spark-silver:
	docker exec -it spark-master spark-submit /home/jovyan/jobs/transformation_silver.py

run-spark-gold:
	@echo "Deteniendo temporalmente streamlit para evitar bloqueo de cores en Spark..."
	docker compose stop streamlit
	docker exec -it spark-master spark-submit /home/jovyan/jobs/training_gold.py
	@echo "Reiniciando streamlit..."
	docker compose start streamlit

run-spark-pipeline: run-spark-bronze run-spark-silver run-spark-gold

run-all: up
	@echo "Esperando 10 segundos a que los servicios se estabilicen..."
	sleep 10
	@echo "Iniciando Job de Apache Flink en tiempo real..."
	docker exec -it flink-jobmanager flink run -d -py /opt/flink/jobs/flink_streaming_job.py
	@echo "Ejecutando Pipeline Batch de Spark (Bronze -> Silver -> Gold)..."
	$(MAKE) run-spark-pipeline
	@echo "¡Todo configurado con éxito!"
	@echo "Abre Streamlit en http://localhost:8501 para ver el dashboard."
	@echo "Para ver eventos en tiempo real, ejecuta: 'make run-simulator'"
