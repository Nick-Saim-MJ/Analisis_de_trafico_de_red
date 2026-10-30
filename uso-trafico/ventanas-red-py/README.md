# ventanas-red-py

Agregador de borde del Proyecto Sello (S10). Convierte los flujos reales que capturó Suricata (`TRCU.csv`) en ventanas de 5 minutos de tráfico del campus y publica cada ventana en el topic `trafico-ventanas`, con las 17 variables que usa el modelo de series de tiempo de `pyspark/sesiones/s10-series-tiempo-red/`.

Las definiciones son las mismas de la Fase 3 del notebook:

- un flujo cuenta en el momento en que se cierra;
- se separan sesiones de captura cuando hay huecos de más de 10 min;
- la escala es `log1p` de bytes/s;
- se calculan rezagos y medias móviles por sesión.

Como cada evento trae su propia historia, Spark predice sin estado. En la corrida del informe, las predicciones en streaming coincidieron con las fuera de línea con una diferencia máxima de 8.6e-07.

## Uso

Requiere `kafka/compose.yml` arriba y `TRCU.csv` en `pyspark/sesiones/s10-series-tiempo-red/data/`, que no se versiona.

```powershell
docker compose -f kafka/compose.yml exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic trafico-ventanas --partitions 1 --replication-factor 1 --bootstrap-server kafka:9092
docker compose -f uso-trafico/ventanas-red-py/compose.yml up -d --build
docker compose -f uso-trafico/ventanas-red-py/compose.yml exec -e VENTANAS_DESDE=2026-08-14T08:00:00 -e MAX_VENTANAS=120 ventanas-red-py python producer_ventanas.py
```

| Variable | Por defecto | Qué hace |
|---|---|---|
| `VENTANAS_DESDE` | *(todas)* | Primera ventana a publicar (hora de Lima). Usa un tramo de la prueba, que el modelo nunca vio. |
| `MAX_VENTANAS` | `120` | Cuántas ventanas publicar. |
| `VENTANAS_INTERVAL_MS` | `2000` | Tiempo real entre ventanas (cada ventana representa 5 min). |
| `KAFKA_TOPIC_VENTANAS` | `trafico-ventanas` | Topic de destino. |
| `FLUJOS_CSV_PATH` | `/data/TRCU.csv` | Archivo de flujos montado en solo lectura. |
