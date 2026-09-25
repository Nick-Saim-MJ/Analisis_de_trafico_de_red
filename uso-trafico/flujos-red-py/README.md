# flujos-red-py

Microservicio pequeño del Proyecto Sello (tráfico de red del campus, equipo
LLSW3), usado como evento propio en la actividad autónoma de S6. Publica el
evento `flujo.cerrado` en el topic `flujo-eventos` a partir de flujos reales
capturados con Suricata (`data/trcu_muestra.csv`, las primeras 2000 filas de
`TRCU.csv`), y lo consume validando el contrato campo por campo.

Requiere que `kafka/compose.yml` esté arriba, porque crea la red `lambda26-kafka-net`.

## Uso

Crear el topic (3 particiones, key = `flujoId`):

```powershell
docker compose -f kafka/compose.yml exec kafka /opt/kafka/bin/kafka-topics.sh --create --topic flujo-eventos --partitions 3 --replication-factor 1 --bootstrap-server kafka:9092
docker compose -f uso-trafico/flujos-red-py/compose.yml up -d --build
```

Consumer (terminal 1) y producer (terminal 2):

```powershell
docker compose -f uso-trafico/flujos-red-py/compose.yml exec flujos-red-py python /app/consumer_flujos.py
docker compose -f uso-trafico/flujos-red-py/compose.yml exec -e FLUJOS_MAX_EVENTOS=20 flujos-red-py python /app/producer_flujos.py
```

Ráfaga para medir el lag del consumer group (con el consumer detenido):

```powershell
docker compose -f uso-trafico/flujos-red-py/compose.yml exec -e FLUJOS_MAX_EVENTOS=500 -e FLUJOS_RAFAGA=1 flujos-red-py python /app/producer_flujos.py
docker compose -f kafka/compose.yml exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --describe --group flujos-red-py-group --bootstrap-server kafka:9092
```

## Variables de entorno

| Variable | Por defecto | Aplica a |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | producer y consumer |
| `KAFKA_TOPIC_FLUJOS` | `flujo-eventos` | producer y consumer |
| `FLUJOS_CSV_PATH` | `/data/trcu_muestra.csv` | producer |
| `FLUJOS_INTERVAL_MS` | `500` | producer: pausa entre eventos |
| `FLUJOS_MAX_EVENTOS` | `0` (todo el CSV) | producer |
| `FLUJOS_RAFAGA` | `0` | producer: `1` publica sin pausa |
| `KAFKA_GROUP_ID` | `flujos-red-py-group` | consumer |

El contrato completo de `flujo.cerrado` está en el informe S06 (Tablas 1 y 2).
