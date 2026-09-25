import csv
import json
import os
import time

from kafka import KafkaProducer


TOPIC_FLUJOS = os.getenv("KAFKA_TOPIC_FLUJOS", "flujo-eventos")
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
CSV_PATH = os.getenv("FLUJOS_CSV_PATH", "/data/trcu_muestra.csv")
INTERVAL_MS = int(os.getenv("FLUJOS_INTERVAL_MS", "500"))
# 0 = recorre todo el CSV. Un valor > 0 corta tras N eventos.
MAX_EVENTOS = int(os.getenv("FLUJOS_MAX_EVENTOS", "0"))
# RAFAGA=1 publica sin pausa entre eventos (prueba de lag del consumer group).
RAFAGA = os.getenv("FLUJOS_RAFAGA", "0") == "1"

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    key_serializer=lambda key: key.encode("utf-8"),
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)

print(json.dumps({
    "service": "flujos-red-py",
    "component": "producer",
    "bootstrapServers": BOOTSTRAP_SERVERS,
    "topic": TOPIC_FLUJOS,
    "csv": CSV_PATH,
    "intervalMs": 0 if RAFAGA else INTERVAL_MS,
    "maxEventos": MAX_EVENTOS,
    "status": "connected",
}))


def fila_a_evento(fila):
    # Solo viajan las columnas que el contrato declara (3.9 del informe), no
    # las 83 del CSV: el evento es la interfaz con otros consumidores, no el
    # volcado completo del registro de Suricata.
    return {
        "tipoEvento": "flujo.cerrado",
        "flujoId": fila["flow_id"],
        "srcAddr": fila["src_addr"],
        "srcPort": int(fila["src_port"]),
        "dstAddr": fila["dst_addr"],
        "dstPort": int(fila["dst_port"]),
        "protocolo": int(fila["ip_prot"]),
        "inicioFlujoUs": int(fila["timestamp"]),
        "duracionUs": int(fila["flow_duration"]),
        "bytesPorSeg": float(fila["bytes_per_s"]),
        "paquetesPorSeg": float(fila["pkt_per_s"]),
        "paquetesFwd": int(fila["fwd_pkt_cnt"]),
        "paquetesBwd": int(fila["bwd_pkt_cnt"]),
        "bytesFwd": int(fila["fwd_pkt_len_tot"]),
        "bytesBwd": int(fila["bwd_pkt_len_tot"]),
        "ratioDescargaCarga": float(fila["down_up_ratio"]),
        "origen": "flujos-red-py",
        "timestamp": int(time.time() * 1000),
    }


publicados = 0
with open(CSV_PATH, newline="", encoding="utf-8") as archivo:
    for fila in csv.DictReader(archivo):
        data = fila_a_evento(fila)

        # key = flujoId: Suricata puede reportar el mismo flujo más de una vez
        # (actualización y cierre); con la misma key caen en la misma
        # partición y se leen en orden, igual que ordenId en orden-eventos.
        metadata = producer.send(TOPIC_FLUJOS, key=data["flujoId"], value=data).get(timeout=10)
        publicados += 1

        print(json.dumps({
            "service": "flujos-red-py",
            "component": "producer",
            "topic": metadata.topic,
            "partition": metadata.partition,
            "offset": metadata.offset,
            "eventType": data["tipoEvento"],
            "flujoId": data["flujoId"],
            "bytesPorSeg": data["bytesPorSeg"],
            "timestamp": data["timestamp"],
            "status": "published",
        }))

        if MAX_EVENTOS and publicados >= MAX_EVENTOS:
            break
        if not RAFAGA:
            time.sleep(INTERVAL_MS / 1000)

producer.flush()
print(json.dumps({
    "service": "flujos-red-py",
    "component": "producer",
    "publicados": publicados,
    "status": "finished",
}))
