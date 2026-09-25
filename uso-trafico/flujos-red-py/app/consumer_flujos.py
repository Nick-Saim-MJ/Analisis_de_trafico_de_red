import json
import os
import time

from kafka import KafkaConsumer


TOPIC_FLUJOS = os.getenv("KAFKA_TOPIC_FLUJOS", "flujo-eventos")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "flujos-red-py-group")
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

# Contrato de flujo.cerrado: campo -> tipos aceptados. bool se excluye aparte
# porque en Python True/False también son int.
CAMPOS_REQUERIDOS = {
    "tipoEvento": (str,),
    "flujoId": (str,),
    "srcAddr": (str,),
    "srcPort": (int,),
    "dstAddr": (str,),
    "dstPort": (int,),
    "protocolo": (int,),
    "inicioFlujoUs": (int,),
    "duracionUs": (int,),
    "bytesPorSeg": (int, float),
    "paquetesFwd": (int,),
    "paquetesBwd": (int,),
    "timestamp": (int,),
}


def deserialize_message(value):
    # Nunca lanza: bytes no UTF-8, texto plano o JSON que no es objeto
    # quedan marcados para que el loop los registre como invalid.
    try:
        text = value.decode("utf-8")
    except UnicodeDecodeError as ex:
        return {"payload": None, "raw": repr(value[:200]), "decodeError": str(ex)}
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as ex:
        return {"payload": None, "raw": text, "decodeError": str(ex)}
    if not isinstance(payload, dict):
        return {"payload": None, "raw": text, "decodeError": "el JSON no es un objeto"}
    return {"payload": payload, "raw": text, "decodeError": None}


def validar_contrato(event):
    for campo, tipos in CAMPOS_REQUERIDOS.items():
        valor = event.get(campo)
        if valor is None:
            return f"falta el campo {campo}"
        if isinstance(valor, bool) or not isinstance(valor, tipos):
            return f"tipo inválido en {campo}"
    if event["tipoEvento"] != "flujo.cerrado":
        return f"tipoEvento no esperado: {event['tipoEvento']}"
    return None


consumer = KafkaConsumer(
    TOPIC_FLUJOS,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id=GROUP_ID,
    value_deserializer=deserialize_message,
    key_deserializer=lambda key: key.decode("utf-8", errors="replace") if key is not None else None,
)

print(json.dumps({
    "service": "flujos-red-py",
    "component": "consumer",
    "topic": TOPIC_FLUJOS,
    "groupId": GROUP_ID,
    "bootstrapServers": BOOTSTRAP_SERVERS,
    "status": "listening",
}))

for msg in consumer:
    decoded = msg.value
    event = decoded["payload"] or {}
    motivo = decoded["decodeError"] or validar_contrato(event)
    is_valid = motivo is None

    processed_at = int(time.time() * 1000)
    timestamp = event.get("timestamp") if is_valid else None
    latency_ms = processed_at - timestamp if timestamp is not None else None

    log = {
        "service": "flujos-red-py",
        "component": "consumer",
        "topic": msg.topic,
        "partition": msg.partition,
        "offset": msg.offset,
        "key": msg.key,
        "groupId": GROUP_ID,
        "eventType": event.get("tipoEvento"),
        "flujoId": event.get("flujoId"),
        "protocolo": event.get("protocolo") if is_valid else None,
        "bytesPorSeg": event.get("bytesPorSeg") if is_valid else None,
        "isValid": is_valid,
        "motivo": motivo,
        "processedAt": processed_at,
        "latencyMs": latency_ms,
        "status": "consumed" if is_valid else "invalid",
    }
    if not is_valid:
        log["rawPayload"] = decoded["raw"]

    print(json.dumps(log, ensure_ascii=False))
