import json
import os
import time

from kafka import KafkaConsumer


TOPIC_RED = os.getenv("KAFKA_TOPIC_RED", "red-telemetria")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "uso-red-group")
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
# Umbral operativo: uso del enlace a partir del cual la carga se considera
# saturación (la alerta de carga anómala de la dimensión U2 del brief). 70% es
# la regla usual de alerta temprana en planificación de capacidad; con 80% no
# dispara nunca en esta PC, porque el plan del ISP topa en ~80 Mbps reales
# sobre un enlace de 100 Mbps (pico medido: 78.8%).
UMBRAL_SATURACION_PCT = float(os.getenv("UMBRAL_SATURACION_PCT", "70"))
# Margen sobre la capacidad nominal: el contador del SO incluye cabeceras y la
# ventana de muestreo no es perfecta, así que un 5% por encima todavía es físico.
TOLERANCIA_CAPACIDAD = 1.05

CAMPOS_REQUERIDOS = {
    "tipoEvento": (str,),
    "sensorId": (str,),
    "interfaz": (str,),
    "velocidadEnlaceMbps": (int, float),
    "bytesRxPorSeg": (int, float),
    "bytesTxPorSeg": (int, float),
    "paquetesRxPorSeg": (int, float),
    "paquetesTxPorSeg": (int, float),
    "erroresPorSeg": (int, float),
    "descartesPorSeg": (int, float),
    "timestamp": (int,),
}
NO_NEGATIVOS = ("bytesRxPorSeg", "bytesTxPorSeg", "paquetesRxPorSeg",
                "paquetesTxPorSeg", "erroresPorSeg", "descartesPorSeg")


def deserialize_message(value):
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


def validar_esquema(event):
    """Forma del mensaje: campos presentes y con el tipo correcto."""
    for campo, tipos in CAMPOS_REQUERIDOS.items():
        valor = event.get(campo)
        if valor is None:
            return f"falta el campo {campo}"
        if isinstance(valor, bool) or not isinstance(valor, tipos):
            return f"tipo inválido en {campo}"
    if event["tipoEvento"] != "red.lectura":
        return f"tipoEvento no esperado: {event['tipoEvento']}"
    return None


def validar_rango_fisico(event):
    """Valor del mensaje: solo corre si el esquema ya es válido."""
    for campo in NO_NEGATIVOS:
        if event[campo] < 0:
            return f"{campo} negativo ({event[campo]}): contador reiniciado o lectura corrupta"
    capacidad = event["velocidadEnlaceMbps"] * 1_000_000 / 8
    trafico = max(event["bytesRxPorSeg"], event["bytesTxPorSeg"])
    if capacidad <= 0:
        return f"enlace sin velocidad reportada con {trafico} B/s de tráfico" if trafico > 0 else None
    if trafico > capacidad * TOLERANCIA_CAPACIDAD:
        return (f"{trafico:.0f} B/s supera la capacidad física del enlace "
                f"({event['velocidadEnlaceMbps']} Mbps = {capacidad:.0f} B/s)")
    return None


def uso_enlace_pct(event):
    capacidad = event["velocidadEnlaceMbps"] * 1_000_000 / 8
    if capacidad <= 0:
        return None
    return round(max(event["bytesRxPorSeg"], event["bytesTxPorSeg"]) / capacidad * 100, 2)


consumer = KafkaConsumer(
    TOPIC_RED,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id=GROUP_ID,
    value_deserializer=deserialize_message,
    key_deserializer=lambda key: key.decode("utf-8", errors="replace") if key is not None else None,
)

print(json.dumps({
    "service": "uso-red",
    "component": "consumer",
    "topic": TOPIC_RED,
    "groupId": GROUP_ID,
    "umbralSaturacionPct": UMBRAL_SATURACION_PCT,
    "status": "listening",
}, ensure_ascii=False))

for msg in consumer:
    decoded = msg.value
    event = decoded["payload"] or {}
    error_esquema = decoded["decodeError"] or validar_esquema(event)
    is_valid = error_esquema is None
    error_rango = validar_rango_fisico(event) if is_valid else None
    uso = uso_enlace_pct(event) if is_valid and error_rango is None else None
    saturado = uso is not None and uso >= UMBRAL_SATURACION_PCT

    if not is_valid:
        status = "invalid"
    elif error_rango:
        status = "alerta"
    elif saturado:
        status = "saturacion"
    else:
        status = "consumed"

    processed_at = int(time.time() * 1000)
    timestamp = event.get("timestamp") if is_valid else None

    log = {
        "service": "uso-red",
        "component": "consumer",
        "topic": msg.topic,
        "partition": msg.partition,
        "offset": msg.offset,
        "key": msg.key,
        "sensorId": event.get("sensorId"),
        "bytesRxPorSeg": event.get("bytesRxPorSeg") if is_valid else None,
        "bytesTxPorSeg": event.get("bytesTxPorSeg") if is_valid else None,
        "usoEnlacePct": uso,
        "isValid": is_valid,
        "fueraDeRango": error_rango is not None,
        "motivo": error_esquema or error_rango or (f"uso {uso}% >= {UMBRAL_SATURACION_PCT}%" if saturado else None),
        "processedAt": processed_at,
        "latencyMs": processed_at - timestamp if timestamp is not None else None,
        "status": status,
    }
    if not is_valid:
        log["rawPayload"] = decoded["raw"]

    print(json.dumps(log, ensure_ascii=False))
