"""Agente sensor de red: corre en Windows (fuera de Docker) y publica por MQTT
la telemetría real de las tarjetas de red de esta PC.

Docker Desktop no ve las NIC físicas del host, solo su red virtual: por eso el
agente vive aquí y no en un contenedor, igual que el ESP32 de S7 vive fuera de
Kafka y solo habla MQTT.
"""
import json
import os
import re
import socket
import subprocess
import time

import paho.mqtt.client as mqtt
import psutil


MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "41883"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "lambda26/red/llsw3/telemetria")
INTERVAL_MS = int(os.getenv("AGENTE_INTERVAL_MS", "1000"))
# Lista separada por comas; vacío = todas las interfaces activas con tráfico medible.
INTERFACES = [i for i in os.getenv("AGENTE_INTERFACES", "").split(",") if i]
EXCLUIDAS = ("loopback", "teredo", "isatap")
HOST = socket.gethostname().lower()


def slug(texto):
    return re.sub(r"[^a-z0-9]+", "-", texto.lower()).strip("-")


def tipos_de_interfaz():
    """Nombre -> "fisica"/"virtual", leído de Windows (HardwareInterface)."""
    try:
        salida = subprocess.run(
            ["powershell", "-NoProfile", "-Command",
             "Get-NetAdapter | Select-Object Name, HardwareInterface | ConvertTo-Json"],
            capture_output=True, text=True, timeout=20,
        ).stdout
        datos = json.loads(salida)
        datos = datos if isinstance(datos, list) else [datos]
        return {d["Name"]: "fisica" if d["HardwareInterface"] else "virtual" for d in datos}
    except Exception:
        return {}


def interfaces_activas():
    stats = psutil.net_if_stats()
    elegidas = INTERFACES or [
        nombre for nombre, st in stats.items()
        if st.isup and not any(x in nombre.lower() for x in EXCLUIDAS)
    ]
    return [n for n in elegidas if n in stats]


TIPOS = tipos_de_interfaz()
SENSORES = interfaces_activas()

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"agente-red-{HOST}")
client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
client.loop_start()

print(json.dumps({
    "service": "agente-red",
    "component": "sensor",
    "host": HOST,
    "mqttHost": MQTT_HOST,
    "mqttTopic": MQTT_TOPIC,
    "interfaces": {n: TIPOS.get(n, "desconocida") for n in SENSORES},
    "intervalMs": INTERVAL_MS,
    "status": "connected",
}, ensure_ascii=False))

anterior = psutil.net_io_counters(pernic=True)
t_anterior = time.monotonic()

while True:
    time.sleep(INTERVAL_MS / 1000)
    actual = psutil.net_io_counters(pernic=True)
    stats = psutil.net_if_stats()
    t_actual = time.monotonic()
    # Se divide por el tiempo realmente transcurrido, no por INTERVAL_MS: si el
    # proceso se retrasa, la tasa sigue siendo correcta.
    dt = t_actual - t_anterior

    for nombre in SENSORES:
        if nombre not in actual or nombre not in anterior:
            continue
        a, b = anterior[nombre], actual[nombre]
        # Sin clamp: si el contador del sistema se reinicia (adaptador
        # deshabilitado y vuelto a habilitar), el delta sale negativo tal cual,
        # como lo mandaría un sensor real con una falla. Validarlo es trabajo
        # del consumer, no del sensor.
        evento = {
            "tipoEvento": "red.lectura",
            "sensorId": f"{HOST}-{slug(nombre)}",
            "host": HOST,
            "interfaz": nombre,
            "tipoInterfaz": TIPOS.get(nombre, "desconocida"),
            "velocidadEnlaceMbps": stats[nombre].speed if nombre in stats else 0,
            "intervaloMs": round(dt * 1000),
            "bytesRxPorSeg": round((b.bytes_recv - a.bytes_recv) / dt, 1),
            "bytesTxPorSeg": round((b.bytes_sent - a.bytes_sent) / dt, 1),
            "paquetesRxPorSeg": round((b.packets_recv - a.packets_recv) / dt, 1),
            "paquetesTxPorSeg": round((b.packets_sent - a.packets_sent) / dt, 1),
            "erroresPorSeg": round(((b.errin + b.errout) - (a.errin + a.errout)) / dt, 1),
            "descartesPorSeg": round(((b.dropin + b.dropout) - (a.dropin + a.dropout)) / dt, 1),
            "origen": "agente-red",
            "timestamp": int(time.time() * 1000),
        }
        info = client.publish(MQTT_TOPIC, json.dumps(evento, ensure_ascii=False), qos=1)
        print(json.dumps({
            "service": "agente-red",
            "component": "sensor",
            "sensorId": evento["sensorId"],
            "tipoInterfaz": evento["tipoInterfaz"],
            "bytesRxPorSeg": evento["bytesRxPorSeg"],
            "bytesTxPorSeg": evento["bytesTxPorSeg"],
            "mqttMid": info.mid,
            "timestamp": evento["timestamp"],
            "status": "published",
        }, ensure_ascii=False))

    anterior, t_anterior = actual, t_actual
