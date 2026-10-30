"""Agregador de borde de S10: convierte flujos reales de Suricata (TRCU.csv) en
ventanas de 5 minutos de tráfico del campus y publica cada ventana en Kafka con
las mismas variables que usó el modelo para entrenar.

Es lo que haría un componente junto al sensor: recibe cada flow.cerrado, cuenta
bytes y flujos por ventana y recuerda las ventanas anteriores. Así Spark no
necesita estado para predecir: cada evento trae todo lo que el modelo usa.

Solo publica ventanas desde VENTANAS_DESDE (el tramo de prueba, que el modelo
nunca vio) y las emite en tiempo comprimido: una ventana de 5 min cada
INTERVAL_MS.
"""
import json
import os
import time

import numpy as np
import pandas as pd
from kafka import KafkaProducer

TOPIC = os.getenv("KAFKA_TOPIC_VENTANAS", "trafico-ventanas")
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
CSV_PATH = os.getenv("FLUJOS_CSV_PATH", "/data/TRCU.csv")
VENTANAS_DESDE = os.getenv("VENTANAS_DESDE")          # ISO en hora de Lima, ej. 2026-08-25T00:00:00
MAX_VENTANAS = int(os.getenv("MAX_VENTANAS", "120"))
INTERVAL_MS = int(os.getenv("VENTANAS_INTERVAL_MS", "2000"))
SENSOR_ID = os.getenv("SENSOR_ID", "campus-trcu")

GAP_SESION = pd.Timedelta("10min")   # hueco que separa dos sesiones de captura
MIN_FLUJOS_SESION = 500
LIMA = "America/Lima"


def construir_ventanas(ruta):
    """Mismas definiciones que la Fase 3 del notebook S10 (pandas en vez de Spark)."""
    df = pd.read_csv(ruta, usecols=["timestamp", "flow_duration", "fwd_pkt_len_tot",
                                    "bwd_pkt_len_tot", "ip_prot", "dst_port"])
    # El registro de un flujo existe recién cuando Suricata lo cierra: se ubica en su fin.
    df["cierre"] = pd.to_datetime(df.timestamp + df.flow_duration, unit="us", utc=True)
    df["bytes"] = df.fwd_pkt_len_tot + df.bwd_pkt_len_tot
    df["tcp"] = (df.ip_prot == 6).astype(int)
    df["web"] = df.dst_port.isin([80, 443]).astype(int)
    df = df.sort_values("cierre")
    df["sesion"] = (df.cierre.diff() > GAP_SESION).cumsum()
    df = df[df.groupby("sesion").cierre.transform("size") >= MIN_FLUJOS_SESION]

    v = (df.assign(ventana=df.cierre.dt.floor("5min"))
           .groupby(["sesion", "ventana"])
           .agg(bytes=("bytes", "sum"), flujos=("bytes", "size"), tcpRatio=("tcp", "mean"), webRatio=("web", "mean"))
           .reset_index()
           .sort_values(["sesion", "ventana"]))
    v["bytesPorSeg"] = v.bytes / 300
    v["ly"] = np.log1p(v.bytesPorSeg)
    v["lf"] = np.log1p(v.flujos)
    g = v.groupby("sesion")
    v["rn"] = g.cumcount() + 1
    v["l1"] = v.ly
    v["l2"] = g.ly.shift(1)
    v["l3"] = g.ly.shift(2)
    v["l6"] = g.ly.shift(5)
    v["l12"] = g.ly.shift(11)
    v["m6"] = g.ly.transform(lambda x: x.rolling(6).mean())
    v["m12"] = g.ly.transform(lambda x: x.rolling(12).mean())
    v["sd6"] = g.ly.transform(lambda x: x.rolling(6).std())
    v["f1"] = v.lf
    v["f2"] = g.lf.shift(1)
    v["f3"] = g.lf.shift(2)
    v["mf6"] = g.lf.transform(lambda x: x.rolling(6).mean())
    local = v.ventana.dt.tz_convert(LIMA)
    v["hSin"] = np.sin(2 * np.pi * local.dt.hour / 24)
    v["hCos"] = np.cos(2 * np.pi * local.dt.hour / 24)
    v["finde"] = (local.dt.dayofweek >= 5).astype(int)
    return v[v.rn >= 12]   # con menos de 12 ventanas previas no hay historia completa


producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    key_serializer=lambda key: key.encode("utf-8"),
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)

ventanas = construir_ventanas(CSV_PATH)
if VENTANAS_DESDE:
    desde = pd.Timestamp(VENTANAS_DESDE).tz_localize(LIMA)
    ventanas = ventanas[ventanas.ventana >= desde]
ventanas = ventanas.head(MAX_VENTANAS)

print(json.dumps({"service": "ventanas-red-py", "component": "producer", "topic": TOPIC,
                  "ventanas": len(ventanas), "desde": str(ventanas.ventana.min().tz_convert(LIMA)),
                  "hasta": str(ventanas.ventana.max().tz_convert(LIMA)), "intervalMs": INTERVAL_MS,
                  "status": "connected"}))

CAMPOS = ["l1", "l2", "l3", "l6", "l12", "m6", "m12", "sd6", "f1", "f2", "f3", "mf6",
          "tcpRatio", "webRatio", "hSin", "hCos", "finde"]
for _, fila in ventanas.iterrows():
    evento = {
        "tipoEvento": "trafico.ventana",
        "sensorId": SENSOR_ID,
        "ventanaInicio": int(fila.ventana.timestamp() * 1000),
        "bytesPorSeg": round(float(fila.bytesPorSeg), 3),
        "flujos": int(fila.flujos),
        **{c: round(float(fila[c]), 6) for c in CAMPOS},
        "origen": "ventanas-red-py",
        "timestamp": int(time.time() * 1000),
    }
    meta = producer.send(TOPIC, key=SENSOR_ID, value=evento).get(timeout=10)
    print(json.dumps({"service": "ventanas-red-py", "component": "producer", "partition": meta.partition,
                      "offset": meta.offset, "ventana": str(fila.ventana.tz_convert(LIMA)),
                      "bytesPorSeg": evento["bytesPorSeg"], "flujos": evento["flujos"], "status": "published"}))
    time.sleep(INTERVAL_MS / 1000)

producer.flush()
print(json.dumps({"service": "ventanas-red-py", "component": "producer", "publicadas": len(ventanas), "status": "finished"}))
