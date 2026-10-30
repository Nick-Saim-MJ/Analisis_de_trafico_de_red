"""Misma construcción de ventanas de 5 min que uso-trafico/ventanas-red-py/app/producer_ventanas.py
(y que la Fase 3 de S10). Se copia aquí porque esa carpeta no se monta en el contenedor de pyspark;
si se cambia allá, hay que cambiarla aquí también."""
import numpy as np
import pandas as pd

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
