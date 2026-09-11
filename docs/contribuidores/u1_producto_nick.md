# Producto U1 — Nick Saim Mayta Jara

**Dimensión:** Volumen de tráfico por flujo (`bytes_per_s`) — U1 batch, regresión
**Rol en el equipo:** Arquitectura Lambda y observabilidad general, coordinación técnica
**Notebook:** [u1_producto_nick.ipynb](../../pyspark/producto/u1_producto_nick.ipynb)

## Fase 1 — Comprensión del negocio

- **Pregunta de negocio:** ¿Cuál ha sido el volumen histórico de tráfico (bytes/segundo) de los flujos capturados, y qué volumen se puede esperar según sus características?
- **Objetivo de minería de datos:** entrenar un modelo de **regresión** que prediga `bytes_per_s` a partir de las demás variables del flujo.
- **Decisión que habilita:** anticipar picos de carga y priorizar capacidad de red antes de que ocurra congestión.
- **Criterio de éxito:** superar con margen claro una línea base ingenua (promedio histórico de `bytes_per_s`) en RMSE, con R² > 0.5 orientativo.

## Fase 2 — Comprensión de los datos

Extracción con `StructType` explícito (83 columnas, validado contra el header real del CSV). EDA sobre `bytes_per_s`, `flow_duration`, `pkt_len_mean`, `iat_mean`: estadísticos descriptivos, conteo de nulos, percentiles (min/p25/mediana/p75/max) y la línea base ingenua (promedio histórico de `bytes_per_s`). Distribución de flujos por protocolo (`ip_prot`).

## Fase 3 — Preparación de los datos

- Deriva `protocolo` (TCP/UDP/OTRO) a partir de `ip_prot`; filtra `bytes_per_s` no nulo.
- Agrega `bytes_per_s` promedio y máximo por protocolo.
- Control de calidad: deduplicación por `flow_id`, relleno de nulos en columnas clave.
- Escritura en **Parquet particionado por `protocolo`**, verificado releyendo el dataset y confirmando `PartitionFilters`.

```python
df_nick = (
    df
    .withColumn(
        "protocolo",
        F.when(F.col("ip_prot") == 6, F.lit("TCP"))
         .when(F.col("ip_prot") == 17, F.lit("UDP"))
         .otherwise(F.lit("OTRO"))
    )
    .filter(F.col("bytes_per_s").isNotNull())
)

df_dedup = df_nick.dropDuplicates(["flow_id"])
df_limpio = df_dedup.na.fill({
    "bytes_per_s": 0.0, "pkt_len_mean": 0.0, "iat_mean": 0.0,
    "active_mean": 0.0, "idle_mean": 0.0,
})

df_limpio.write.mode("overwrite").partitionBy("protocolo") \
    .parquet("/opt/artifacts/nick/flujos_particionado")
```

**Decisión — features seleccionadas para el modelo:** `ip_prot`, `flow_duration`, `pkt_len_mean`, `pkt_len_std`, `fwd_pkt_len_mean`, `bwd_pkt_len_mean`, `iat_mean`, `active_mean`, `idle_mean`, `flag_SYN`, `flag_ack`, `fwd_tcp_init_win_bytes`, `bwd_tcp_init_win_bytes`. No hay fuga de información en esta dimensión (a diferencia de Henyelrey/David) porque `bytes_per_s` no se deriva de ninguna de estas columnas.

```python
predictores_nick = [
    "ip_prot", "flow_duration", "pkt_len_mean", "pkt_len_std",
    "fwd_pkt_len_mean", "bwd_pkt_len_mean", "iat_mean",
    "active_mean", "idle_mean", "flag_SYN", "flag_ack",
    "fwd_tcp_init_win_bytes", "bwd_tcp_init_win_bytes",
]
ensamblador = VectorAssembler(inputCols=predictores_nick, outputCol="features", handleInvalid="skip")
```

## Fase 4 — Modelado

Comparación de 5 configuraciones sobre `randomSplit([0.8, 0.2], seed=42)`: `LinearRegression` base, + Ridge (L2), + Lasso (L1), + Elastic Net, y `RandomForestRegressor`.

```python
configuraciones_nick = {
    "LinearRegression base": LinearRegression(featuresCol="features", labelCol="valor_real"),
    "LinearRegression + Ridge (L2)": LinearRegression(featuresCol="features", labelCol="valor_real", regParam=0.1, elasticNetParam=0.0),
    "LinearRegression + Lasso (L1)": LinearRegression(featuresCol="features", labelCol="valor_real", regParam=0.1, elasticNetParam=1.0),
    "LinearRegression + Elastic Net": LinearRegression(featuresCol="features", labelCol="valor_real", regParam=0.1, elasticNetParam=0.5),
    "RandomForestRegressor": RandomForestRegressor(featuresCol="features", labelCol="valor_real", seed=42),
}
```

## Fase 5 — Evaluación

Evaluación con `RegressionEvaluator` (RMSE, R², MAE) frente a la línea base ingenua. **Ejecutado contra el dataset real** (397 354 flujos):

| Modelo | RMSE | R² | MAE | ¿Supera línea base? |
|---|---|---|---|---|
| Línea base (promedio histórico) | 34 245 209.16 | — | — | — |
| LinearRegression base | 33 787 402.38 | 0.0259 | 3 159 715.73 | Sí |
| LinearRegression + Ridge (L2) | 33 787 402.37 | 0.0259 | 3 159 715.73 | Sí |
| LinearRegression + Lasso (L1) | 33 786 414.89 | 0.0260 | 3 164 290.79 | Sí |
| LinearRegression + Elastic Net | 33 786 414.90 | 0.0260 | 3 164 290.78 | Sí |
| **RandomForestRegressor (ganador)** | **16 116 512.40** | **0.7784** | **638 386.18** | Sí (-53% RMSE) |

```python
resultados_nick = {}
for nombre, pred in predicciones_nick.items():
    rmse = ev_rmse.evaluate(pred)
    resultados_nick[nombre] = rmse

nombre_ganador_nick = min(resultados_nick, key=resultados_nick.get)
modelo_ganador_nick = modelos_entrenados_nick[nombre_ganador_nick]
modelo_ganador_nick.write().overwrite().save("/opt/artifacts/nick/modelo_volumen")
```

Cumple el criterio de éxito de la Fase 1 (R² > 0.5) y queda guardado como modelo ganador.

## Cierre de fases y alcance

Cubre las 5 fases de CRISP-DM hasta modelado/evaluación. La Fase 6 (Despliegue) — poner el modelo a inferir sobre flujos en vivo — es la dimensión U2 de Nick, contenido de Unidad 2 (Spark Structured Streaming + Kafka).

## Hallazgos de la ejecución real

- **Calidad de datos:** la deduplicación por `flow_id` eliminó el 66.5% de los registros (397 354 → 133 311) — casi dos tercios de las capturas de Suricata llegan repetidas al pipeline batch.
- **Distribución del tráfico:** UDP domina en número de flujos (374 483 de 397 354, ~94%), pero TCP tiene el `bytes_per_s` promedio más alto (6.21M vs. 209K de UDP).

## Cómo ejecutar este notebook

1. Levantar el laboratorio (`docker compose up -d` desde `pyspark/`).
2. El dataset real (`TRCU.csv`) ya está en `pyspark/data/`, montado en `/opt/data/` — no requiere ajustar `RUTA_DATOS`.
3. Ejecutar de punta a punta (`Run All`): el modelo ganador se elige y se guarda automáticamente.
4. Confirmar la carpeta de salida Parquet y modelo en `pyspark/artifacts/nick/`.
