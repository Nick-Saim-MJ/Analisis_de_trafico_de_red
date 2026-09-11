# Producto U1 — Jhan Logan Ramos Quispe

**Dimensión:** Duración del flujo de red (`flow_duration`) — U1 batch, regresión
**Rol en el equipo:** Streaming / Kafka
**Notebook:** [u1_producto_jhan.ipynb](../../pyspark/producto/u1_producto_jhan.ipynb)

## Fase 1 — Comprensión del negocio

- **Pregunta de negocio:** ¿Qué duración histórica (`flow_duration`) han tenido los flujos según sus características iniciales, y qué duración se puede esperar?
- **Objetivo de minería de datos:** entrenar un modelo de **regresión** que prediga `flow_duration` a partir de variables disponibles temprano en el ciclo de vida del flujo.
- **Decisión que habilita:** dimensionar ventanas de sesión y tiempos de retención en Kafka (Unidad 2) según la duración esperada.
- **Criterio de éxito:** reducción de al menos 20% de RMSE frente a una línea base ingenua (mediana histórica de `flow_duration`).

## Fase 2 — Comprensión de los datos

Mismo esquema explícito de 83 columnas que el resto del equipo. EDA sobre `flow_duration`, `iat_mean`, `fwd_iat_mean`, `bwd_iat_mean`: estadísticos descriptivos, nulos en columnas clave, percentiles de `flow_duration` (min/p25/mediana/p75/max) y la línea base ingenua (mediana histórica). Distribución de flujos por protocolo.

## Fase 3 — Preparación de los datos

- Deriva `protocolo` (TCP/UDP/OTRO); filtra `flow_duration` no nulo y mayor a cero.
- Agrega duración promedio y mediana (`percentile_approx`) por protocolo.
- Control de calidad: deduplicación por `flow_id`, relleno de nulos en columnas de tiempos y conteos de paquetes.
- Escritura en **Parquet particionado por `protocolo`**, verificado con `PartitionFilters`.

```python
df_jhan = (
    df
    .withColumn(
        "protocolo",
        F.when(F.col("ip_prot") == 6, F.lit("TCP"))
         .when(F.col("ip_prot") == 17, F.lit("UDP"))
         .otherwise(F.lit("OTRO"))
    )
    .filter(F.col("flow_duration").isNotNull() & (F.col("flow_duration") > 0))
)

df_dedup = df_jhan.dropDuplicates(["flow_id"])
df_limpio = df_dedup.na.fill({
    "flow_duration": 0.0, "iat_mean": 0.0, "fwd_iat_mean": 0.0,
    "bwd_iat_mean": 0.0, "fwd_pkt_cnt": 0, "bwd_pkt_cnt": 0,
})

df_limpio.write.mode("overwrite").partitionBy("protocolo") \
    .parquet("/opt/artifacts/jhan/flujos_particionado")
```

**Decisión — features seleccionadas:** `ip_prot`, `iat_mean`, `iat_std`, `fwd_iat_mean`, `bwd_iat_mean`, `fwd_pkt_cnt`, `bwd_pkt_cnt`, `pkt_len_mean`, `flag_SYN`, `flag_ack`, `active_mean`, `idle_mean`, `fwd_tcp_init_win_bytes`, `bwd_tcp_init_win_bytes` — todas disponibles temprano en el ciclo de vida del flujo, coherente con el objetivo de estimar la duración antes de que el flujo cierre (relevante para la dimensión U2).

```python
predictores_jhan = [
    "ip_prot", "iat_mean", "iat_std", "fwd_iat_mean", "bwd_iat_mean",
    "fwd_pkt_cnt", "bwd_pkt_cnt", "pkt_len_mean", "flag_SYN", "flag_ack",
    "active_mean", "idle_mean", "fwd_tcp_init_win_bytes", "bwd_tcp_init_win_bytes",
]
ensamblador = VectorAssembler(inputCols=predictores_jhan, outputCol="features", handleInvalid="skip")
```

## Fase 4 — Modelado

Comparación de 3 configuraciones sobre `randomSplit([0.8, 0.2], seed=42)`: `LinearRegression` base, `LinearRegression` con regularización (Elastic Net, `regParam=0.1`), y `RandomForestRegressor`.

```python
configuraciones_jhan = {
    "LinearRegression base": LinearRegression(featuresCol="features", labelCol="valor_real"),
    "LinearRegression + regularizacion": LinearRegression(featuresCol="features", labelCol="valor_real", regParam=0.1, elasticNetParam=0.5),
    "RandomForestRegressor": RandomForestRegressor(featuresCol="features", labelCol="valor_real", seed=42),
}
```

## Fase 5 — Evaluación

Evaluación con `RegressionEvaluator` (RMSE, R², MAE) frente a la línea base ingenua (mediana). **Ejecutado contra el dataset real:**

| Modelo | RMSE | R² | MAE | Reducción vs. base |
|---|---|---|---|---|
| Línea base (mediana histórica = 0) | 24 653 396.58 | — | — | — |
| LinearRegression base | 23 136 259.74 | 0.0054 | 4 342 154.17 | 6.2% |
| LinearRegression + regularización | 23 280 744.85 | -0.0070 | 4 355 295.43 | 5.6% |
| **RandomForestRegressor (ganador)** | **7 387 473.02** | **0.8986** | **2 332 560.44** | **70.0%** |

```python
resultados_jhan = {}
for nombre, pred in predicciones_jhan.items():
    rmse = ev_rmse.evaluate(pred)
    resultados_jhan[nombre] = rmse

nombre_ganador_jhan = min(resultados_jhan, key=resultados_jhan.get)
modelo_ganador_jhan = modelos_entrenados_jhan[nombre_ganador_jhan]
modelo_ganador_jhan.write().overwrite().save("/opt/artifacts/jhan/modelo_duracion")
```

Supera ampliamente el criterio de éxito de la Fase 1 (≥20% de reducción de RMSE) y queda guardado como modelo ganador.

## Cierre de fases y alcance

Cubre las 5 fases de CRISP-DM hasta modelado/evaluación. La Fase 6 (Despliegue) — estimar la duración de un flujo recién iniciado en vivo — es la dimensión U2 de Jhan, contenido de Unidad 2 (Spark Structured Streaming + Kafka).

## Hallazgos de la ejecución real

- **Calidad de datos:** tras filtrar `flow_duration > 0` y deduplicar, solo quedan 39 728 de 397 354 flujos (~10%) — más de la mitad de las capturas son instantáneas (duración ≈ 0), por eso la mediana histórica completa es 0.
- **Por protocolo:** TCP tiene duración mediana mucho menor (228 220 µs ≈ 0.23s, n=15 563) que UDP (9 211 691 µs ≈ 9.2s, n=87 203) — señal directa para dimensionar ventanas de sesión por protocolo en Kafka (U2).

## Cómo ejecutar este notebook

1. Levantar el laboratorio (`docker compose up -d` desde `pyspark/`).
2. El dataset real (`TRCU.csv`) ya está en `pyspark/data/`, montado en `/opt/data/` — no requiere ajustar `RUTA_DATOS`.
3. Ejecutar de punta a punta (`Run All`): el modelo ganador se elige y se guarda automáticamente.
4. Confirmar la carpeta de salida Parquet y modelo en `pyspark/artifacts/jhan/`.
