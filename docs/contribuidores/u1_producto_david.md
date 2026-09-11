# Producto U1 — David Romero Nina

**Dimensión:** Dirección dominante del flujo (`down_up_ratio`) — U1 batch, clasificación (ángulo BI)
**Rol en el equipo:** BI / ML
**Notebook:** [u1_producto_david.ipynb](../../pyspark/producto/u1_producto_david.ipynb)

## Fase 1 — Comprensión del negocio

- **Pregunta de negocio:** ¿Qué proporción de los flujos históricos son de descarga dominante, carga dominante o balanceados, y qué patrón se puede esperar?
- **Objetivo de minería de datos:** entrenar un modelo de **clasificación** (3 categorías) que prediga la dirección dominante del flujo a partir de variables de comportamiento (protocolo, tiempos, banderas).
- **Decisión que habilita:** priorizar ancho de banda saliente vs. entrante y detectar cambios inusuales en el patrón de uso del campus.
- **Criterio de éxito:** accuracy > 50%, frente a una línea base ingenua de ~33-34% (clase mayoritaria, por construcción de las categorías en cuantiles 33/33/33).

## Fase 2 — Comprensión de los datos

Mismo esquema explícito de 83 columnas. EDA sobre `down_up_ratio`: estadísticos descriptivos, percentiles (min/p33/mediana/p66/max), conteo de nulos y distribución de flujos por protocolo.

## Fase 3 — Preparación de los datos

- Calcula los cortes de cuantil (p33, p66) sobre el histórico real y deriva `categoria_direccion` (`carga_dominante` / `balanceado` / `descarga_dominante`).
- Control de calidad: deduplicación por `flow_id`, descarte de nulos en `down_up_ratio` y en la categoría derivada.
- Escritura en **Parquet particionado por `categoria_direccion`**, verificado con `PartitionFilters`.

```python
cuantiles = df.approxQuantile("down_up_ratio", [0.33, 0.66], 0.01)
p33, p66 = cuantiles[0], cuantiles[1]  # en la ejecucion real: p33 = p66 = 0.0 (ver hallazgos)

df_david = df.withColumn(
    "categoria_direccion",
    F.when(F.col("down_up_ratio") < p33, F.lit("carga_dominante"))
     .when(F.col("down_up_ratio") > p66, F.lit("descarga_dominante"))
     .otherwise(F.lit("balanceado"))
)
```

**Decisión — por qué se excluyen las columnas de tamaño/bytes de los predictores:** `pkt_len_*`, `*_pkt_len_tot`, `bytes_per_s` y `*_bulk_*` quedan fuera a propósito, porque `down_up_ratio` se deriva directamente de ellas — usarlas como predictor sería, en la práctica, la respuesta disfrazada (fuga de información). Los predictores se limitan a protocolo, tiempos y banderas: comportamiento, no volumen.

```python
predictores_david = [
    "ip_prot", "flow_duration", "iat_mean", "iat_std", "fwd_iat_mean", "bwd_iat_mean",
    "flag_SYN", "flag_ack", "flag_psh", "active_mean", "idle_mean",
    "fwd_tcp_init_win_bytes", "bwd_tcp_init_win_bytes",
]  # columnas de tamano/bytes excluidas a proposito (fuga de informacion)

indexador = StringIndexer(inputCol="categoria_direccion", outputCol="label_idx")
ensamblador = VectorAssembler(inputCols=predictores_david, outputCol="features", handleInvalid="skip")
```

## Fase 4 — Modelado

Comparación de 3 configuraciones sobre `randomSplit([0.8, 0.2], seed=42)`: `LogisticRegression` base, `LogisticRegression` con regularización (Elastic Net, `regParam=0.1`), y `RandomForestClassifier`.

```python
configuraciones_david = {
    "LogisticRegression base": LogisticRegression(featuresCol="features", labelCol="label_idx"),
    "LogisticRegression + regularizacion": LogisticRegression(featuresCol="features", labelCol="label_idx", regParam=0.1, elasticNetParam=0.5),
    "RandomForestClassifier": RandomForestClassifier(featuresCol="features", labelCol="label_idx", seed=42),
}
```

## Fase 5 — Evaluación

Evaluación con `MulticlassClassificationEvaluator` (accuracy, F1, precisión ponderada) frente a la línea base de clase mayoritaria, con matriz de confusión. **Ejecutado contra el dataset real:**

| Modelo | Accuracy | F1 | Precisión |
|---|---|---|---|
| Línea base (clase mayoritaria = `balanceado`) | 0.9958 | — | — |
| LogisticRegression base | 0.9977 | 0.9973 | 0.9977 |
| LogisticRegression + regularización | 0.9958 | 0.9938 | 0.9917 |
| **RandomForestClassifier (ganador)** | **0.9978** | **0.9974** | **0.9978** |

Matriz de confusión del ganador (`label_idx`: 0=balanceado, 1=descarga_dominante; `carga_dominante` no aparece — ver hallazgos):

```text
+---------+----------+-----+
|label_idx|prediction|count|
+---------+----------+-----+
|      0.0|       0.0|26384|
|      1.0|       0.0|   59|
|      1.0|       1.0|   51|   <- descarga_dominante: 51/110 correctas (~46%)
+---------+----------+-----+
```

El accuracy alto (+0.20 pp vs. línea base) es engañoso — ver hallazgos: el supuesto de 3 categorías balanceadas no se cumplió con datos reales, y la matriz de confusión es la evidencia real de cuánto valor aporta el modelo.

## Cierre de fases y alcance

Cubre las 5 fases de CRISP-DM hasta modelado/evaluación. La Fase 6 (Despliegue) — proyectar la dirección dominante de un flujo en curso — es la dimensión U2 de David, contenido de Unidad 2 (Spark Structured Streaming + Kafka).

## Hallazgos de la ejecución real

- **El supuesto de cuantiles 33/33/33 no se cumple:** `down_up_ratio` está concentrado en 0 (p25=mediana=p66=0.0), así que los cortes de cuantil dan **p33 = p66 = 0.0**. Eso colapsa las 3 categorías a 2 en la práctica: `balanceado` (down_up_ratio == 0) es el **99.85%** de los flujos (396 747 de 397 354) y `descarga_dominante` solo el 0.15% (607); `carga_dominante` queda **vacía**.
- **La matriz de confusión es más informativa que el accuracy:** de los 110 flujos reales de `descarga_dominante` en prueba, el modelo identificó 51 (~46%) — mejor que azar, pero lejos de ser confiable tal como está planteada la categorización.
- **Recomendación:** separar `down_up_ratio == 0` como categoría propia (en vez de fusionarla con "balanceado") o recalibrar los cortes solo sobre la subpoblación con `down_up_ratio > 0`, antes de usar esta dimensión como base de U2.

## Cómo ejecutar este notebook

1. Levantar el laboratorio (`docker compose up -d` desde `pyspark/`).
2. El dataset real (`TRCU.csv`) ya está en `pyspark/data/`, montado en `/opt/data/` — no requiere ajustar `RUTA_DATOS`.
3. Ejecutar de punta a punta (`Run All`): el modelo ganador se elige y se guarda automáticamente.
4. Confirmar la carpeta de salida Parquet y modelo en `pyspark/artifacts/david/`.
