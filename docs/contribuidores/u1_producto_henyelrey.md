# Producto U1 — Henyelrey Lucio Garcia Chura

**Dimensión:** Tipo de servicio del flujo vs. catálogo IANA de puertos — U1 batch, clasificación
**Rol en el equipo:** Batch / Spark y fuentes externas
**Notebook:** [u1_producto_henyelrey.ipynb](https://github.com/Nick-Saim-MJ/Analisis_de_trafico_de_red/blob/main/pyspark/producto/u1_producto_henyelrey.ipynb)

## Fase 1 — Comprensión del negocio

- **Pregunta de negocio:** ¿Qué tipo de servicio corresponde a cada flujo según su comportamiento, comparado con la referencia oficial del catálogo IANA de puertos conocidos?
- **Objetivo de minería de datos:** entrenar un modelo de **clasificación** que prediga la categoría de servicio a partir de variables de comportamiento del flujo, sin usar el puerto.
- **Decisión que habilita:** caracterizar la composición del tráfico del campus por tipo de servicio, sin depender de que el puerto esté siempre bien declarado.
- **Criterio de éxito:** superar en 15-20 puntos porcentuales de accuracy a una línea base ingenua (predecir siempre la categoría más frecuente).

## Fase 2 — Comprensión de los datos

Mismo esquema explícito de 83 columnas. EDA sobre los 15 puertos de destino (`dst_port`) más frecuentes y las variables de comportamiento (`pkt_len_mean`, `flow_duration`, `iat_mean`, `bytes_per_s`), con conteo de nulos en columnas clave.

## Fase 3 — Preparación de los datos

- Construye un **catálogo IANA simplificado** de puertos *well-known* (ftp, ssh, dns, web, correo, bd, vpn, escritorio remoto, etc.) y lo cruza contra `dst_port` para derivar `categoria_servicio_ref` (con `otro_desconocido` para los puertos fuera del catálogo).
- Control de calidad: deduplicación por `flow_id`, descarte de nulos en la categoría de referencia.
- Escritura en **Parquet particionado por `categoria_servicio_ref`**, verificado con `PartitionFilters`.

```python
catalogo_iana = spark.createDataFrame([
    (20, "ftp"), (21, "ftp"), (22, "ssh"), (23, "telnet"), (25, "correo"),
    (53, "dns"), (67, "dhcp"), (68, "dhcp"), (80, "web"), (110, "correo"),
    (123, "ntp"), (143, "correo"), (161, "snmp"), (194, "chat"), (443, "web"),
    (445, "archivos"), (465, "correo"), (587, "correo"), (993, "correo"),
    (995, "correo"), (1433, "bd"), (1521, "bd"), (1723, "vpn"), (3306, "bd"),
    (3389, "escritorio_remoto"), (5432, "bd"), (5900, "escritorio_remoto"),
    (8080, "web"), (8443, "web"),
], ["puerto", "categoria_servicio_iana"])

df_henyelrey = (
    df.join(catalogo_iana, df.dst_port == catalogo_iana.puerto, "left")
    .withColumn(
        "categoria_servicio_ref",
        F.coalesce(F.col("categoria_servicio_iana"), F.lit("otro_desconocido"))
    )
    .drop("puerto", "categoria_servicio_iana")
)
```

**Decisión — por qué se excluye el puerto de los predictores:** `dst_port`/`src_port` quedan fuera a propósito. Usar el puerto como predictor sería trivial y circular — es, literalmente, la columna con la que se construyó la propia etiqueta (`categoria_servicio_ref`) dos líneas arriba. El objetivo de la dimensión es clasificar el servicio a partir del **comportamiento** del flujo (tamaños de paquete, tiempos, banderas), no leyendo el puerto de vuelta.

```python
predictores_henyelrey = [
    "ip_prot", "flow_duration", "pkt_len_mean", "pkt_len_std", "bytes_per_s",
    "fwd_pkt_len_mean", "bwd_pkt_len_mean", "iat_mean", "flag_SYN", "flag_ack",
    "flag_psh", "fwd_tcp_init_win_bytes", "bwd_tcp_init_win_bytes",
]  # dst_port/src_port excluidos a proposito (fuga de informacion)

indexador = StringIndexer(inputCol="categoria_servicio_ref", outputCol="label_idx")
ensamblador = VectorAssembler(inputCols=predictores_henyelrey, outputCol="features", handleInvalid="skip")
```

## Fase 4 — Modelado

Comparación de 3 configuraciones sobre `randomSplit([0.8, 0.2], seed=42)`: `LogisticRegression` base, `LogisticRegression` con regularización (Elastic Net, `regParam=0.1`), y `RandomForestClassifier`.

```python
configuraciones_henyelrey = {
    "LogisticRegression base": LogisticRegression(featuresCol="features", labelCol="label_idx"),
    "LogisticRegression + regularizacion": LogisticRegression(featuresCol="features", labelCol="label_idx", regParam=0.1, elasticNetParam=0.5),
    "RandomForestClassifier": RandomForestClassifier(featuresCol="features", labelCol="label_idx", seed=42),
}
```

## Fase 5 — Evaluación

Evaluación con `MulticlassClassificationEvaluator` (accuracy, F1, precisión ponderada) frente a la línea base de clase mayoritaria, con matriz de confusión. **Ejecutado contra el dataset real:**

| Modelo | Accuracy | F1 | Precisión | vs. línea base |
|---|---|---|---|---|
| Línea base (clase mayoritaria = `otro_desconocido`) | 0.9696 | — | — | — |
| LogisticRegression base | 0.9873 | 0.9865 | 0.9860 | +1.8 pp |
| LogisticRegression + regularización | 0.9686 | 0.9551 | 0.9457 | -0.1 pp |
| **RandomForestClassifier (ganador)** | **0.9931** | **0.9927** | **0.9924** | **+2.4 pp** |

Matriz de confusión del ganador (`label_idx`: 0=otro_desconocido, 1=web, 2=dhcp, 3=dns, 4=snmp, 5=ntp):

```text
+---------+----------+-----+
|label_idx|prediction|count|
+---------+----------+-----+
|      0.0|       0.0|25692|
|      0.0|       1.0|   23|
|      0.0|       2.0|    4|
|      1.0|       0.0|   96|
|      1.0|       1.0|  485|   <- web: 485/584 correctas (~83%)
|      1.0|       2.0|    3|
|      2.0|       0.0|    1|
|      2.0|       1.0|   41|
|      2.0|       2.0|  166|   <- dhcp: 166/208 correctas (~80%)
|      3.0|       0.0|    5|   <- dns: 0/5 (muestra insuficiente)
|      4.0|       0.0|    4|   <- snmp: 0/4 (muestra insuficiente)
|      5.0|       0.0|    5|   <- ntp: 0/5 (muestra insuficiente)
+---------+----------+-----+
```

**No cumple el criterio de éxito de la Fase 1** (objetivo +15-20 pp de accuracy) — ver hallazgos para la causa raíz (cobertura del catálogo IANA).

## Cierre de fases y alcance

Cubre las 5 fases de CRISP-DM hasta modelado/evaluación. La Fase 6 (Despliegue) — estimar el tipo de servicio de un flujo en vivo antes de que cierre — es la dimensión U2 de Henyelrey, contenido de Unidad 2 (Spark Structured Streaming + Kafka).

## Hallazgos de la ejecución real

- **Cobertura del catálogo IANA:** el catálogo simplificado (29 puertos) solo etiqueta al **1.7%** de los flujos en una categoría conocida (6 824 de 397 354: web=3 046, dhcp=2 745, dns=963, snmp=43, ntp=21, archivos=6) — el 98.3% cae en `otro_desconocido`, porque los puertos reales más frecuentes del campus (10001, 10002, 5355, 5353, 8014...) no están en el catálogo. Eso limita el margen real de mejora del modelo pese a su alta accuracy nominal.
- **Matriz de confusión:** el modelo ganador distingue razonablemente bien las clases con volumen suficiente en prueba — web: 485/584 correctas (~83%), dhcp: 166/208 correctas (~80%) — pero dns/snmp/ntp tuvieron muy pocos casos de prueba (≤5) para evaluarse con confianza.
- **Recomendación:** ampliar el catálogo IANA antes de calibrar la dimensión U2 con este modelo.

## Cómo ejecutar este notebook

1. Levantar el laboratorio (`docker compose up -d` desde `pyspark/`).
2. El dataset real (`TRCU.csv`) ya está en `pyspark/data/`, montado en `/opt/data/` — no requiere ajustar `RUTA_DATOS`.
3. Ejecutar de punta a punta (`Run All`): el modelo ganador se elige y se guarda automáticamente.
4. Confirmar la carpeta de salida Parquet y modelo en `pyspark/artifacts/henyelrey/`.
