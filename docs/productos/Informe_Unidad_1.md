# Informe — Unidad 1

**Producto U1:** pipeline batch de ETL distribuido con salidas analíticas en Parquet, listas para BI/ML, sobre el dataset propio de tráfico de red del campus (equipo LLSW3, sección GU).

## 1. Resumen ejecutivo

El equipo construyó la capa **batch** de una arquitectura **Lambda** para caracterizar el tráfico de red del campus universitario UPeU Juliaca, a partir de un dataset propio de ~400 000 flujos de red (83 columnas, estilo NetFlow/CICFlowMeter) capturado con **Suricata**. Cada integrante entrega un notebook PySpark independiente, aplicando **CRISP-DM** (fases 1 a 5) sobre su propia dimensión de la pregunta central del [Brief técnico-analítico](../proyecto-sello/brief.md):

> ¿Cómo caracterizar el comportamiento de los flujos de tráfico de red del campus — volumen, duración, tipo de servicio y dirección dominante — para anticipar la carga esperada de la red y detectar patrones que se aparten de lo habitual, apoyando la gestión de capacidad y la vigilancia del equipo de TI?

Los cuatro notebooks comparten el mismo dataset de entrada, el mismo esquema explícito y la misma disciplina de calidad de datos y particionamiento analítico (Parquet), y sus salidas convergen en un único conjunto de indicadores del tráfico del campus — la base sobre la que, en Unidad 2, se despliega la capa de velocidad (Kafka + Spark Structured Streaming).

## 2. Arquitectura Big Data

**Lambda**, justificada en el brief por la necesidad de reprocesar el histórico completo cada vez que se ajusta el criterio de etiquetado de la columna `label` (hoy `NeedLabel` en todos los registros) — una validación diferida por lotes, no dato por dato, que Kappa no resuelve igual de bien.

```mermaid
flowchart TB
    S[Suricata\ncaptura de flujos del campus] --> H[Histórico de flujos\n~400k registros, 83 columnas]
    S -.U2 - Unidad 2.-> K[Kafka\nflujos en vivo]
    H --> B["Capa batch (este producto)\n4 notebooks PySpark / CRISP-DM"]
    B --> P[(Parquet particionado\npor dimensión)]
    P --> M[Modelos MLlib\nregresión y clasificación]
    K -.U2 - Unidad 2.-> ST[Spark Structured Streaming]
    ST -.U2 - Unidad 2.-> G[Grafana\ntablero único]
    M -.calibra.-> ST
```

## 3. Fuente de datos

- **Batch:** histórico de flujos de red del campus (~400 000 registros, 83 columnas), capturado por el pipeline Suricata del equipo. Incluye identificador de flujo, IP/puerto origen y destino, protocolo IP, timestamp, duración, estadísticas de tamaño y tasa de paquetes (fwd/bwd), tiempos entre paquetes (IAT), banderas TCP, periodos activos/inactivos, transferencias en ráfaga, subflujos, ventana TCP inicial y la columna `label` (pendiente de asignación).
- **Batch externa:** catálogo IANA de puertos conocidos (*well-known ports*), usado por la dimensión de tipo de servicio para derivar una etiqueta de referencia a partir de `dst_port`.
- **Streaming (Unidad 2):** los mismos flujos, publicados por Suricata vía tópicos Kafka a medida que se cierran o actualizan.

## 4. Metodología

Los cuatro notebooks aplican **CRISP-DM** de forma consistente — ver el detalle completo en [Aplicación de CRISP-DM al proyecto](Aplicacion_de_CRISPDM_al_Proyecto.md). En resumen: comprensión del negocio (pregunta propia + criterio de éxito frente a una línea base ingenua) → comprensión de los datos (EDA sobre el esquema explícito de 83 columnas) → preparación (derivación de la variable de análisis, control de calidad, particionamiento Parquet, selección de features sin fuga de información) → modelado (comparación de 3 configuraciones por dimensión) → evaluación (métricas frente a la línea base y elección del modelo ganador).

## 5. Resultados por dimensión

Los cuatro notebooks fueron ejecutados de punta a punta contra el dataset real (`TRCU.csv`, 397 354 flujos):

| Integrante | Dimensión | Modelo ganador | Métrica principal | vs. línea base | ¿Cumple criterio de éxito? | Notebook |
|---|---|---|---|---|---|---|
| Nick | Volumen de tráfico (`bytes_per_s`) | RandomForestRegressor | R²=0.7784, RMSE=16.1M | RMSE -53% | Sí (R²>0.5) | [u1_producto_nick.ipynb](https://github.com/Nick-Saim-MJ/Analisis_de_trafico_de_red/blob/main/pyspark/producto/u1_producto_nick.ipynb) |
| Jhan | Duración del flujo (`flow_duration`) | RandomForestRegressor | R²=0.8986, RMSE=7.39M | RMSE -70.0% | Sí (≥20%) | [u1_producto_jhan.ipynb](https://github.com/Nick-Saim-MJ/Analisis_de_trafico_de_red/blob/main/pyspark/producto/u1_producto_jhan.ipynb) |
| Henyelrey | Tipo de servicio vs. IANA | RandomForestClassifier | Accuracy=0.9931, F1=0.9927 | +2.4 pp | **No** (objetivo +15-20 pp) | [u1_producto_henyelrey.ipynb](https://github.com/Nick-Saim-MJ/Analisis_de_trafico_de_red/blob/main/pyspark/producto/u1_producto_henyelrey.ipynb) |
| David | Dirección dominante (`down_up_ratio`) | RandomForestClassifier | Accuracy=0.9978, F1=0.9974 | +0.20 pp | Trivialmente sí (>50%), pero ver hallazgo | [u1_producto_david.ipynb](https://github.com/Nick-Saim-MJ/Analisis_de_trafico_de_red/blob/main/pyspark/producto/u1_producto_david.ipynb) |

`RandomForestRegressor`/`RandomForestClassifier` ganó las 4 comparaciones frente a `LinearRegression`/`LogisticRegression` (con y sin regularización) — señal consistente de que las relaciones entre features de flujo y las 4 variables objetivo son fuertemente no lineales. El detalle de cada dimensión (pregunta de negocio, glosario, EDA, features y hallazgos completos) está documentado en su propia [página de contribución individual](../index.md#contenido-del-sitio).

**Notebook consolidado:** [u1_producto_consolidado.ipynb](https://github.com/Nick-Saim-MJ/Analisis_de_trafico_de_red/blob/main/pyspark/producto/u1_producto_consolidado.ipynb) reúne las 4 dimensiones en un solo documento ejecutable (una sola extracción del dataset, un bloque Fase 3→4→5 por integrante), para leer el proceso CRISP-DM completo del equipo de punta a punta. Ejecutado contra `TRCU.csv` y verificado bit a bit contra los 4 notebooks individuales — mismos resultados.

**Hallazgos de calidad de datos que afectan la interpretación de resultados:**

- **Duplicados:** entre 66.5% (Nick) y 90% (Jhan, tras filtrar duraciones > 0) de los flujos capturados se eliminan al deduplicar por `flow_id` — señal a revisar en la fuente Suricata antes de escalar a Unidad 2.
- **Catálogo IANA insuficiente (Henyelrey):** el catálogo simplificado solo etiqueta al 1.7% de los flujos en una categoría de servicio conocida; el 98.3% cae en `otro_desconocido`, lo que limita el margen real de mejora del modelo pese a su alta accuracy nominal.
- **Distribución degenerada de `down_up_ratio` (David):** el diseño de 3 categorías por cuantiles (33/33/33) colapsa en la práctica a 2, porque `down_up_ratio` está concentrado en 0 (p33=p66=0.0) — `balanceado` termina siendo el 99.85% de los flujos y `carga_dominante` queda vacía. La categorización debe rediseñarse antes de usarse como base de U2.

## 6. Calidad de datos y particionamiento analítico

Los cuatro notebooks aplican el mismo patrón, heredado de S03: deduplicación por `flow_id`, relleno o descarte de nulos en las columnas clave de cada dimensión, y escritura en **Parquet particionado** (`partitionBy` sobre `protocolo`, `categoria_servicio_ref` o `categoria_direccion` según el caso), verificado releyendo el Parquet y confirmando `PartitionFilters` en el plan de ejecución (`.explain(True)`).

## 7. Contribución por integrante

- **Nick:**

**Evidencia. Volumen histórico de tráfico:**
![Volumen histórico de tráfico:](img/nick/Comprension_de_los_datos.png)
![Volumen histórico de tráfico:](img/nick/EDA.png)
![Volumen histórico de tráfico:](img/nick/Preparacion.png)
![Volumen histórico de tráfico:](img/nick/Calidad_de_Datos.png)
![Volumen histórico de tráfico:](img/nick/Ensamblado.png)
![Volumen histórico de tráfico:](img/nick/Modelado.png)
![Volumen histórico de tráfico:](img/nick/Evaluacion.png)
![Volumen histórico de tráfico:](img/nick/git_push.png)
![Volumen histórico de tráfico:](img/nick/repositorio.png)

- **Jhan:**

**Evidencia. Duración del flujo de red:**
![Duración del flujo de red:](img/jhan/Screenshot_2026-09-10_231701.png)
![Duración del flujo de red:](img/jhan/Screenshot_2026-09-10_231815.png)
![Duración del flujo de red:](img/jhan/Screenshot_2026-09-10_232254.png)
![Duración del flujo de red:](img/jhan/Screenshot_2026-09-10_232429.png)
![Duración del flujo de red:](img/jhan/Screenshot_2026-09-10_232438.png)
![Duración del flujo de red:](img/jhan/Screenshot_2026-09-10_232446.png)
![Duración del flujo de red:](img/jhan/Screenshot_2026-09-10_232453.png)

- **Henyelrey:**

**Evidencia. Clasificacion de flujos con spark:**
![Clasificacion de flujos con spark:](img/henyelrey/Calidad_de_datos_y_particionamiento_analitico.png)
![Clasificacion de flujos con spark:](img/henyelrey/Exploracion_EDA.png)
![Clasificacion de flujos con spark:](img/henyelrey/Selección_de_predictores_y_ensamblado_del_vector_de_features.png)
![Clasificacion de flujos con spark:](img/henyelrey/Transformacion_y_Agregacion.png)
![Clasificacion de flujos con spark:](img/henyelrey/Transormacion_salida.png)
![Clasificacion de flujos con spark:](img/henyelrey/Fase_4_modelado.png)
![Clasificacion de flujos con spark:](img/henyelrey/Fase_5_evaluacion.png)



- **David:**
**Dirección dominante:**
![Dirección dominante:](img/david/EDA.png)
![Dirección dominante:](img/david/PreparacionDeDatos.png)
![Dirección dominante:](img/david/CalidadDeDatos.png)
![Dirección dominante:](img/david/ExtracciónConEsquemaExplícito.png)
![Dirección dominante:](img/david/Modelado.png)
![Dirección dominante:](img/david/Evaluacion.png)


## 8. Limitaciones y pendientes

- La columna `label` del dataset está sin asignar (`NeedLabel`) en el corte actual — la validación definitiva de etiquetas de seguridad es batch, no en vivo, y queda fuera del alcance de U1 (ver el brief, sección "fuera de alcance").
- La dimensión de tipo de servicio (Henyelrey) no cumple su criterio de éxito de la Fase 1 tal como está definida: el catálogo IANA simplificado necesita ampliarse para que la clasificación aporte valor más allá de predecir la clase mayoritaria.
- La dimensión de dirección dominante (David) necesita rediseñar su regla de categorización: el supuesto de 3 categorías balanceadas por cuantiles no se sostiene frente a la distribución real de `down_up_ratio` (fuertemente concentrada en 0).
- La tasa de duplicados en el histórico (66-90% según la dimensión) debe investigarse con el pipeline de captura de Suricata antes de escalar a la ingesta en vivo (Unidad 2), para no arrastrar el mismo problema al streaming.
- La ruta batch de este producto es la base de entrenamiento para las cuatro dimensiones U2 (streaming) declaradas en el brief; el despliegue en Spark Structured Streaming y el tablero Grafana común son contenido de Unidad 2.

## 9. Próximos pasos hacia Unidad 2

1. Ampliar el catálogo IANA de la dimensión de Henyelrey y rediseñar la categorización de dirección dominante de David, antes de calibrar sus modelos U2 con estas bases.
2. Investigar la causa de la alta tasa de duplicados por `flow_id` en la captura de Suricata.
3. Levantar la capa de velocidad (Kafka + Spark Structured Streaming) sobre el mismo esquema de flujos, calibrando cada modelo U2 con el modelo/línea base entrenado en esta unidad.
4. Integrar los cuatro paneles U2 en un único tablero Grafana de "salud y comportamiento del tráfico del campus", con alertas por umbral en las dimensiones que lo requieren (volumen, dirección dominante).
