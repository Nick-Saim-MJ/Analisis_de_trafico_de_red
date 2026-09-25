# S12 - Evaluación de la Unidad II

## 1. Propósito de la evaluación

Esta sesión no enseña contenido nuevo: cierra la Unidad II de **Big Data**. El sílabo (sesión 12) define dos actividades para esta evaluación:

1. Resolver la evaluación teórico-práctica de los temas de la Unidad II (sesiones 6 a 11).
2. Presentar y sustentar el Pipeline en tiempo real con ingesta, streaming, observabilidad y salidas BI/ML distribuidas.

**Esta sesión no repite lo evaluado en S5**: la Unidad I (arquitectura, ETL batch, calidad, particionamiento y primer modelo) ya quedó certificada; S12 evalúa exclusivamente lo que se construyó encima en S6-S11.

## 2. Producto evaluado

Del sílabo, el producto de la Unidad II es:

> Pipeline Big Data en tiempo real que procesa eventos, produce resultados analíticos, expone métricas operativas y prepara resultados para BI/ML distribuido.

El producto completo — plantilla-ejemplo con el ejemplo de referencia del docente — vive en [`u2-producto.md`](../proyecto-sello/u2-producto.md): ingesta de eventos empresariales e IoT, procesamiento streaming, observabilidad y costos, series de tiempo, BI/ML distribuido. La estructura es exigible a todos; el contenido se reemplaza por el del propio instrumento de cada equipo.

Lo que sustentas en S12 es **tu propia dimensión U2**: las capacidades que tú construiste en S6-S11, sobre el instrumento de tu Proyecto Sello. `u2-producto.md` (Tabla 1) muestra cómo cada sesión se ensambla en el pipeline terminado.

## 3. Evaluación teórico-práctica (S6-S11)

Cubre los seis temas dictados antes de esta sesión. El docente puede tomarla escrita, oral o mixta.

**Tabla 1. Temario de la evaluación teórico-práctica**

| Sesión | Tema | Qué puede evaluar el docente |
|---|---|---|
| S6 | Ingesta de eventos empresariales en tiempo real | Publicación y consumo con Kafka, contrato de evento, tópico, particionado. |
| S7 | Ingesta de eventos IoT/sensores en tiempo real | Esquema de evento, frecuencia, volumen y validaciones de datos para telemetría o sensores. |
| S8 | Procesamiento en streaming con Spark: ventanas, watermarking y semántica de entrega | Agregaciones en ventana, tratamiento de datos tardíos con *watermarking*, *checkpointing* y su efecto en latencia y throughput. |
| S9 | Observabilidad con Grafana y costos | Métricas, tablero de observabilidad, umbrales, riesgos operativos y estimación de costos de escalado. |
| S10 | Series de tiempo e inferencia en streaming | Modelo o inferencia de series de tiempo sobre datos batch y/o streaming, comparación de resultados y supuestos. |
| S11 | BI/ML distribuido con Spark: KPIs y visualización de la predicción | Preparación de KPIs del flujo de eventos, relación entre la predicción de S10 y su visualización en el tablero BI. |

Preguntas de referencia (el docente puede formular equivalentes):

1. ¿Qué diferencia hay entre el contrato de un evento empresarial y uno de un sensor IoT, y por qué esa diferencia importa al diseñar el tópico de Kafka?
2. Si un evento llega tarde a tu ventana de agregación, ¿qué hace el *watermarking* con él, y qué pasaría si no lo tuvieras configurado?
3. ¿Qué métrica de tu panel de observabilidad te alertaría primero si el pipeline empieza a acumular retraso (*backpressure*), y por qué esa y no otra?
4. ¿Por qué estimar el costo de escalar tu pipeline streaming no es lo mismo que estimar el costo de tu pipeline batch de Unidad 1?
5. ¿Qué gana tu tablero BI al mostrar la predicción de series de tiempo junto a los KPIs del negocio, en vez de mostrarlos por separado?

## 4. Sustentación de tu dimensión U2

**Tabla 2. Distribución de tiempo por integrante**

| Momento | Tiempo | Propósito |
|---|---:|---|
| Presentación técnica | 8 min | Explicar tu dimensión U2 (sección 2), las decisiones tomadas y su evolución desde S5. |
| Demo técnica | 8 min | Ejecutar en vivo la publicación/consumo de eventos, el procesamiento streaming, el panel de observabilidad y el tablero BI. |
| Preguntas individuales | 5 min | Verificar dominio y aporte propio, con base en la Tabla 1. |

**Tabla 3. Entregables obligatorios**

| Entregable | Evidencia mínima | Criterio de aceptación |
|---|---|---|
| Producto de tu dimensión U2 | [`u2-producto.md`](../proyecto-sello/u2-producto.md), con tu propio instrumento | Coherente con el sílabo y con el pipeline real ejecutable |
| Evidencia de ingesta y streaming | Eventos empresariales e IoT publicados/consumidos en vivo; procesamiento con ventana y *watermarking* verificado | Trazabilidad verificable en logs, no solo documentada |
| Evidencia de observabilidad y costos | Panel operativo con métricas correlacionadas; estimación de costos justificada | Verificable en vivo, no solo descrita |
| Evidencia de BI/ML | Serie de tiempo o inferencia validada, integrada al tablero BI con KPIs | Métricas reales reportadas |
| Sustentación individual | Preguntas y defensa por integrante (sección 3) | Autoría demostrada |

Secuencia sugerida de presentación (referencias a secciones de `u2-producto.md`):

1. Presentar cómo evolucionó tu pipeline desde la Unidad I.
2. Publicar y consumir en vivo un evento empresarial y uno IoT/sensor.
3. Mostrar el procesamiento streaming con ventana, *watermarking* y *checkpointing* activos.
4. Mostrar el panel de observabilidad y la estimación de costos de escalado.
5. Mostrar el modelo o inferencia de series de tiempo y sus métricas de validación.
6. Mostrar el tablero BI con los KPIs y la predicción integrados.
7. Cerrar explicando al menos una decisión propia distinta al ejemplo del docente.

Criterios mínimos de aceptación:

- Al menos un evento empresarial y uno IoT/sensor se publican y consumen en vivo por Kafka.
- El procesamiento streaming aplica ventana, *watermarking* y *checkpointing*, verificable en logs.
- El panel de observabilidad correlaciona métricas reales del pipeline, con costos de escalado estimados y justificados.
- El modelo o inferencia de series de tiempo está validado con métricas, no solo entrenado.
- El tablero BI muestra la predicción junto a los KPIs del negocio, no por separado.
- Respondes individualmente al menos una pregunta de la Tabla 1.

## 5. Rúbrica de evaluación

La rúbrica (8 criterios: 7 cita literal de los criterios de evaluación del producto de la Unidad II en el sílabo de Big Data + sustentación) vive en [`u2-producto.md`](../proyecto-sello/u2-producto.md#2-rubrica-de-evaluacion), junto con la plantilla del producto y su trazabilidad con la malla curricular (segunda mitad de CE043 Nivel 3 + CE044 Nivel 3 completo). Úsala directamente desde ahí para calificar la sustentación de esta sesión — no se duplica aquí.
