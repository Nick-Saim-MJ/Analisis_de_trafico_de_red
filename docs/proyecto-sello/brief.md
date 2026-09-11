# Brief Técnico-Analítico del Proyecto Sello

Hito **S2** — declaración del sistema Big Data del equipo (arquitectura Lambda). Curso Big Data · `lambda26` · caso propio: análisis de tráfico de red del campus universitario (dataset propio capturado vía Suricata).

## 1. Datos del equipo

- **Nombre del equipo:** LLSW3
- **Sección:** GU
- **Repositorio (URL):** [github.com/Nick-Saim-MJ/Analisis_de_trafico_de_red](https://github.com/Nick-Saim-MJ/Analisis_de_trafico_de_red)
- **Topics del repositorio configurados:** sí — `campus-juliaca`, `semestre-2026-2`, `linea-cdia`, `tipo-ps`, `bigdata`, `seccion-gu`, `grupo-unico-trafico-red`

**Integrantes:**

| Integrante | Rol o énfasis previsto |
|---|---|
| Nick Saim Mayta Jara | Arquitectura Lambda y observabilidad general — dimensión "volumen de tráfico por flujo (bytes/s)" — coordinación técnica del equipo |
| Jhan Logan Ramos Quispe | Streaming / Kafka — dimensión "duración del flujo de red" |
| Henyelrey Lucio Garcia Chura | Batch / Spark y fuentes externas — dimensión "tipo de servicio del flujo (comportamiento vs. catálogo IANA de puertos)" |
| David Romero Nina | BI / ML — dimensión "dirección dominante del flujo (ratio descarga/carga)" |

Este rol es un énfasis de coordinación a nivel de equipo; cada integrante construye de extremo a extremo sus dos dimensiones (U1 y U2, sección 3), sin importar el rol que tenga aquí.

## 2. Dominio del proyecto

- **Nombre del proyecto:** Sistema de monitoreo Big Data del tráfico de red del campus universitario (UPeU Juliaca).
- **Problema o necesidad que resuelve:** el campus genera un volumen de tráfico de red (decenas de miles de flujos por sesión de captura, ~400 000 registros en el corte actual) que no puede revisarse manualmente. Hoy no existe una plataforma analítica que caracterice ese tráfico de forma sistemática: ni el perfil de carga por tipo de servicio, ni la duración esperada de las conexiones, ni la dirección dominante del flujo (descarga vs. carga), lo que limita la planificación de capacidad de red y la detección temprana de patrones atípicos que ameriten revisión del equipo de TI/seguridad.
- **Dominio de datos:** ciberseguridad / redes — tráfico de red en formato de flujo (flow-level, estilo NetFlow/CICFlowMeter), capturado en el campus mediante Suricata. El dataset propio tiene ~400 000 registros y ~83 columnas: identificador de flujo, IP/puerto origen y destino, protocolo IP, timestamp de inicio (microsegundos), duración del flujo, estadísticas de tamaño y tasa de paquetes (adelante/atrás — fwd/bwd), tiempos entre paquetes (IAT), banderas TCP (SYN, FIN, RST, ACK, PSH, URG, CWR, ECE), períodos de actividad/inactividad (active/idle), transferencias en ráfaga (bulk), subflujos, ventana TCP inicial (fwd/bwd) y una columna `label` aún sin asignar (`NeedLabel` en todos los registros del corte actual).
- **Pregunta central de negocio:** ¿cómo caracterizar el comportamiento de los flujos de tráfico de red del campus — volumen, duración, tipo de servicio y dirección dominante — para anticipar la carga esperada de la red y detectar patrones que se aparten de lo habitual, apoyando la gestión de capacidad y la vigilancia del equipo de TI?
- **Usuarios / actores principales:** administrador de red/TI del campus (decide sobre dimensionamiento de ancho de banda y priorización de tráfico), analista de seguridad (evalúa si un patrón detectado amerita revisión — este Proyecto Sello alimenta, sin sustituirlo, el trabajo de tesis del integrante Nick sobre un NIDS propio), docente/jurado del curso, y el sistema de alertas de Grafana como actor automatizado que centraliza los paneles del equipo.
- **Arquitectura Big Data prevista:** Lambda. Los flujos capturados por Suricata pueden publicarse casi en tiempo real (capa de velocidad, vía Kafka), pero el corpus histórico (~400 000 flujos) requiere reprocesamiento batch para control de calidad, consistencia de features y, sobre todo, para la asignación definitiva de la columna `label` — que hoy está pendiente y se validará por lotes, no dato por dato. Esa validación diferida es la misma razón de fondo que justificaba Lambda sobre Kappa en el caso anterior del equipo: los modelos U1 necesitan recomputar sobre el histórico completo cada vez que el criterio de etiquetado se ajusta, no solo sobre el log de eventos en vivo.
- **Fuente de datos batch:** el archivo histórico de flujos ya capturado (~400 000 registros, 83 columnas) por el pipeline Suricata del equipo; y, como fuente batch externa, el catálogo IANA de puertos conocidos (*well-known ports*), usado para derivar una referencia de tipo de servicio por puerto.
- **Fuente de eventos en tiempo real:** flujos de red publicados por Suricata vía tópicos Kafka a medida que se cierran o se actualizan (esquema real ya validado en S03/S04, extensible a más sensores del campus).
- **¿Continúa un proyecto de un ciclo anterior?:** no es continuación del proyecto de Big Data de otro equipo. El dominio de tráfico de red es el que este mismo equipo adoptó desde S03 de este curso (reemplazando el caso de amarre autocompensado de trucha, que sigue vigente solo en Electivo 1). El dataset es propio, capturado por el equipo en el campus, y se desarrolla en paralelo al trabajo de tesis de Nick (framework NIDS basado en Mamba-3); este Proyecto Sello usa únicamente la ruta analítica batch (regresión/clasificación clásica con Spark MLlib), sin las arquitecturas de deep learning de la tesis.

## 3. Dimensiones de análisis y fuentes previstas

Las ocho dimensiones (dos por integrante, U1 batch + U2 streaming) responden todas al mismo dataset de flujos de red y convergen en un único tablero Grafana de "salud y comportamiento del tráfico del campus", con sus modelos U1 documentados en los notebooks individuales de cada integrante.

**Tabla de asignación (resumen):**

| Integrante | Tipo | Dimensión (resumen) | Indicador (resumen) | Fuente batch | Fuente streaming |
|---|---|---|---|---|---|
| Nick | U1 batch | ¿Cuál ha sido el volumen histórico de tráfico por flujo y qué se espera? | Bytes/s promedio + proyección (regresión, ya entrenado en S04) | Histórico de flujos (~400k) | Bytes/s en vivo (acumulado) |
| Nick | U2 streaming | ¿Qué volumen de tráfico tendrá un flujo activo en los próximos minutos? | Bytes/s en vivo + proyección corto plazo | Mismo histórico (entrenamiento) | Flujos Kafka en vivo |
| Jhan | U1 batch | ¿Qué duración histórica tuvieron los flujos según sus características iniciales? | Duración promedio/mediana + proyección por protocolo/puerto (regresión) | Histórico de flujos | Duración en vivo (acumulada) |
| Jhan | U2 streaming | ¿Cuánto durará un flujo recién iniciado, según sus primeros paquetes? | Duración estimada en vivo, actualizada por paquete | Mismo histórico (entrenamiento) | Paquetes Kafka en vivo |
| Henyelrey | U1 batch | ¿Qué tipo de servicio corresponde a cada flujo según su comportamiento, frente al puerto oficial? | Categoría de servicio (clasificación) vs. etiqueta derivada del catálogo IANA | Catálogo IANA + histórico de flujos | Mismo (acumulado) |
| Henyelrey | U2 streaming | ¿Qué tipo de servicio tiene un flujo en vivo antes de que termine? | Categoría de servicio estimada en vivo | Mismo histórico (calibración) | Paquetes en vivo |
| David | U1 batch | ¿Qué proporción de flujos históricos son de descarga dominante, carga dominante o balanceados? | Categoría de dirección (down/up ratio) + distribución | Histórico de flujos | Mismo (acumulado) |
| David | U2 streaming | ¿Qué dirección dominante tendrá un flujo en curso? | Categoría de dirección estimada en vivo | Mismo histórico (entrenamiento) | Flujos Kafka en vivo |

### Fichas por dimensión

#### 1. Volumen de tráfico por flujo — histórico (U1)

- **Integrante:** Nick Saim Mayta Jara
- **Tipo:** Predictiva (U1, batch)
- **Dimensión y relación con la pregunta central:** ¿Cuál ha sido el volumen histórico de tráfico (bytes por segundo) de los flujos capturados, y qué volumen se puede esperar según sus características? Es el ángulo central de la pregunta de negocio: el volumen de tráfico es el indicador directo de carga sobre la red.
- **Indicador(es):** `bytes_per_s` promedio por segmento de tráfico; `bytes_per_s` proyectado por un modelo de regresión sobre el resto de features del flujo. (Ya ejecutado en S04: comparación `LinearRegression` + 3 configuraciones de regularización vs. `RandomForestRegressor`, con el Random Forest guardado como ganador.)
- **Decisión o acción que habilita:** anticipar picos de carga y priorizar capacidad de red antes de que ocurra congestión.
- **Instrumento/fuente batch:** histórico de flujos (~400 000 registros) con sus columnas de tamaño y tasa de paquetes (fwd/bwd).
- **Instrumento/fuente streaming:** lecturas en vivo de `bytes_per_s` del mismo origen que su dimensión U2, acumuladas aquí (no se procesan en vivo en esta dimensión).
- **Modelo predictivo:** regresión (Linear + Random Forest) sobre el histórico; corre una sola vez por corrida del pipeline batch.
- **Salida:** notebook Jupyter/PySpark: tabla de volumen por segmento, gráfico de distribución, métricas RMSE/R²/MAE de ambos modelos y el ganador persistido.
- **Se combina con:** su propia dimensión U2 y con la duración de flujo (Jhan): volumen y duración juntos explican el perfil de carga real.
- **Lista inicial de requisitos:**
    1. El sistema debe calcular `bytes_per_s` agregado por segmento (protocolo/puerto) a partir del histórico de flujos.
    2. El sistema debe entrenar y versionar un modelo de regresión que proyecte `bytes_per_s` a partir de las demás features del flujo.
    3. El sistema debe dejar la tabla, el gráfico y las métricas de comparación documentados en el notebook de la unidad U1, reproducibles al re-ejecutar el pipeline.

#### 2. Volumen de tráfico por flujo — tiempo real (U2)

- **Integrante:** Nick Saim Mayta Jara
- **Tipo:** Predictiva (U2, streaming)
- **Dimensión y relación con la pregunta central:** ¿Qué volumen de tráfico tendrá un flujo activo en los próximos minutos, según su comportamiento hasta el momento?
- **Indicador(es):** `bytes_per_s` en vivo del flujo; proyección de corto plazo actualizada con cada lectura.
- **Decisión o acción que habilita:** dispara la alerta operativa de carga anómala antes de que afecte a otros usuarios de la red.
- **Instrumento/fuente batch:** el mismo histórico de flujos, usado únicamente para entrenar el modelo de series de tiempo.
- **Instrumento/fuente streaming:** flujos publicados vía tópico Kafka por Suricata.
- **Modelo predictivo:** modelo de series de tiempo entrenado sobre el histórico y desplegado en Spark Structured Streaming.
- **Salida:** panel de KPI (bytes/s en vivo) y panel de predicción en el Grafana común, con alerta visual si se supera el umbral esperado.
- **Se combina con:** su propia dimensión U1 y con la duración de flujo en vivo (Jhan).
- **Lista inicial de requisitos:**
    1. El sistema debe ingerir los flujos vía Kafka con una latencia menor a 1 minuto desde su cierre/actualización.
    2. El sistema debe actualizar la predicción con cada nuevo flujo, sin reiniciar el modelo completo.
    3. El sistema debe disparar una alerta visual en Grafana cuando `bytes_per_s` proyectado supere el umbral definido.

#### 3. Duración del flujo de red — histórico (U1)

- **Integrante:** Jhan Logan Ramos Quispe
- **Tipo:** Predictiva (U1, batch)
- **Dimensión y relación con la pregunta central:** ¿Qué duración histórica (`flow_duration`) han tenido los flujos según sus características iniciales (protocolo, puertos, primeros paquetes), y qué duración se puede esperar? Cubre el ángulo temporal de la carga de red: conexiones muy largas o muy cortas tienen implicancias distintas de capacidad y de gestión de sesiones Kafka.
- **Indicador(es):** duración promedio y mediana de flujo por segmento (protocolo/puerto); duración proyectada por un modelo de regresión sobre features iniciales del flujo.
- **Decisión o acción que habilita:** dimensionar ventanas de sesión y tiempos de retención en Kafka según la duración esperada.
- **Instrumento/fuente batch:** histórico de flujos, columna `flow_duration` y features iniciales (protocolo, puertos, primeros tamaños de paquete).
- **Instrumento/fuente streaming:** duración acumulada en vivo del mismo origen que su dimensión U2, acumulada aquí (no se procesa en vivo en esta dimensión).
- **Modelo predictivo:** regresión sobre el histórico; corre una sola vez por corrida del pipeline batch.
- **Salida:** notebook Jupyter/PySpark: tabla de duración por segmento, gráfico de distribución y la duración proyectada del modelo incluida en la salida.
- **Se combina con:** su propia dimensión U2 y con el volumen de tráfico (Nick): duración y volumen juntos describen el perfil completo del flujo.
- **Lista inicial de requisitos:**
    1. El sistema debe calcular la duración promedio y mediana de flujo por protocolo/puerto a partir del histórico.
    2. El sistema debe entrenar un modelo de regresión que proyecte la duración esperada a partir de las features iniciales del flujo.
    3. El sistema debe documentar en el notebook los segmentos (protocolo/puerto) usados para agrupar el histórico de entrenamiento.

#### 4. Duración del flujo de red — tiempo real (U2)

- **Integrante:** Jhan Logan Ramos Quispe
- **Tipo:** Predictiva (U2, streaming)
- **Dimensión y relación con la pregunta central:** ¿Cuánto durará un flujo que se acaba de iniciar, dado sus primeros paquetes?
- **Indicador(es):** duración estimada en vivo, actualizada con cada nuevo paquete del flujo activo.
- **Decisión o acción que habilita:** permite anticipar si un flujo activo se comportará como sesión corta o larga, para ajustar la ventana de agregación en Kafka.
- **Instrumento/fuente batch:** el mismo histórico de duración, usado únicamente para entrenar el modelo de series de tiempo.
- **Instrumento/fuente streaming:** paquetes en vivo del flujo activo, publicados vía tópico Kafka.
- **Modelo predictivo:** modelo de series de tiempo entrenado sobre el histórico y desplegado en Spark Structured Streaming.
- **Salida:** panel de KPI (duración en vivo) y panel de predicción en el Grafana común.
- **Se combina con:** su propia dimensión U1 y con el volumen en vivo (Nick).
- **Lista inicial de requisitos:**
    1. El sistema debe ingerir los primeros paquetes de un flujo vía Kafka con latencia menor a 1 minuto.
    2. El sistema debe actualizar la duración estimada con cada nuevo paquete, sin reiniciar el modelo.
    3. El sistema debe distinguir visualmente en Grafana entre flujos activos de duración corta y larga proyectada.

#### 5. Tipo de servicio del flujo — histórico (U1)

- **Integrante:** Henyelrey Lucio Garcia Chura
- **Tipo:** Predictiva (U1, batch)
- **Dimensión y relación con la pregunta central:** ¿Qué tipo de servicio (web, DNS, correo, etc.) corresponde a cada flujo según su comportamiento (tamaños de paquete, tiempos, banderas), comparado con la referencia oficial del catálogo IANA de puertos conocidos? Cubre el ángulo de clasificación de servicios sin depender únicamente del número de puerto, útil cuando el puerto no es concluyente.
- **Indicador(es):** categoría de servicio predicha por un modelo de clasificación entrenado sobre variables de comportamiento del flujo (sin usar `dst_port` como predictor); etiqueta de referencia derivada del catálogo IANA a partir de `dst_port`.
- **Decisión o acción que habilita:** caracterizar la composición del tráfico del campus por tipo de servicio, sin depender de que el puerto esté siempre bien declarado.
- **Instrumento/fuente batch:** catálogo IANA de puertos conocidos (*well-known ports*), fuente batch externa y pública; histórico de flujos con sus features de comportamiento.
- **Instrumento/fuente streaming:** mismo tipo de servicio estimado en vivo, acumulado aquí (no se procesa en vivo en esta dimensión).
- **Modelo predictivo:** modelo de clasificación (regresión logística o árbol de decisión / Random Forest) entrenado sobre el histórico; corre una sola vez por corrida del pipeline batch.
- **Salida:** notebook Jupyter/PySpark: tabla de distribución de tipos de servicio, matriz de confusión frente a la referencia IANA, y métricas de clasificación (accuracy, F1) incluidas en la salida.
- **Se combina con:** su propia dimensión U2 y con la dirección dominante del flujo (David): tipo de servicio y dirección juntos afinan el perfil de uso de la red.
- **Lista inicial de requisitos:**
    1. El sistema debe integrar por lotes el catálogo IANA de puertos conocidos como fuente batch externa, y derivar de ahí una etiqueta de referencia por flujo.
    2. El sistema debe entrenar un modelo de clasificación que prediga el tipo de servicio a partir de variables de comportamiento del flujo, no del puerto.
    3. El sistema debe documentar en el notebook la matriz de confusión entre la predicción y la referencia IANA.

#### 6. Tipo de servicio del flujo — tiempo real (U2)

- **Integrante:** Henyelrey Lucio Garcia Chura
- **Tipo:** Predictiva (U2, streaming)
- **Dimensión y relación con la pregunta central:** ¿Qué tipo de servicio tiene un flujo en vivo antes de que termine, según su comportamiento parcial?
- **Indicador(es):** categoría de servicio estimada en vivo, actualizada con cada paquete adicional del flujo.
- **Decisión o acción que habilita:** habilita al equipo a tener una composición de tráfico por servicio actualizada minuto a minuto, sin esperar el cierre del flujo.
- **Instrumento/fuente batch:** el histórico y el catálogo IANA, usados para calibrar periódicamente el modelo de clasificación en vivo.
- **Instrumento/fuente streaming:** paquetes en vivo del flujo, publicados vía tópico Kafka.
- **Modelo predictivo:** modelo de clasificación desplegado en Spark Structured Streaming, aplicando inferencia sobre cada paquete que llega.
- **Salida:** panel de KPI (composición de tráfico por tipo de servicio, en vivo) en el Grafana común.
- **Se combina con:** su propia dimensión U1 y con todas las demás dimensiones U2, como variable de contexto del resto del tablero.
- **Lista inicial de requisitos:**
    1. El sistema debe estimar el tipo de servicio en vivo a partir del comportamiento del flujo, con la calibración obtenida del histórico.
    2. El sistema debe mostrar la composición de tráfico por servicio actualizada en tiempo real.
    3. El sistema debe recalibrar el modelo cada vez que se reprocese el histórico con un catálogo IANA actualizado.

#### 7. Dirección dominante del flujo — histórico (U1)

- **Integrante:** David Romero Nina
- **Tipo:** Predictiva (U1, batch)
- **Dimensión y relación con la pregunta central:** ¿Qué proporción de los flujos históricos son de descarga dominante, carga dominante o balanceados (`down_up_ratio`), y qué patrón se puede esperar? Cubre el ángulo de negocio/BI: entender si el campus consume mayormente contenido (descarga) o produce tráfico saliente (carga, backups, servidores internos).
- **Indicador(es):** categoría de dirección (descarga-dominante / carga-dominante / balanceado) derivada de `down_up_ratio`; distribución histórica por categoría y proyección por un modelo de clasificación sobre features del flujo.
- **Decisión o acción que habilita:** decidir sobre priorización de ancho de banda saliente vs. entrante y detectar cambios inusuales en el patrón de uso.
- **Instrumento/fuente batch:** histórico de flujos, columna `down_up_ratio` y features de tamaño de paquete fwd/bwd (nodo/segmento como línea base).
- **Instrumento/fuente streaming:** el mismo diferencial en vivo, acumulado aquí (no se procesa en vivo en esta dimensión).
- **Modelo predictivo:** modelo de clasificación sobre el histórico; corre una sola vez por corrida del pipeline batch.
- **Salida:** notebook Jupyter/PySpark: tabla y gráfico de la distribución de categorías de dirección por periodo, con la proyección del modelo incluida.
- **Se combina con:** su propia dimensión U2 y con el tipo de servicio (Henyelrey): explica qué parte de la dirección dominante se debe al tipo de servicio y no solo al volumen.
- **Lista inicial de requisitos:**
    1. El sistema debe calcular la categoría de dirección (`down_up_ratio` discretizado) por flujo a partir del histórico.
    2. El sistema debe comparar la distribución de categorías por segmento (protocolo/puerto) en el mismo periodo.
    3. El sistema debe entrenar un modelo de clasificación que prediga la categoría de dirección esperada.

#### 8. Dirección dominante del flujo — tiempo real (U2)

- **Integrante:** David Romero Nina
- **Tipo:** Predictiva (U2, streaming)
- **Dimensión y relación con la pregunta central:** ¿Qué dirección dominante tendrá un flujo en curso, según su comportamiento hasta el momento?
- **Indicador(es):** categoría de dirección proyectada en vivo para el flujo activo.
- **Decisión o acción que habilita:** alerta al equipo si un flujo se desvía del patrón esperado para su tipo de servicio (posible comportamiento atípico), habilitando revisión.
- **Instrumento/fuente batch:** el mismo histórico de categorías de dirección, usado para entrenar el modelo de series de tiempo.
- **Instrumento/fuente streaming:** lecturas en vivo de tamaño de paquete fwd/bwd del flujo activo, publicadas vía tópico Kafka.
- **Modelo predictivo:** modelo de series de tiempo/clasificación entrenado sobre el histórico y desplegado en Spark Structured Streaming.
- **Salida:** panel de KPI (dirección dominante en vivo) y panel de predicción en el Grafana común, con alerta si la dirección se desvía del patrón esperado.
- **Se combina con:** su propia dimensión U1 y con el tipo de servicio en vivo (Henyelrey).
- **Lista inicial de requisitos:**
    1. El sistema debe calcular la dirección dominante en vivo con cada par de lecturas fwd/bwd del flujo activo.
    2. El sistema debe actualizar la predicción con cada nueva lectura, sin reiniciar el modelo.
    3. El sistema debe alertar cuando la dirección dominante en vivo se desvíe del patrón esperado para ese tipo de servicio.

### Alcance del proyecto en conjunto

- **Qué SÍ cubre este proyecto:** monitoreo integrado (histórico y, en Unidad 2, en tiempo real) de cuatro variables de comportamiento del tráfico de red del campus — volumen, duración, tipo de servicio y dirección dominante — sobre el dataset propio (~400 000 flujos), con modelos predictivos U1 por integrante y un tablero Grafana unificado.
- **Qué NO cubre — fuera de alcance:** no incluye la clasificación definitiva de ataques/intrusiones ni el etiquetado de seguridad del tráfico — eso es alcance del trabajo de tesis de Nick (framework NIDS basado en Mamba-3), un proyecto aparte; no reemplaza a Suricata como herramienta de captura ni a ningún IDS/IPS de producción; no incluye bloqueo ni actuación automática sobre el tráfico (la Unidad 1 es solo analítica, sin actuador); no cubre redes fuera del campus piloto; no incluye la gestión administrativa de la infraestructura de red.

**Pendiente para las siguientes sesiones:** la infraestructura compartida (Spark, Kafka, almacenamiento analítico, observabilidad con Grafana y estimación de costos operacionales, prácticas de DataOps) se construye en clase, sesión por sesión, sobre las dimensiones y fuentes de esta ficha.

## 4. Aprobación

- **Docente:**
- **Fecha:**
