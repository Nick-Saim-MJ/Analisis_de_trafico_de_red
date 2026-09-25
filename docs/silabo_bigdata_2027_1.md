<!-- Borrador v2027-1, construido desde silabo_bigdata_2026_2.md. Solo cubre hasta la Unidad 1 -->
<!-- por ahora -- Unidad 2 y 3 quedan pendientes de una revisión posterior. Cambio de fondo -->
<!-- respecto a 2026-2: el producto del curso nombra explícitamente el DataLake (antes -->
<!-- implícito en "salidas... en Parquet") y el reporte KPI+ML (ya ocurría en S4, sin -->
<!-- nombrarlo así) -- ver [[project_bi_bigdata_v2027_producto_dw_datalake_kpi_ml]]. -->
<!-- NO reemplaza a silabo_bigdata_2026_2.md como sílabo vigente hasta que se confirme y complete. -->

Universidad Peruana Unión
Carret. Central km. 19.5 Ñaña. Telf. 01-6186300 Casilla 3564 Lima 1, Perú

# Sílabo: Desarrollo de Aplicaciónes Big Data

## I. Información General de Asignatura

| N. | Campo | Información | N. | Campo | Información |
|---|---|---|---|---|---|
| 01 | Facultad/EGP | Facultad de Ingeniería y Arquitectura | 09 | Año de plan de estudio | 2022-1 |
| 02 | Programa de estudio | EP Ingeniería de Sistemas | 10 | Ciclo de estudio | 9 |
| 03 | Tipo de estudio | General | 11 | Código de asignatura |  |
| 04 | Nombre de asignatura | Big Data | 12 | Número de créditos | 3 |
| 05 | Duración |  | 13 | Nota mínima probatoria | 13 |
| 06 | Horas de la asignatura | H. Te. Pract: 32 / H. Prc. Pres: 32 | 14 | Año y semestre académico | 2027-1 |
| 07 | Docente | Sullon Macalupu Abel Angel |  |  |  |
| 08 | Pre-requisito | Minería de datos |  |  |  |

## II. Sumilla

La asignatura es de naturaleza teórico-práctica, pertenece al Área de Estudios Específicos y Especialidad y a la subárea de Ingeniería de Software. Su propósito es brindar al estudiante los conocimientos necesarios para desarrollar aplicaciones distribuidas de procesamiento de datos a gran escala: arquitectura Big Data, procesamiento distribuido con Spark, un DataLake analítico como repositorio central, analítica/ML sobre ese DataLake, ingesta y procesamiento en streaming, observabilidad y visualización BI para la toma de decisiones. El curso desarrolla progresivamente un sistema Big Data que llega a un DataLake y reporta, sobre él, indicadores descriptivos (KPI) y modelos predictivos (ML) — primero en modo batch, después también en streaming — ejecutable en entornos reproducibles con Docker y Spark.

## III. Competencia del perfil de egreso en relación a la asignatura

| Tipo | Competencia | Nivel / dimensiones |
|---|---|---|
| Específica | **INGENIERÍA DE SOFTWARE:** Gestiona y desarrolla software de manera eficiente y efectiva, basándose en estándares internacionales de calidad a fin de lograr el control y aseguramiento de la calidad según el contexto de la organización. | N. 1.1: Programación. Desarrolla aplicaciones de escritorio, web y móvil. |
| General | **CARACTER Y APRENDIZAJE AUTONOMO:** Cultiva un carácter integro y autonomo, guíado por principios biblicos y adventistas, integrando un enfoque espiritual con la proactividad en el aprendizaje y el desarrollo personal. | N. 1.1: Firmeza de Proposito, Ejecucion, Dominio Propio, Mantener el Esfuerzo, Salud SocioEmocional. |

## IV. Resultado de aprendizaje de la asignatura

| Resultado de aprendizaje | Producto Académico |
|---|---|
| Diseña y construye un sistema Big Data que integra arquitectura Lambda/Kappa, procesamiento distribuido con Spark, un DataLake analítico particionado, modelos predictivos distribuidos (ML), ingesta y procesamiento en streaming, observabilidad y visualización BI, reportando sobre el mismo DataLake indicadores descriptivos (KPI) y predicciones (ML), validando su funcionamiento mediante evidencias técnicas y defensa individual de aportes. | **Nombre:** Sistema Big Data distribuido end-to-end, con DataLake analítico, reportes KPI+ML en batch y en streaming, observable y defendido técnicamente. |
|  | **Descripción:** Desarrolla un sistema Big Data mediante laboratorios reproducibles. La solución integra infraestructura distribuida, un DataLake analítico particionado (Parquet) como repositorio central, modelos predictivos (ML) entrenados sobre ese DataLake, ingesta y procesamiento de eventos en streaming, observabilidad y documentación técnica. El producto se presenta en equipo, pero cada estudiante evidencia y defiende su aporte individual. |

## V. Unidades de aprendizaje

## Unidad 1: Arquitecturas Big Data y DataLake batch con KPI+ML

| Resultado de aprendizaje | Producto |
|---|---|
| Decide una arquitectura Big Data, procesa y valida datos distribuidamente, construye un DataLake analítico particionado en formatos analíticos, y entrena sobre ese DataLake un modelo de regresión distribuida, reportando indicadores descriptivos (KPI, dentro del propio pipeline) y la predicción del modelo (ML) como un mismo entregable. | **Nombre:** DataLake batch distribuido con reportes de indicadores descriptivos (KPI) y modelo predictivo (ML) entrenado y comparado. |

| Criterios de evaluación del producto | Descripción del producto |
|---|---|
| Servicio de arquitectura decidido y justificado (Lambda o Kappa). Procesamiento distribuido validado (esquema, nulos, duplicados). DataLake analítico particionado en Parquet, verificado por lectura. Modelo de regresión distribuida entrenado y comparado contra al menos otra configuración/algoritmo, con métricas reportadas (RMSE, R², MAE) como indicador (KPI) del propio pipeline. Evidencias de ejecución reproducible y documentación técnica básica. | Construye la base técnica del sistema Big Data: arquitectura Lambda o Kappa, procesamiento distribuido con Spark, DataLake analítico particionado listo para consulta, y un primer modelo predictivo distribuido entrenado sobre ese DataLake, reportado junto con sus métricas dentro del propio pipeline (equivalente batch de un dashboard KPI+ML). |

### Sesiónes de aprendizaje

| N. | Fecha | Contenido | HT | HP | Actividad práctica | Actividad autónoma |
|---|---|---|---|---|---|---|
| 1 | 09/08/2027 - 14/08/2027 | Arquitectura Big Data: arquitecturas Lambda y Kappa, batch vs. streaming. | 2 | 2 | Analiza LambdaLab y decide entre arquitectura Lambda o Kappa, justificando batch vs. streaming para un caso de negocio. | Elabora un diagrama de arquitectura (Lambda o Kappa), decisiones técnicas, supuestos y riesgos. |
| 2 | 15/08/2027 - 21/08/2027 | Fundamentos PySpark: extracción, transformaciones, funciones, agrupaciones, agregaciones, RDD y evaluación perezosa (lazy evaluation). | 2 | 2 | Ejecuta transformaciones distribuidas con PySpark y valida resultados mediante DataFrames/RDD, identificando el momento en que Spark ejecuta realmente el cálculo (por ejemplo, con `explain()`). | Documenta transformaciones, funciones aplicadas, evidencias de ejecución y el efecto de la evaluación perezosa en el plan de ejecución. |
| 3 | 22/08/2027 - 28/08/2027 | Procesamiento distribuido y carga de datos particionada en un DataLake analítico (formatos columnares), con validación de calidad de datos (esquema, nulos, duplicados). | 2 | 2 | Construye una salida analítica particionada en el DataLake, en formato columnar, lista para BI/ML, aplicando controles de calidad (esquema, nulos, duplicados) antes de la carga. | Justifica formato, particionado, estructura de carpetas, criterios de consulta y los controles de calidad aplicados. |
| 4 | 29/08/2027 - 04/09/2027 | ML distribuido con Spark MLlib (Regresión): entrenamiento, evaluación y reporte de indicadores como KPI del pipeline. | 2 | 2 | Entrena un modelo de regresión distribuida sobre el DataLake con Spark MLlib, compara configuraciones básicas y reporta sus métricas (RMSE, R², MAE) como indicadores del pipeline. | Compara configuraciones básicas adicionales y documenta resultados del modelo, incluida la importancia de cada variable. |
| 5 | 05/09/2027 - 11/09/2027 | Integración del procesamiento batch distribuido: arquitectura Big Data, PySpark, DataLake, formatos analíticos, particionamiento, ML distribuido y reporte KPI+ML. | 2 | 2 | **Evaluación de la Unidad I:**<br>1. Resolver la evaluación teórico-práctica de los temas de la Unidad I.<br>2. Presentar y sustentar el DataLake batch distribuido con reportes KPI+ML. | Corrige observaciones y consolida la documentación técnica de U1. |

<!-- Unidad 2 (streaming: Kafka, Spark Structured Streaming, series de tiempo, observabilidad -->
<!-- con Grafana, BI/ML en streaming) y Unidad 3 (validación y consolidación) quedan pendientes -->
<!-- de esta revisión -- Unidad 2 ya cubre en la práctica la Dimensión U2 (streaming) del -->
<!-- patrón KPI+ML, con Grafana como la herramienta BI; falta revisar si su redacción también -->
<!-- debe nombrar explícitamente "DataLake en streaming" o un término equivalente. -->
