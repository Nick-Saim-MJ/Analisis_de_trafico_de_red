<!-- Transcripción fiel generada desde: bigdata2026-1.docx -->

Universidad Peruana Unión
Carret. Central km. 19.5 Ñaña. Telf. 01-6186300 Casilla 3564 Lima 1, Perú

# Sílabo: Big Data

## I. Información General de Asignatura

| N. | Campo | Información | N. | Campo | Información |
|---|---|---|---|---|---|
| 01 | Facultad/EGP | Facultad de Ingeniería y Arquitectura | 09 | Año de plan de estudio | 2022-1 |
| 02 | Programa de estudio | EP Ingeniería de Sistemas | 10 | Ciclo de estudio | 9 |
| 03 | Tipo de estudio | General | 11 | Código de asignatura |  |
| 04 | Nombre de asignatura | Big Data | 12 | Número de créditos | 3 |
| 05 | Duración |  | 13 | Nota mínima probatoria | 13 |
| 06 | Horas de la asignatura | H. Te. Pract: 32 / H. Prc. Pres: 32 | 14 | Año y semestre académico | 2026-1 |
| 07 | Docente | Sullon Macalupu Abel Angel |  |  |  |
| 08 | Pre-requisito | Minería de datos |  |  |  |

## II. Sumilla

Diseña e implementa soluciones Big Data integrando arquitecturas batch y streaming, construyendo pipelines distribuidos con Spark y Kafka, instrumentándolos con observabilidad y buenas prácticas operativas, y ejecutando experimentos de analítica/ML a escala para generar resultados reproducibles y orientados a decisión.

## III. Competencia del perfil de egreso en relación a la asignatura

| Tipo | Competencia | Nivel / dimensiones |
|---|---|---|
| General | **INVESTIGACIÓN E INNOVACIÓN:** Desarrolla y aplica habilidades de investigación científica y formativa, así como la capacidad de innovar de manera ética y basada en principios bíblico-cristianos, para contribuir al avance del conocimiento y la solución de problemas en la sociedad. | N. 1.1: Problematización, Diseño de Investigación. |
| Específica | **CIENCIA DE DATOS E INTELIGENCIA ARTIFICIAL:** Diseña y gestiona sistemas inteligentes basándose en metodologías, estándares y herramientas a fin de lograr estrategias de mejora para la organización. | N. 1.1: Analista de negocios, ingeniería de datos, científico de datos, analista de datos. |

## IV. Resultado de aprendizaje de la asignatura

| Resultado de aprendizaje | Producto Académico |
|---|---|
| Diseña e implementa soluciones Big Data integrando arquitecturas batch y streaming, construyendo pipelines distribuidos con Spark y Kafka, instrumentándolos con observabilidad y buenas prácticas operativas, y ejecutando experimentos de analítica/ML a escala para generar resultados reproducibles y orientados a decisión. | **Nombre:** Demo de arquitectura integrada (batch + streaming). |
|  | **Descripción:** Solución Big Data que integra un pipeline batch (ETL distribuido con Spark) y un pipeline streaming (Kafka + Spark Structured Streaming) con observabilidad (métricas, logging, alertas) y documentación operativa. Incluye un caso aplicado a regresión o series de tiempo a escala con reporte de métricas y guía de ejecución/reproducción. |

## V. Unidades de aprendizaje

## Unidad 1: Arquitecturas Big Data y ETL distribuido

| Resultado de aprendizaje | Producto |
|---|---|
| Analiza arquitecturas Big Data y construye un pipeline batch distribuido con Spark, desde la ingesta hasta la salida verificada, usando almacenamiento y formatos optimizados. | **Nombre:** Pipeline batch en Spark: ingesta -> transformación -> salida verificada. |

| Criterios de evaluación del producto | Descripción del producto |
|---|---|
| Distingue arquitecturas Lambda/Kappa y selecciona enfoque batch/streaming según el caso. Configura almacenamiento distribuido y elige formatos (Parquet/Avro/ORC) con particionado adecuado. Implementa transformaciones distribuidas en Spark usando DataFrames y evidencia lazy evaluation/planes. Aplica joins y funciones ventana considerando particionado y performance. Verifica la salida con controles de calidad y evidencia de ejecución reproducible. | Pipeline batch implementado con Spark que ingesta datos, transforma con operaciones distribuidas (joins/ventanas) y publica una salida verificada (controles de calidad, particionado y formato columnar). |

### Sesiones de aprendizaje

| N. | Fecha | Contenido | HT | HP | Actividad práctica | Actividad autónoma |
|---|---|---|---|---|---|---|
| 1 | 15/03/2026 - 21/03/2026 | Arquitecturas Big Data: ecosistema Big Data, arquitecturas Lambda/Kappa, batch vs. streaming, casos reales. | 2 | 2 | Analizar un caso real y decidir arquitectura (Lambda/Kappa) justificando batch vs. streaming y componentes del ecosistema. | Completar una ficha de arquitectura (diagrama, decisiones, supuestos y riesgos) para el caso elegido. |
| 2 | 22/03/2026 - 28/03/2026 | Almacenamiento distribuido: HDFS, formatos Parquet/Avro/ORC, particionado. | 2 | 2 | Diseñar layout de almacenamiento (carpetas/particiones) y convertir un dataset a formato columnar con particionado. | Documentar decisión de formato y particionado (criterios de consulta, volumen, cardinalidad) con evidencias de lectura/tamaño. |
| 3 | 29/03/2026 - 04/04/2026 | Fundamentos de Spark: modelo de ejecución, DataFrames API, lazy evaluation, planes (logical/physical). | 2 | 2 | Ejecutar transformaciones/acciones en Spark e inspeccionar el plan con explain(), identificando optimizaciones básicas. | Responder guía breve: qué se observa en el plan y cómo impacta en performance (capturas y comentario). |
| 4 | 05/04/2026 - 11/04/2026 | ETL escalable con Spark: transformaciones, joins distribuidos, funciones ventana. | 2 | 2 | Construir un ETL con joins distribuidos y al menos una función ventana, incorporando validaciones básicas. | Refinar el ETL: optimizar joins (broadcast/particiones si aplica) y documentar decisiones de performance. |
| 5 | 12/04/2026 - 18/04/2026 | Producto Unidad 1: pipeline batch en Spark, ingesta -> transformación -> salida verificada. | 2 | 2 | Integrar el pipeline batch completo y presentar evidencia de ejecución, salida verificada y particionado. | Consolidar entrega final del producto U1 con documentación (pasos, parámetros, supuestos, validaciones). |

## Unidad 2: Streaming, observabilidad y operación

| Resultado de aprendizaje | Producto |
|---|---|
| Implementa un pipeline streaming con Kafka y Spark Structured Streaming, instrumentándolo con métricas y logs, y aplicando criterios de operación y costos para escalar de forma cloud-agnostic. | **Nombre:** Pipeline streaming (Kafka + Spark) con métricas de rendimiento. |

| Criterios de evaluación del producto | Descripción del producto |
|---|---|
| Configura Kafka (tópicos, productores, consumidores) y define patrones de ingesta. Procesa streaming con ventanas y watermarking comprendiendo semántica de entrega. Implementa observabilidad (métricas, logging estructurado) para el pipeline. Define alertas y umbrales operativos para latencia, throughput y errores. Justifica decisiones de escalado y costos con buenas prácticas cloud-agnostic. | Pipeline en tiempo real que ingiere eventos con Kafka y procesa con Spark Structured Streaming, incorporando ventanas/watermarking y reportando métricas de latencia/throughput con logging estructurado y alertas básicas. |

### Sesiones de aprendizaje

| N. | Fecha | Contenido | HT | HP | Actividad práctica | Actividad autónoma |
|---|---|---|---|---|---|---|
| 1 | 19/04/2026 - 25/04/2026 | Kafka para ingesta en tiempo real: Apache Kafka, tópicos, productores, consumidores; patrones de ingesta en tiempo real. | 2 | 2 | Crear un tópico y ejecutar productor/consumidor; definir esquema de evento y patrón de ingesta para el caso. | Documentar contrato de evento (campos, tipos, ejemplos) y estrategia de particionado del tópico. |
| 2 | 26/04/2026 - 02/05/2026 | Spark Structured Streaming: micro-batch, ventanas, watermarking, semántica de entrega. | 2 | 2 | Implementar un stream con ventanas y watermarking, y medir comportamiento (latencia y throughput). | Ajustar parámetros (trigger, watermark) y registrar efectos en latencia/throughput (tabla). |
| 3 | 03/05/2026 - 09/05/2026 | Observabilidad de pipelines: métricas latencia/throughput, logging estructurado, alertas. | 2 | 2 | Instrumentar el pipeline con métricas (latencia/throughput/errores) y logging estructurado; definir alertas y umbrales. | Proponer un tablero mínimo de operación (qué medir, umbrales, frecuencia) y adjuntar evidencias. |
| 4 | 10/05/2026 - 16/05/2026 | Costos y escalado cloud-agnostic: estimación, elasticidad, buenas prácticas. | 2 | 2 | Estimar recursos/costos del pipeline (batch/stream) y proponer estrategia de escalado (CPU/memoria/particiones). | Redactar nota operativa: buenas prácticas, riesgos (backpressure) y plan de escalado/optimización. |
| 5 | 17/05/2026 - 23/05/2026 | Producto Unidad 2: pipeline streaming (Kafka + Spark) con métricas de rendimiento. | 2 | 2 | Integrar pipeline streaming end-to-end con métricas y evidenciar rendimiento bajo carga controlada. | Consolidar documentación operativa (métricas, alertas, parámetros, pasos de ejecución). |

## Unidad 3: ML a escala, regresión y series de tiempo distribuidos

| Resultado de aprendizaje | Producto |
|---|---|
| Ejecuta experimentos de ML a escala con Spark MLlib (regresión/clasificación y/o series de tiempo), ajusta hiperparámetros de forma distribuida e integra criterios de calidad y DataOps conectando resultados a BI a escala. | **Nombre:** Experimento MLlib con regresión o serie de tiempo + reporte de métricas. |

| Criterios de evaluación del producto | Descripción del producto |
|---|---|
| Construye un pipeline MLlib escalable para regresión o clasificación. Aplica regularizacion (Ridge/Lasso) y evalua con métricas pertinentes. Implementa enfoque de series de tiempo (descomposicion y modelo) en contexto Big Data. Ejecuta ajuste de hiperparámetros distribuido (CrossValidator/ParamGridBuilder) y justifica selección. Integra controles de calidad y criterios DataOps, conectando resultados a BI a escala. | Experimento en Spark MLlib aplicando regresión o series de tiempo a escala, con pipeline, ajuste de hiperparámetros distribuido, métricas de evaluación y reporte técnico con interpretacion y recomendaciones. |

### Sesiones de aprendizaje

| N. | Fecha | Contenido | HT | HP | Actividad práctica | Actividad autónoma |
|---|---|---|---|---|---|---|
| 1 | 24/05/2026 - 30/05/2026 | Regresión distribuida con MLlib: pipeline de ML escalable, regresión distribuida (Ridge/Lasso a escala). | 2 | 2 | Construir pipeline MLlib de regresión y entrenar Ridge/Lasso, reportando métricas y tiempos de ejecución. | Comparar Ridge vs Lasso (métricas + complejidad) y redactar conclusiones técnicas breves. |
| 2 | 31/05/2026 - 06/06/2026 | Series de tiempo a escala: series de tiempo en Big Data, descomposicion, ARIMA/Prophet distribuido con Spark. | 2 | 2 | Preparar series (resampling/estacionalidad) y ejecutar un modelo (ARIMA/Prophet) en enfoque distribuido o por particiones. | Documentar supuestos del modelo y comparar al menos dos configuraciónes con métricas/errores. |
| 3 | 07/06/2026 - 13/06/2026 | Hiperparámetros distribuidos: clasificación y ajuste de hiperparámetros distribuido, CrossValidator, ParamGridBuilder. | 2 | 2 | Implementar CrossValidator con ParamGridBuilder para un modelo (clasificación o regresión) y seleccionar mejor configuración. | Completar tabla de experimentos (parámetros, métricas, tiempo) y justificar la selección final. |
| 4 | 14/06/2026 - 20/06/2026 | DataOps y BI a escala: calidad de datos y analítica BI a escala, DataOps, conexión Power BI/Databricks. | 2 | 2 | Definir controles de calidad y preparar dataset/modelo para consumo BI (Power BI/Databricks). | Redactar mini guía DataOps: validaciones, linaje básico y versionado de artefactos. |
| 5 | 21/06/2026 - 27/06/2026 | Producto Unidad 3: experimento MLlib con regresión o serie de tiempo + reporte de métricas. | 2 | 2 | Integrar experimento (pipeline + tuning + métricas) y presentar reporte con interpretacion y recomendaciones. | Consolidar reporte final y evidencias (configuración, métricas, reproducibilidad mínima). |

## Unidad 4: Proyecto perfil de egreso

| Resultado de aprendizaje | Producto |
|---|---|
| Presenta proyecto de perfil de egreso. | **Nombre:** Proyecto de perfil de egreso. |

| Criterios de evaluación del producto | Descripción del producto |
|---|---|
| Evalua si el producto incorpora buenas prácticas y estándares reconocidos en la gestión de servicios TI, como ITIL, COBIT o ISO 27001. Evidencia la aplicacion de conocimientos teoricos en un contexto practico y real. Analiza la claridad, precision y fundamentacion técnica en la documentación del plan de operación, soporte y evaluación de servicios TI. Evalua si el producto integra un diagnostico efectivo de la gestión de incidentes, infraestructura tecnologica y desempeno de los servicios. | Un informe técnico del Plan Integral incluye procedimientos de gestión operativa de servicios, estrategias de resolución de incidentes, administracion de problemas y gestión de cambios; administracion de infraestructura tecnologica; monitoreo y auditoria de desempeno; propuesta de mejora continua basada en auditorias y análisis de desempeno. |

### Sesiones de aprendizaje

| N. | Fecha | Contenido | HT | HP | Actividad práctica | Actividad autónoma |
|---|---|---|---|---|---|---|
| 1 | 28/06/2026 - 04/07/2026 | Producto de Curso: demo de arquitectura integrada (batch + streaming) + documentación operativa de pipeline aplicado a la regresión o series de tiempo. Presentacion final de proyectos, sustentación de proyecto de perfil de egreso. | 2 | 2 | Realizar demo end-to-end mostrando batch + streaming, observabilidad y el caso ML (regresión o series) con métricas y evidencia de operación. Presentacion del producto y evaluación con rúbricas de aprendizaje. | Preparar sustentación final y documentación operativa completa (pasos, parámetros, métricas, alertas, guía de reproducción). Organizar los entregables del producto y subirlo en la tarea de la unidad. |

## VI. Estrategias metodologicas

| N. | Estrategias de ensenanza y de aprendizaje que se aplicaran en la asignatura |
|---|---|
| 1.1 | Aprendizaje Cooperativo: Fomenta habilidades colaborativas y de trabajo en equipo, cruciales en la mayoria de los entornos laborales modernos. |
| 1.2 | Flipped Classroom (Clase Invertida): En esta metodologia, los estudiantes revisan el material teorico fuera del aula, generalmente en linea, y utilizan el tiempo en clase para actividades prácticas y discusiones. |
| 1.3 | Simulacion: Ofrece experiencias prácticas que son esenciales para la aplicacion de teorias en situaciones del mundo real. |
| 1.4 | Estudios de caso: Desarrolla el pensamiento crítico y la toma de decisiones al analizar situaciones complejas, preparando a los estudiantes para enfrentar problemas similares en sus futuras carreras profesionales. |
| 1.5 | Proyectos: Fomentan habilidades de investigación, gestión del tiempo y trabajo en equipo, todas cruciales en el mundo profesional. |

## VII. Evaluación

La evaluación de los estudiantes se rige por el Reglamento de Estudios, disponible en: <https://upeu.edu.pe/reglamentos/evaluación/>.

La estructura evaluativa comprende componentes formativos y/o de procesos, de producto y genéricos, reflejando un enfoque integral.

### Componentes de evaluación y ponderacion

- **Evaluación de Sesiones (ES):** Es el promedio de las evaluaciónes aplicadas a los estudiantes para verificar su proceso de aprendizaje durante las sesiones de las unidades. Su contribucion a la nota final es de hasta el 20%.
- **Evaluación de Productos (EP):** Es el promedio ponderado de las evaluaciónes de los productos entregados en cada unidad. Este componente representa un mínimo del 70% de la nota final.
- **Evaluación de Competencias Generales (ECG):** Esta evaluación aporta hasta un 10% al calculo de la nota final.

### Programacion de evaluaciónes

| Fecha | Unidad | Producto | Evaluación de proceso y de resultado | Pesos |
|---|---|---|---|---|
| 14/04/2026 | Unidad 1: Arquitecturas Big Data y ETL distribuido | Pipeline batch en Spark: ingesta -> transformación -> salida verificada. | Evaluación del producto | 20% |
| 14/04/2026 | Unidad 1: Arquitecturas Big Data y ETL distribuido | Pipeline batch en Spark: ingesta -> transformación -> salida verificada. | Evaluación de sesiones | 5% |
| 19/05/2026 | Unidad 2: Streaming, observabilidad y operación | Pipeline streaming (Kafka + Spark) con métricas de rendimiento. | Evaluación de sesiones | 5% |
| 19/05/2026 | Unidad 2: Streaming, observabilidad y operación | Pipeline streaming (Kafka + Spark) con métricas de rendimiento. | Evaluación del producto | 20% |
| 23/06/2026 | Unidad 3: ML a escala, regresión y series de tiempo distribuidos | Experimento MLlib con regresión o serie de tiempo + reporte de métricas. | Evaluación de sesiones | 5% |
| 23/06/2026 | Unidad 3: ML a escala, regresión y series de tiempo distribuidos | Experimento MLlib con regresión o serie de tiempo + reporte de métricas. | Evaluación del producto | 20% |
| 30/06/2026 | Unidad 4: Proyecto perfil de egreso | Proyecto de perfil de egreso. | Evaluación de sesiones | 5% |
| 30/06/2026 | Unidad 4: Proyecto perfil de egreso | Proyecto de perfil de egreso. | Evaluación del producto | 10% |
| 30/06/2026 | Competencia General | INVESTIGACIÓN E INNOVACIÓN: Desarrolla y aplica habilidades de investigación científica y formativa, así como la capacidad de innovar de manera ética y basada en principios bíblico-cristianos, para contribuir al avance del conocimiento y la solución de problemas en la sociedad. | Competencia General | 10% |

| Componente | Peso |
|---|---|
| Evaluación de sesiones | 20% |
| Evaluación del producto | 70% |
| Evaluación de competencia generica | 10% |
| **Total** | **100%** |

## VIII. Recursos, medios y materiales

| N. | Recursos, medios y materiales |
|---|---|
| 1 | Guias y/o tutoriales |
| 2 | PC de Escritorio con programas de ofimática |
| 3 | Laboratorios |
| 4 | Internet - Wifi |
| 5 | Proyector y/o TV Smart |

## IX. Referencias

### Básica (Fuentes primarias)

- Brink, H., Richards, J. and Fetherolf, M. (2016). *Real-World Machine Learning*. Manning Publications.
- Erl, T., Khattak, W. and Buhler, P. (2016). *Big Data Fundamentals: Concepts, Drivers & Techniques*. Prentice Hall.
- Cielen, D., Meysman, A. and Ali, M. (2016). *Introducing Data Science*. Manning Publications.

### Complementaria (Fuentes secundarias)

- Garillot, F. and Maas, G. (2017). *Stream Processing with Apache Spark*. O'Reilly.
- Richert, W. and Pedro-Coelho, L. (2013). *Building Machine Learning Systems with Python*. Packt Publishing.
- Grus, J. (2015). *Data Science from Scratch*. O'Reilly Media Inc.
- Poole, D. L. and Mackworth, A. K. (2010). *Artificial Intelligence: Foundations of Computer Agents*. Cambridge University Press.
- Marz, N. and Warren J. (2015). *Big Data: Principles and Best Practices of Scalable Realtime Data Systems*. Manning Publications.
- Ryza, S., Laserson, U., Owen, S., and Wills, J. (2017). *Advanced Analytics with Spark*. 2nd ed. O'Reilly.
- Harrington, P. (2012). *Machine Learning in Action*. Manning.
- Kelleher, J. D., Mac Namee, B. and D'Arcy, A. (2015). *Fundamentals of Machine Learning for Predictive Data Analytics: Algorithms, Worked Examples, and Case Studies*. The MIT Press.
- Gurin, J. (2014). *Open Data Now*. McGraw-Hill.
- Ryza, S., Laserson, U., Owen, S., and Wills, J. (2015). *Advanced Analytics with Spark*. O'Reilly.
- Geron, A. (2017). *Hands-on Machine Learning with Scikit-Learn & TensorFlow*. O'Reilly.
- Kitchin, R. (2014). *The Data Revolution: Big Data, Open Data, Data Infrastructures and Their Consequences*. SAGE Publications.

### Libros

- Pena, S. (2017). *Análisis de datos*. Fundacion universitaria. Bogota.
- Caballero, R., Riesco, E. (2019). *Big Data con Python: recoleccion, almacenamiento y proceso*. Alfaomega Cloud.
- Holmes, D. (2017). *Big Data, una breve introduccion*. Alfaomega Cloud.

### Enlaces de internet

- Corpus de modelos entrenados en español: <https://github.com/roquegv/spanishNLPModelCorpus>
- Pandas: <https://pandas.pydata.org/>
- Beautiful Soup: <https://beautiful-soup-4.readthedocs.io/en/latest/>
- Curso Machine Learning: <https://www.aprendemachinelearning.com>
- Data.gov: <https://www.data.gov>
- Documentación Spark: <https://spark.apache.org/docs/latest/>
