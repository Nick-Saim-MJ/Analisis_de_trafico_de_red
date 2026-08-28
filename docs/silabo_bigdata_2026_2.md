<!-- Versión 2026-2 construida desde silabo_bigdata_2026_1.md y propuesta_silabo_2026_2.md -->

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
| 06 | Horas de la asignatura | H. Te. Pract: 32 / H. Prc. Pres: 32 | 14 | Año y semestre académico | 2026-2 |
| 07 | Docente | Sullon Macalupu Abel Angel |  |  |  |
| 08 | Pre-requisito | Minería de datos |  |  |  |

## II. Sumilla

La asignatura de Big Data es de carácter teórico-práctico. Desarrolla competencias para implementar soluciones Big Data distribuidas con procesamiento batch, procesamiento streaming, observabilidad, analítica/ML y visualización BI. El curso utiliza LambdaLab como entorno integrado de procesamiento distribuido en batch y streaming con Spark y Kafka, permitiendo construir pipelines de datos, gestionar almacenamiento analítico en Parquet, aplicar observabilidad y desarrollar soluciones BI/ML distribuidas mediante laboratorios reproducibles basados en Docker.

## III. Competencia del perfil de egreso en relación a la asignatura

| Tipo | Competencia | Nivel / dimensiones |
|---|---|---|
| General | **INVESTIGACIÓN E INNOVACIÓN:** Desarrolla y aplica habilidades de investigación científica y formativa, así como la capacidad de innovar de manera ética y basada en principios bíblico-cristianos, para contribuir al avance del conocimiento y la solución de problemas en la sociedad. | N. 1.1: Problematización, Diseño de Investigación. |
| Específica | **CIENCIA DE DATOS E INTELIGENCIA ARTIFICIAL:** Diseña y gestiona sistemas inteligentes basándose en metodologías, estándares y herramientas a fin de lograr estrategias de mejora para la organización. | N. 1.1: Analista de negocios, ingeniería de datos, científico de datos, analista de datos. |

## IV. Resultado de aprendizaje de la asignatura

| Resultado de aprendizaje | Producto Académico |
|---|---|
| Implementa, integra y sustenta una solución Big Data end-to-end que combina pipelines batch distribuidos, ingesta y procesamiento de eventos en tiempo real, analítica/ML a escala, observabilidad técnica y una capa de visualización BI, demostrando valor para la toma de decisiones. | **Nombre:** Sistema Big Data distribuido end-to-end para procesamiento batch y streaming, analítica/ML, observabilidad y visualización BI para la toma de decisiones. |
|  | **Descripción:** Solución Big Data reproducible que integra Spark, Kafka, almacenamiento analítico, procesamiento streaming, observabilidad con Grafana, analítica/ML distribuida, series de tiempo o inferencia y visualización BI. La solución muestra evidencias de ejecución, métricas técnicas y del modelo, documentación operativa y demo end-to-end. |

## V. Unidades de aprendizaje

## Unidad 1: Arquitecturas Big Data y ETL batch distribuido

| Resultado de aprendizaje | Producto |
|---|---|
| Construye un pipeline batch reproducible con procesamiento distribuido, aplica transformaciones sobre datos a escala, valida la calidad básica de los datos, organiza salidas en formatos analíticos como Parquet y deja un dataset preparado para consumo BI/ML. | **Nombre:** Pipeline batch de ETL distribuido con salidas analíticas en Parquet listas para BI/ML. |

| Criterios de evaluación del producto | Descripción del producto |
|---|---|
| Arquitectura Big Data seleccionada y justificada. Uso correcto de Spark/PySpark para extracción, transformación, agregacion y procesamiento distribuido. Datos cargados y particionados en formatos analíticos. Pipeline batch reproducible. Primer componente ML distribuido con métricas básicas. Evidencias técnicas y documentación de ejecución. | Pipeline batch que procesa datos distribuidos con Spark, genera salidas analíticas en Parquet y deja resultados preparados para analítica, ML y BI. |

### Sesiones de aprendizaje

| N. | Fecha | Contenido | HT | HP | Actividad práctica | Actividad autónoma |
|---|---|---|---|---|---|---|
| 1 | 10/08/2026 - 15/08/2026 | Arquitectura Big Data: arquitecturas Lambda y Kappa, batch vs. streaming. | 2 | 2 | Analiza LambdaLab y decide entre arquitectura Lambda o Kappa, justificando batch vs. streaming para un caso de negocio. | Elabora un diagrama de arquitectura (Lambda o Kappa), decisiones técnicas, supuestos y riesgos. |
| 2 | 16/08/2026 - 22/08/2026 | Fundamentos PySpark: extracción, transformaciones, funciones, agrupaciones, agregaciones, RDD y evaluación perezosa (lazy evaluation). | 2 | 2 | Ejecuta transformaciones distribuidas con PySpark y valida resultados mediante DataFrames/RDD, identificando el momento en que Spark ejecuta realmente el cálculo (por ejemplo, con `explain()`). | Documenta transformaciones, funciones aplicadas, evidencias de ejecución y el efecto de la evaluación perezosa en el plan de ejecución. |
| 3 | 23/08/2026 - 29/08/2026 | Procesamiento distribuido y carga de datos particionada en HDFS y formatos analíticos, con validación de calidad de datos (esquema, nulos, duplicados). | 2 | 2 | Construye una salida analítica particionada en formato columnar lista para BI/ML, aplicando controles de calidad (esquema, nulos, duplicados) antes de la carga. | Justifica formato, particionado, estructura de carpetas, criterios de consulta y los controles de calidad aplicados. |
| 4 | 30/08/2026 - 05/09/2026 | ML distribuido con Spark MLlib (Regresión). | 2 | 2 | Entrena un modelo de regresión distribuida con Spark MLlib y reporta métricas iniciales. | Compara configuraciónes básicas y documenta resultados del modelo. |
| 5 | 06/09/2026 - 12/09/2026 | Integración del procesamiento batch distribuido: arquitectura Big Data, PySpark, HDFS, formatos analíticos, particionamiento y ML distribuido. | 2 | 2 | **Evaluación de la Unidad I:**<br>1. Resolver la evaluación teórico-práctica de los temas de la Unidad I.<br>2. Presentar y sustentar el Pipeline batch de ETL distribuido con salidas analíticas en Parquet listas para BI/ML. | Corrige observaciones y consolida la documentación técnica de U1. |

## Unidad 2: Sistema Big Data en tiempo real: ingesta, streaming, observabilidad y BI/ML

| Resultado de aprendizaje | Producto |
|---|---|
| Implementa un pipeline Big Data en tiempo real que integra ingesta de eventos empresariales e IoT/sensores mediante Kafka, procesamiento distribuido con Spark Structured Streaming, observabilidad con Grafana y estimación de costos operaciónales. Además, prepara salidas BI/ML distribuidas y reutiliza modelos para series de tiempo e inferencia batch y/o streaming. | **Nombre:** Pipeline en tiempo real con ingesta de eventos empresariales e IoT/sensores, procesamiento streaming con Spark, observabilidad/costos y salidas BI/ML distribuidas. |

| Criterios de evaluación del producto | Descripción del producto |
|---|---|
| Ingesta de eventos empresariales mediante Kafka. Ingesta de eventos IoT/sensores. Procesamiento streaming con Spark Structured Streaming. Observabilidad con métricas y tableros. Estimacion de costos y criterios de escalado. Salidas BI/ML distribuidas. Series de tiempo o inferencia en streaming documentada y validada. | Pipeline Big Data en tiempo real que procesa eventos, produce resultados analíticos, expone métricas operativas y prepara resultados para BI/ML distribuido. |

### Sesiones de aprendizaje

| N. | Fecha | Contenido | HT | HP | Actividad práctica | Actividad autónoma |
|---|---|---|---|---|---|---|
| 6 | 13/09/2026 - 19/09/2026 | Ingesta de eventos empresariales en tiempo real. | 2 | 2 | Publica y consume eventos empresariales con Kafka para un flujo de negocio. | Documenta contrato de evento, tópico, particionado y evidencia de publicación/consumo. |
| 7 | 20/09/2026 - 26/09/2026 | Ingesta de eventos IoT/sensores en tiempo real. | 2 | 2 | Simula eventos de sensores o telemetría y los integra al pipeline de Kafka. | Ajusta esquema de evento, frecuencia, volumen y validaciones de datos. |
| 8 | 27/09/2026 - 03/10/2026 | Procesamiento en streaming con Spark: ventanas, watermarking y semántica de entrega. | 2 | 2 | Implementa procesamiento con Spark Structured Streaming usando ventanas, agregaciones, watermarking para datos tardíos y checkpointing. | Registra efectos de parámetros de streaming, watermark, latencia y throughput. |
| 9 | 04/10/2026 - 10/10/2026 | Observabilidad con Grafana y costos. | 2 | 2 | Instrumenta el pipeline con métricas, tablero de observabilidad y estimación de costos. | Define umbrales, riesgos operativos, costos estimados y plan de escalado. |
| 10 | 11/10/2026 - 17/10/2026 | Series de tiempo e inferencia en streaming. | 2 | 2 | Aplica un modelo o inferencia de series de tiempo sobre datos batch y/o eventos streaming. | Compara resultados, errores, supuestos y recomendaciones técnicas. |
| 11 | 18/10/2026 - 24/10/2026 | BI/ML distribuido con Spark: KPIs del BI y visualización de la predicción de series de tiempo en streaming. | 2 | 2 | Prepara y visualiza en el BI los KPIs del flujo de eventos en streaming junto con la predicción de series de tiempo generada en la sesión anterior. | Documenta datos de salida, KPIs, métricas, validaciones y la relación entre la predicción y su visualización en BI. |
| 12 | 25/10/2026 - 31/10/2026 | Integración del procesamiento en tiempo real: Kafka, eventos empresariales e IoT, Spark Structured Streaming, observabilidad, costos y salidas BI/ML. | 2 | 2 | **Evaluación de la Unidad II:**<br>1. Resolver la evaluación teórico-práctica de los temas de la Unidad II.<br>2. Presentar y sustentar el Pipeline en tiempo real con ingesta, streaming, observabilidad y salidas BI/ML distribuidas. | Corrige observaciones y prepara la integración final del sistema. |

## Unidad 3: Integración, DataOps y despliegue del sistema final

| Resultado de aprendizaje | Producto |
|---|---|
| Integra los componentes desarrollados en las unidades anteriores, despliega o empaqueta el sistema mediante prácticas de DataOps/DevOps, prepara una demo end-to-end, documenta la operación del sistema, valida resultados técnicos y analíticos, y sustenta una solución final orientada a la toma de decisiones. | **Nombre:** Sistema Big Data distribuido end-to-end para procesamiento batch y streaming, analítica/ML, observabilidad y visualización BI para la toma de decisiones. |

| Criterios de evaluación del producto | Descripción del producto |
|---|---|
| Integración end-to-end de batch, streaming y BI/ML. Prácticas DataOps/DevOps aplicadas al empaquetado o despliegue. Documentación operativa completa. Hardening y revisión técnica final. Demo reproducible. Sustentación técnica con evidencias de ejecución, métricas, resultados y valor para la toma de decisiones. | Producto final que integra pipelines batch y streaming, observabilidad, analítica/ML, visualización BI y documentación operativa para una demo end-to-end defendida técnicamente. |

### Sesiones de aprendizaje

| N. | Fecha | Contenido | HT | HP | Actividad práctica | Actividad autónoma |
|---|---|---|---|---|---|---|
| 13 | 01/11/2026 - 07/11/2026 | Integración del sistema, DataOps y BI. | 2 | 2 | Integra los componentes batch, streaming, observabilidad y BI/ML en una demo end-to-end. | Documenta flujo completo, dependencias, pasos de ejecución y evidencias de integración. |
| 14 | 08/11/2026 - 14/11/2026 | Revisión técnica final y hardening. | 2 | 2 | Realiza revisión técnica, estabiliza configuración, corrige fallos y mejora documentación operativa. | Completa el checklist de hardening y prepara el guion de demostración, las evidencias, las métricas y la explicación técnica del sistema. |
| 15 | 15/11/2026 - 21/11/2026 | Integración del sistema Big Data end-to-end: procesamiento batch y streaming, DataOps, observabilidad, BI/ML, documentación y despliegue. | 2 | 2 | **Evaluación de la Unidad III:**<br>1. Resolver la evaluación teórico-práctica de los temas de la Unidad III.<br>2. Presentar y sustentar el Sistema Big Data distribuido end-to-end para procesamiento batch y streaming, analítica/ML y visualización BI. | Registra las observaciones del docente y actualiza el repositorio, las evidencias, las métricas y la documentación final. |
| 16 | 22/11/2026 - 28/11/2026 | Integración de sistemas Big Data: arquitectura, procesamiento distribuido, batch, streaming, observabilidad, DataOps, BI/ML y despliegue. | 2 | 2 | **Continuación de la evaluación de la Unidad III:**<br>Realizar, según corresponda, la evaluación final individual o las presentaciones y sustentaciones pendientes mediante demostración, preguntas técnicas y revisión de evidencias. | Actualiza la entrega final y reflexiona sobre aprendizajes, limitaciones y mejoras futuras del sistema. |

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
| 18/09/2026 | Unidad 1: Arquitecturas Big Data y ETL batch distribuido | Pipeline batch de ETL distribuido con salidas analíticas en Parquet listas para BI/ML. | Evaluación de sesiones | 5% |
| 18/09/2026 | Unidad 1: Arquitecturas Big Data y ETL batch distribuido | Pipeline batch de ETL distribuido con salidas analíticas en Parquet listas para BI/ML. | Evaluación del producto | 20% |
| 30/10/2026 | Unidad 2: Sistema Big Data en tiempo real: ingesta, streaming, observabilidad y BI/ML | Pipeline en tiempo real con ingesta de eventos empresariales e IoT/sensores, procesamiento streaming con Spark, observabilidad/costos y salidas BI/ML distribuidas. | Evaluación de sesiones | 5% |
| 30/10/2026 | Unidad 2: Sistema Big Data en tiempo real: ingesta, streaming, observabilidad y BI/ML | Pipeline en tiempo real con ingesta de eventos empresariales e IoT/sensores, procesamiento streaming con Spark, observabilidad/costos y salidas BI/ML distribuidas. | Evaluación del producto | 25% |
| 20/11/2026 | Unidad 3: Integración, DataOps y despliegue del sistema final | Sistema Big Data distribuido end-to-end para procesamiento batch y streaming, analítica/ML, observabilidad y visualización BI para la toma de decisiones. | Evaluación de sesiones | 10% |
| 20/11/2026 | Unidad 3: Integración, DataOps y despliegue del sistema final | Sistema Big Data distribuido end-to-end para procesamiento batch y streaming, analítica/ML, observabilidad y visualización BI para la toma de decisiones. | Evaluación del producto | 25% |
| 20/11/2026 | Competencia General | INVESTIGACIÓN E INNOVACIÓN: Desarrolla y aplica habilidades de investigación científica y formativa, así como la capacidad de innovar de manera ética y basada en principios bíblico-cristianos, para contribuir al avance del conocimiento y la solución de problemas en la sociedad. | Competencia General | 10% |

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
