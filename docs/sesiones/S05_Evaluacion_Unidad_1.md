# S5 - Evaluación de la Unidad I

## 1. Propósito de la evaluación

Esta sesión no enseña contenido nuevo: cierra la Unidad I de **Big Data**. El sílabo (sesión 5) define dos actividades para esta evaluación:

1. Resolver la evaluación teórico-práctica de los temas de la Unidad I (sesiones 1 a 4).
2. Presentar y sustentar el Pipeline batch de ETL distribuido con salidas analíticas en Parquet listas para BI/ML.

## 2. Producto evaluado

Del sílabo, el producto de la Unidad I es:

> Pipeline batch de ETL distribuido con salidas analíticas en Parquet listas para BI/ML.

El producto completo — plantilla-ejemplo con los datasets reales del docente (H&M para calidad y particionamiento; campo eléctrico/magnético para ML) — vive en [`u1-producto.md`](../proyecto-sello/u1-producto.md): arquitectura seleccionada, transformaciones distribuidas, calidad y particionamiento, componente ML. La estructura es exigible a todos; el contenido se reemplaza por el del propio instrumento de cada equipo, declarado en su [Brief técnico-analítico](../proyecto-sello/brief.md) de S2.

Lo que sustentas en S5 es **tu propia dimensión U1**: el indicador y la pregunta predictiva que tú declaraste en el brief, sobre el instrumento de tu Proyecto Sello — no el dataset H&M ni el del campo eléctrico, y no necesariamente el mismo indicador que el resto de tu equipo.

### Lo que acumulaste sesión por sesión

Este producto no se construye en S5: se ensambla con lo que cada sesión anterior ya te pidió sobre tu propia dimensión U1.

**Tabla 1. De la sesión a tu dimensión U1 evaluada**

| Sesión | Qué produjiste (tu propia dimensión, sobre el instrumento de tu Proyecto Sello) | Dónde queda en `u1-producto.md` |
|---|---|---|
| S1 | Diagrama de arquitectura Big Data (Lambda o Kappa) del equipo, con decisiones técnicas, tecnologías propuestas, supuestos y riesgos. | 1. Arquitectura Big Data seleccionada |
| S2 | Notebook con extracción, transformaciones, funciones, agrupaciones/agregaciones, RDD y evidencia del plan de ejecución (`explain()`) sobre el instrumento batch de tu dimensión. | 2. Transformaciones distribuidas con PySpark |
| S3 | Esquema explícito, duplicados y nulos tratados, salida particionada y verificada en Parquet, sobre tu propio histórico. | 3. Calidad de datos y particionamiento analítico |
| S4 | Modelo predictivo entrenado sobre tu variable objetivo (el indicador de tu dimensión), comparado y guardado, con métricas reportadas. | 4. Componente ML distribuido |
| S5 (esta sesión) | Ensamblas todo lo anterior en tu propio notebook end-to-end y lo sustentas individualmente. | Tu dimensión completa + sección 4 de esta guía |

`u1-producto.md` muestra cómo se ve una dimensión terminada usando los ejemplos reales de clase; tu entregable real tiene la misma estructura, pero con el contenido que tú construiste en S1-S4 sobre tu propia pregunta.

## 3. Evaluación teórico-práctica (S1-S4)

Cubre los cuatro temas dictados antes de esta sesión. El docente puede tomarla escrita, oral o mixta.

**Tabla 2. Temario de la evaluación teórico-práctica**

| Sesión | Tema | Qué puede evaluar el docente |
|---|---|---|
| S1 | Arquitectura Big Data: Lambda y Kappa, batch vs. streaming | Diferencia entre arquitectura Lambda y Kappa, criterios para elegir una u otra, y por qué un caso de negocio concreto necesita batch, streaming, o ambos. |
| S2 | Fundamentos PySpark: transformaciones, funciones, agrupaciones y evaluación perezosa | Extracción con esquema explícito, transformaciones (`withColumn()`, `filter()`), agrupaciones/agregaciones, procesamiento RDD, y en qué momento Spark ejecuta realmente el cálculo (*lazy evaluation*, Catalyst Optimizer). |
| S3 | Procesamiento y calidad de datos: filtrado, duplicados, nulos y particionamiento analítico | Detección y tratamiento de duplicados y nulos, deduplicación determinista con `Window`+`row_number()`, particionamiento en Parquet y verificación con `PartitionFilters`. |
| S4 | ML distribuido con Spark MLlib (Regresión) | `VectorAssembler`, patrón `fit()`/`transform()`, métricas de regresión (RMSE, R², MAE), regularización, comparación entre algoritmos e importancia de variables. |

Preguntas de referencia (el docente puede formular equivalentes):

1. ¿Por qué una arquitectura Kappa no necesita una capa batch separada, y qué se pierde o se gana frente a Lambda al tomar esa decisión?
2. ¿Qué diferencia hay entre una transformación y una acción en Spark, y por qué esa diferencia explica que `explain(True)` muestre un plan sin que nada se haya ejecutado todavía?
3. Si no resuelves los duplicados de una fuente antes de integrarla con otra, ¿qué error concreto se propaga al resultado final, y por qué no siempre es visible de inmediato?
4. ¿Por qué particionar una salida Parquet por una columna de alta cardinalidad (como un ID único) sería un error, y qué columna sí conviene usar?
5. ¿Por qué `VectorAssembler` es obligatorio en Spark MLlib y no en scikit-learn, y qué pasaría si tu modelo ganara la comparación pero guardaras el primero que entrenaste en vez de ese?

## 4. Sustentación de tu dimensión U1

Aunque el equipo comparte instrumento y pregunta central, la sustentación de U1 es **individual**: cada integrante presenta y defiende su propia dimensión (brief, sección 3) — el aporte individual es, además, un subaspecto explícito de la sustentación integral del Proyecto Sello.

**Tabla 3. Distribución de tiempo por integrante**

| Momento | Tiempo | Propósito |
|---|---:|---|
| Presentación técnica | 8 min | Explicar tu dimensión U1 (sección 2), las decisiones tomadas y su evolución desde S1. |
| Demo técnica | 5 min | Ejecutar tu notebook (o las celdas clave) en vivo: transformación, control de calidad, particionamiento y entrenamiento/comparación del modelo. |
| Preguntas individuales | 5 min | Verificar dominio y aporte propio, con base en la Tabla 2. |

**Tabla 4. Entregables obligatorios**

| Entregable | Evidencia mínima | Criterio de aceptación |
|---|---|---|
| Producto de tu dimensión U1 | [`u1-producto.md`](../proyecto-sello/u1-producto.md), con tu propia dimensión y variable objetivo (brief, sección 3) | Coherente con el sílabo y con el notebook real ejecutable |
| Evidencia de calidad de datos | Controles de la Tabla 1 de `u1-producto.md` aplicados y verificados sobre tu propio histórico | Trazabilidad verificable con capturas y código, no solo documentada |
| Evidencia del componente ML | Componentes de la Tabla 2 de `u1-producto.md` aplicados y verificados sobre tu propia variable objetivo | Métricas reales reportadas, modelo guardado coincide con el ganador |
| Sustentación individual | Preguntas y defensa por integrante (sección 3) | Autoría demostrada |

Cada captura de pantalla del informe debe mostrar, sin recortar, el reloj del sistema (fecha y hora) y tu usuario o foto de perfil (Windows, VS Code o navegador) visibles en pantalla — es lo que permite verificar que la evidencia es tuya y que corresponde al momento real de tu trabajo, cruzado con el historial de commits de tu repositorio en GitHub.

Secuencia sugerida de presentación (referencias a secciones de `u1-producto.md`):

1. Presentar la arquitectura Big Data del equipo y tu propia dimensión U1 dentro de la pregunta central del Proyecto Sello.
2. Ejecutar en vivo una transformación distribuida y mostrar el plan de ejecución con `explain(True)`.
3. Mostrar los controles de calidad aplicados: duplicados y nulos, antes y después.
4. Ejecutar la escritura y lectura de la salida particionada, con `PartitionFilters` confirmado en el plan de ejecución.
5. Mostrar tu modelo entrenado, sus métricas, y la tabla comparativa contra al menos una configuración alternativa.
6. Cerrar mostrando que el modelo guardado coincide con el que realmente ganó la comparación, no con el primero que se entrenó.

Criterios mínimos de aceptación:

- Tu notebook corre de punta a punta sobre el instrumento real (o representativo) de tu dimensión.
- Al menos un control de duplicados y uno de nulos están aplicados y documentados, no solo mencionados.
- La salida está particionada en Parquet y verificada con lectura de vuelta.
- El modelo base está entrenado y evaluado con al menos tres métricas, comparado contra al menos una configuración alternativa.
- El modelo guardado coincide con el de mejor desempeño en la comparación.
- Respondes individualmente al menos una pregunta de la Tabla 2.

## 5. Rúbrica de evaluación

La rúbrica (7 criterios: 6 cita literal de los criterios de evaluación del producto de la Unidad I en el sílabo de Big Data + sustentación) vive en [`u1-producto.md`](../proyecto-sello/u1-producto.md#5-rubrica-de-evaluacion), junto con la plantilla del producto y su trazabilidad con la malla curricular (CE042 Nivel 3, con la primera mitad de CE043 Nivel 3). Úsala directamente desde ahí para calificar la sustentación de esta sesión — no se duplica aquí.
