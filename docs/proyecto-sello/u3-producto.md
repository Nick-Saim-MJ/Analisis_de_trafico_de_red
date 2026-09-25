# Big Data - Producto de Unidad 3 (Producto Final del curso)

**Esta es la plantilla-ejemplo del producto de Unidad 3 de Big Data.** Según el sílabo, Unidad 3 se llama "Integración, DataOps y despliegue del sistema final" — el Producto Final del curso es, literalmente, el producto de Unidad 3: no son dos entregas distintas, es el mismo sistema (Unidad 1 + Unidad 2) integrado, endurecido, documentado y defendido. La estructura de esta plantilla es exigible a todos.

## Producto

**Sistema Big Data distribuido end-to-end para procesamiento batch y streaming, analítica/ML, observabilidad y visualización BI para la toma de decisiones.**

Integra los componentes desarrollados en las unidades anteriores, despliega o empaqueta el sistema mediante prácticas de DataOps/DevOps, prepara una demo end-to-end, documenta la operación del sistema, valida resultados técnicos y analíticos, y sustenta una solución final orientada a la toma de decisiones.

## 1. Componentes mínimos del producto final

Al cierre de Unidad 3, el sistema debe integrar — sin excepción — todo lo construido en las dos unidades anteriores:

- Problema de datos delimitado y arquitectura Big Data justificada (Lambda o Kappa) (Unidad 1).
- Pipeline batch con Spark: extracción, transformaciones, agregaciones y validación básica de calidad (Unidad 1).
- Salida analítica en formato eficiente, particionada, como Parquet (Unidad 1).
- Ingesta de eventos empresariales e IoT/sensores con Kafka (Unidad 2).
- Procesamiento streaming con Spark Structured Streaming (ventanas, watermarking) (Unidad 2).
- Observabilidad técnica con métricas, paneles y estimación de costos (Unidad 2).
- Modelo ML, inferencia de series de tiempo o salida BI según el alcance del proyecto (Unidad 1 y 2).
- Prácticas de DataOps/DevOps para la integración, empaquetado o despliegue del sistema.
- Evidencias reproducibles de ejecución en el laboratorio.
- Documentación técnica, operativa y demo final end-to-end.

## 2. Rúbrica de Evaluación

**Tabla 1. Rúbrica de evaluación de la Unidad 3 (Producto Final)**

| Criterio | Peso | CE / Nivel | A (20 pts) | B (15 pts) | C (10 pts) | D (5 pts) | Calificación obtenida |
|---|---:|---|---|---|---|---|---:|
| 1. Integración end-to-end de batch, streaming y BI/ML | 20% | CE042/043/044-N3 (integración) | Los tres flujos (batch, streaming, BI/ML) integrados y verificados juntos en un solo sistema. | Integración funcional, con alguno de los tres flujos parcialmente conectado. | Integración parcial, flujos aislados entre sí. | No presenta integración end-to-end. | |
| 2. Prácticas DataOps/DevOps aplicadas al empaquetado o despliegue | 15% | — (apoyo) | Empaquetado o despliegue reproducible con prácticas DataOps/DevOps verificables (Docker, CI, scripts). | Prácticas aplicadas parcialmente, con reproducibilidad limitada. | Prácticas mencionadas, sin evidencia clara de aplicación. | No aplica prácticas de DataOps/DevOps. | |
| 3. Documentación operativa completa | 15% | — (apoyo) | Documentación publicada en MkDocs o equivalente, completa y reproducible por otra persona sin ayuda del equipo. | Documentación completa, con pasos de reproducción menores por aclarar. | Documentación parcial o con vacíos que dificultan reproducirla. | No presenta documentación operativa. | |
| 4. Hardening y revisión técnica final | 10% | — (apoyo) | Checklist de hardening aplicado, fallos corregidos y sistema estabilizado, verificable en vivo. | Hardening aplicado parcialmente, con fallos menores pendientes. | Revisión técnica superficial, sin corrección verificable. | No presenta evidencia de hardening ni revisión técnica. | |
| 5. Demo reproducible | 15% | — (apoyo) | Demo end-to-end reproducible por otra persona siguiendo la documentación, sin ayuda del equipo. | Demo reproducible con ajustes menores necesarios. | Demo parcialmente reproducible. | Demo no reproducible fuera del equipo. | |
| 6. Sustentación técnica con evidencias de ejecución, métricas, resultados y valor para la toma de decisiones | 25% | CE042/043/044-N3 (cierre) + CG | Explica y defiende el producto con solvencia; demuestra aporte individual, dominio técnico, comunicación clara, repositorio, documentación y actitud profesional. | Sustentación clara y funcional, con detalles menores en defensa técnica, evidencias, comunicación o documentación. | Sustentación parcial; dominio, evidencias, comunicación o aporte individual insuficientemente demostrados. | No sustenta adecuadamente, no demuestra autoría o no presenta evidencias mínimas del producto. | |

Nota final = suma de (`Peso` × `Puntos de la calificación obtenida`) / 100 × 20.

`CE042/043/044-N3` = las tres competencias que Big Data evidencia en Nivel 3, ya cerradas por separado en Unidad 1 y 2; aquí no se genera evidencia de Nivel nueva, se integra o se cierra. `— (apoyo)` = infraestructura técnica (DataOps, documentación, hardening, demo) sin Nivel propio. `CG` = Competencia General "Investigación e Innovación" del sílabo.

**Tabla 2. Subaspectos de la sustentación integral**

El criterio 6 se evalúa con los siguientes subaspectos. La sustentación integral representa como mínimo el 25% de la evaluación del producto final — coherente con el peso del criterio 6 en esta rúbrica.

| Subaspecto | Qué observa |
|---|---|
| 1. Defensa técnica | Explicación del problema, arquitectura, flujo batch/streaming, decisiones técnicas, validaciones, resultados, limitaciones y evidencias generadas. |
| 2. Comunicación y orden | Claridad, estructura, tiempo y lenguaje técnico. |
| 3. Presentación personal y actitud | Puntualidad, vestimenta limpia y adecuada, higiene, cabello ordenado, actitud profesional, respeto, honestidad y coherencia con los valores y principios cristianos de la institución. |
| 4. Aporte individual | Cada integrante demuestra lo que hizo. |
| 5. Repositorio y estándares | Topics, organización, commits, documentación y reproducibilidad. |
| 6. MkDocs o equivalente | Documentación publicada, navegable y alineada al producto. |
| 7. Pitch/demo ejecutiva | Introducción clara del problema, solución y valor, con apoyo visual (.pptx, Canva o equivalente), seguida de una demo funcional. |

Para usar la rúbrica con IA, solicita:

```text
Evalúa la sustentación y el producto (u3-producto.md, adaptada al instrumento propio del estudiante) usando la rúbrica de esta sección.
Para cada criterio selecciona la calificación obtenida: A=20, B=15, C=10, D=5.
Justifica brevemente cada nivel con evidencia concreta del sistema completo (Unidades 1 y 2 integradas).
Para el criterio 6, verifica explícitamente los 7 subaspectos de la Tabla 2 antes de asignar el nivel.
Calcula la nota final con la fórmula: suma de (Peso × Puntos de la calificación obtenida) / 100 × 20.
Indica 2 fortalezas y 2 recomendaciones finales del producto.
```

## 3. Trazabilidad y procedencia de la rúbrica

Los seis criterios son cita literal de los criterios de evaluación del producto de la Unidad III en el sílabo de Big Data — a diferencia de Unidad 1 y Unidad 2, el sílabo de Unidad 3 ya incluye la sustentación técnica (con evidencias, métricas y valor para decisiones) como parte de sus criterios oficiales (criterio 6), por eso no se agrega aquí un criterio de sustentación aparte.

**Con la malla curricular:** este producto final es el cierre del curso `BIGDATA` dentro de **tres competencias de la línea Ciencia de Datos e IA**, cada una ya evidenciada por separado en Unidad 1 y Unidad 2: **Nivel 3 de CE042** (Construye Dataset, pipeline batch de Unidad 1), **Nivel 3 de CE043** (Genera Modelos, primer modelo de Unidad 1 + series de tiempo de Unidad 2) y **Nivel 3 de CE044** (Analiza y Define Estrategias, BI/ML de Unidad 2). Unidad 3 no genera una evidencia de Nivel nueva: integra y consolida las tres. El cierre real de las cuatro competencias de CD/IA (CE041-CE044) en un sistema real ocurre recién en `PI2`, Ciclo 10.

## 4. Secuencia sugerida de presentación

La presentación puede organizarse con una secuencia breve de apoyo visual. El video pitch o introducción ejecutiva abre la sustentación y no reemplaza la demo ni la defensa técnica.

**Tabla 3. Secuencia sugerida de presentación**

| Orden | Slide o momento | Propósito | Competencia evidenciada |
|---:|---|---|---|
| 1 | Título del proyecto y equipo | Identificar el proyecto, integrantes y dominio de datos elegido. | CE044 |
| 2 | Video pitch o introducción ejecutiva | Presentar problema, solución, valor y participación del equipo. | CE044 |
| 3 | Problema y alcance | Explicar la necesidad analítica, las fuentes de datos y el alcance del sistema. | CE042 |
| 4 | Arquitectura Big Data | Mostrar ingesta, procesamiento, almacenamiento, consumo y observabilidad. | CE042 |
| 5 | Procesamiento batch | Explicar el pipeline Spark, transformaciones, calidad de datos y salidas en Parquet. | CE042 |
| 6 | Ingesta y streaming | Mostrar el flujo de eventos con Kafka y el procesamiento con Spark Structured Streaming. | CE042 |
| 7 | Observabilidad y costos | Presentar métricas, paneles, logs y estimación de costos operacionales. | CE044 |
| 8 | Resultados BI/ML | Mostrar el modelo, la inferencia o el tablero BI generado para la toma de decisiones. | CE043 + CE044 |
| 9 | Demo end-to-end | Ejecutar o evidenciar el flujo batch/streaming completo del sistema. | CE042 + CE043 |
| 10 | Aporte individual | Indicar qué hizo cada integrante. | CE044 |
| 11 | Repositorio y estándares | Mostrar repositorio, topics, estructura, documentación publicada en MkDocs o equivalente, y forma de ejecución. | CE044 |
| 12 | Limitaciones y mejoras | Reconocer límites del producto y mejoras posibles. | CE044 |

## 5. Documentación y reporte del producto final

### 5.1 Plantilla mínima de documentación MkDocs o equivalente

La documentación publicada no reemplaza al informe. Su función es permitir que otra persona comprenda, ejecute, revise y verifique el producto desde el repositorio.

**Tabla 4. Plantilla mínima de documentación**

| Página o sección | Contenido mínimo | Evidencia esperada |
|---|---|---|
| Inicio | Nombre del proyecto, problema, solución, curso, integrantes y enlace al repositorio. | Presentación clara del producto. |
| Instalación o ejecución | Requisitos, dependencias, configuración y comandos para ejecutar el proyecto. | Instrucciones reproducibles. |
| Uso del sistema | Flujo principal, notebooks, comandos o casos de uso según corresponda. | Guía breve para probar el producto. |
| Arquitectura o estructura | Diagrama, componentes, carpetas principales y decisiones técnicas. | Vista técnica comprensible. |
| Módulos o funcionalidades | Descripción de las funciones principales del producto. | Relación entre funcionalidades y problema. |
| Datos | Modelo, archivos, datasets, fuentes o estructura de almacenamiento. | Evidencia de gestión de datos. |
| Pruebas y evidencias | Casos de prueba, capturas, resultados, métricas, validaciones o salidas generadas. | Verificación del funcionamiento. |
| Equipo y aporte individual | Integrantes, responsabilidades, aportes y evidencias de participación. | Autoría verificable. |
| Repositorio y estándares | Topics académicos, estructura, commits y criterios de reproducibilidad. | Cumplimiento de estándares técnicos. |
| Limitaciones y mejoras | Restricciones del producto y mejoras futuras priorizadas. | Cierre reflexivo y realista. |

### 5.2 Plantilla sugerida de informe del proyecto

El informe debe documentar el producto de manera breve, verificable y alineada a las competencias evaluadas. No reemplaza la demo ni la sustentación; organiza las evidencias del proyecto.

**Tabla 5. Plantilla sugerida de informe**

| Sección | Contenido mínimo | Evidencia esperada |
|---|---|---|
| Portada | Nombre del proyecto, curso, sección, integrantes, docente y semestre. | Datos completos del equipo. |
| Resumen del proyecto | Problema de datos, solución Big Data y valor analítico. | Síntesis de 8 a 12 líneas. |
| Competencia y alcance | Competencia/capacidad del proyecto y competencias relacionadas. | CE042, CE043 y CE044 vinculadas al producto. |
| Problema y datos | Necesidad analítica, fuentes, alcance y restricciones. | Descripción del problema y dataset. |
| Arquitectura Big Data | Componentes de ingesta, procesamiento, almacenamiento, consumo y observabilidad. | Diagrama y decisiones técnicas. |
| Procesamiento batch | Jobs, transformaciones, validaciones de calidad y salidas en Parquet. | Notebooks, comandos, salidas y capturas. |
| Ingesta y streaming | Flujo de eventos empresariales e IoT, procesamiento con Spark Structured Streaming y resultados. | Evidencias de ejecución o simulación. |
| Observabilidad y costos | Métricas, paneles, logs y estimación de costos operacionales. | Capturas, instrucciones y resultados reproducibles. |
| Resultados BI/ML | Modelo, inferencia, series de tiempo o tablero BI generado. | Métricas, tablas o visualizaciones. |
| Integración y DataOps | Prácticas de integración, empaquetado o despliegue del sistema final. | Evidencias de hardening y ejecución end-to-end. |
| Repositorio y documentación | Repositorio, topics, estructura, notebooks y documentación publicada. | URL del repositorio y MkDocs o equivalente. |
| Aporte individual | Responsabilidad de cada integrante. | Tabla de tareas, commits o evidencias por integrante. |
| Limitaciones y mejoras | Límites técnicos y mejoras posibles. | Lista priorizada y realista. |
