# S15 - Evaluación de la Unidad III

## 1. Propósito de la evaluación

Esta sesión no enseña contenido nuevo: cierra la Unidad III de **Big Data** y, con ella, el curso completo. El sílabo (sesión 15) define dos actividades para esta evaluación:

1. Resolver la evaluación teórico-práctica de los temas de la Unidad III.
2. Presentar y sustentar el Sistema Big Data distribuido end-to-end para procesamiento batch y streaming, analítica/ML y visualización BI.

**Esta sesión no repite lo evaluado en S5 ni en S12**: la Unidad I (batch) y la Unidad II (tiempo real) ya quedaron certificadas por separado; S15 evalúa la integración, DataOps y estabilización final de ambas juntas — el sistema completo, no una capacidad nueva.

**S16 (Continuación de la evaluación de la Unidad III)** no es una evaluación distinta: es la misma evaluación de esta guía, para los equipos o integrantes cuya presentación, sustentación o evidencia quedó pendiente en S15.

## 2. Producto evaluado

Del sílabo, el producto de la Unidad III es:

> Producto final que integra pipelines batch y streaming, observabilidad, analítica/ML, visualización BI y documentación operativa para una demo end-to-end defendida técnicamente.

El producto completo — plantilla-ejemplo con los componentes mínimos, rúbrica y plantillas de documentación — vive en [`u3-producto.md`](../proyecto-sello/u3-producto.md). La estructura es exigible a todos.

**Nota de alcance:** según el sílabo, Unidad 3 se llama "Integración, DataOps y despliegue del sistema final" — el producto de Unidad 3 **es** el Producto Final del curso, no una entrega distinta ni adicional.

## 3. Evaluación teórico-práctica (S13-S14)

Cubre la integración y el hardening final dictados antes de esta sesión. El docente puede tomarla escrita, oral o mixta.

**Tabla 1. Temario de la evaluación teórico-práctica**

| Sesión | Tema | Qué puede evaluar el docente |
|---|---|---|
| S13 | Integración del sistema, DataOps y BI | Cómo se integran los componentes batch, streaming, observabilidad y BI/ML en un solo flujo, y qué prácticas de DataOps/DevOps se aplicaron al empaquetado o despliegue. |
| S14 | Revisión técnica final y hardening | Qué se estabilizó (configuración, documentación, evidencias) y por qué un sistema "que funciona en la demo" no es lo mismo que un sistema reproducible. |

Preguntas de referencia (el docente puede formular equivalentes):

1. ¿Qué componente de tu sistema (batch, streaming, observabilidad o BI/ML) fue el más difícil de integrar con los demás, y por qué?
2. ¿Qué práctica de DataOps/DevOps aplicaste al empaquetado o despliegue de tu sistema, y qué problema real resuelve?
3. ¿Qué tuviste que estabilizar entre la Unidad II y esta evaluación para que el sistema fuera reproducible por otra persona?
4. Si otra persona clona tu repositorio hoy, ¿qué pasos exactos necesita seguir para levantar el sistema completo, y dónde están documentados?
5. ¿Qué limitación real del producto reconoces, y por qué no la resolviste dentro del alcance del curso?

## 4. Sustentación del producto final

**Tabla 2. Distribución de tiempo por integrante**

| Momento | Tiempo | Propósito |
|---|---:|---|
| Video pitch / introducción ejecutiva | 3 min | Presentar problema, solución, valor del producto y participación del equipo. |
| Exposición técnica | 10 min | Presentar problema, arquitectura, fuentes, pipelines, observabilidad, resultados y valor analítico ([Tabla 3 de `u3-producto.md`](../proyecto-sello/u3-producto.md#4-secuencia-sugerida-de-presentacion)). |
| Demostración end-to-end | 5 min | Ejecutar o evidenciar el flujo batch/streaming completo, salidas generadas, métricas y resultados BI/ML. |
| Preguntas individuales | 5 min | Verificar dominio y aporte propio, con base en la Tabla 1 y el producto completo. |

**Tabla 3. Entregables obligatorios**

| Entregable | Evidencia mínima | Criterio de aceptación |
|---|---|---|
| Producto final | [`u3-producto.md`](../proyecto-sello/u3-producto.md), adaptado al instrumento propio del equipo | Integra y estabiliza lo construido en Unidad 1 y Unidad 2 |
| Evidencia de integración end-to-end | Flujo batch, streaming y BI/ML ejecutados juntos, con al menos un caso de error | Verificable en vivo, no solo documentada |
| Evidencia de DataOps y hardening | Empaquetado/despliegue reproducible; checklist de hardening completado | Reproducible por otra persona desde el repositorio |
| Repositorio y documentación | Topics académicos vigentes, documentación completa publicada en MkDocs o equivalente, informe del proyecto | Reproducible sin ayuda del equipo |
| Sustentación grupal con aporte individual | Video pitch + exposición + demo + defensa por integrante, con los 7 subaspectos de [`u3-producto.md`, Tabla 2](../proyecto-sello/u3-producto.md#2-rubrica-de-evaluacion) | Aporte individual verificable dentro de la defensa grupal, conectado a valor para la toma de decisiones |

Criterios mínimos de aceptación:

- El sistema completo (Unidad 1 + Unidad 2) se ejecuta de extremo a extremo desde el repositorio, sin pasos no documentados.
- Los tres flujos (batch, streaming, BI/ML) se muestran integrados, no como componentes aislados.
- Al menos una práctica de DataOps/DevOps es verificable (empaquetado, script de despliegue, CI).
- La documentación (MkDocs o equivalente) permite a otra persona reproducir el sistema sin ayuda del equipo.
- El repositorio mantiene los topics académicos y evidencia de commits a lo largo del curso.
- La sustentación cubre los 7 subaspectos de la sustentación integral (`u3-producto.md`, Tabla 2), conectando resultados con valor real para la toma de decisiones.
- Cada integrante responde individualmente al menos una pregunta de la Tabla 1.

## 5. Rúbrica de evaluación

La rúbrica (6 criterios, cita literal de los criterios de evaluación del producto de la Unidad III en el sílabo de Big Data — el propio criterio 6 ya incluye la sustentación técnica con evidencias, métricas y valor para decisiones, por eso no se agrega un criterio de sustentación aparte) vive en [`u3-producto.md`](../proyecto-sello/u3-producto.md#2-rubrica-de-evaluacion), junto con la plantilla del producto, los subaspectos de sustentación y su trazabilidad con la malla curricular (CE042, CE043 y CE044, Nivel 3). Úsala directamente desde ahí para calificar la sustentación de esta sesión — no se duplica aquí.
