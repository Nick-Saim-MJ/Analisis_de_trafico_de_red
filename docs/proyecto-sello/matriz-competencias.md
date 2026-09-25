# Matriz de Competencias — Big Data

**Propósito:** consolidar en una sola vista la trazabilidad completa `Competencia (SO) → Nivel → Sesión → Criterio de rúbrica → Peso → Umbral de logro → Última medición`, hoy repartida entre `malla-curricular-2024.md` (competencia → curso → nivel) y cada `uN-producto.md` (criterio → competencia, ya etiquetado con la columna "CE / Nivel"). Es el mismo formato construido para [DIST](https://github.com/262dist/pagatu/blob/main/docs/proyecto-sello/matriz-competencias.md) — ver ese documento para la explicación completa de cada sección.

Este documento **no reemplaza** las rúbricas de `u1-producto.md`, `u2-producto.md` y `u3-producto.md` — las resume y las conecta con el marco institucional.

## 1. Competencias evaluadas en este curso

A diferencia de DIST (una sola competencia técnica), Big Data aporta evidencia a **tres** competencias técnicas más una general:

| Competencia | Fuente | Naturaleza |
|---|---|---|
| **CE042 — Construye Dataset**, Nivel 3 | Malla curricular 2024, línea Ciencia de Datos e IA | Técnica. Evidencia: `BIGDATA` (junto con `MD`). Evidencia completa en Unidad 1. |
| **CE043 — Genera Modelos**, Nivel 3 | Malla curricular 2024, línea Ciencia de Datos e IA | Técnica. Evidencia: `BIGDATA` (junto con `MD`). Se completa en dos mitades: primer modelo en Unidad 1, series de tiempo en Unidad 2. |
| **CE044 — Analiza y Define Estrategias**, Nivel 3 | Malla curricular 2024, línea Ciencia de Datos e IA | Técnica. Evidencia: `BIGDATA` (junto con `MD`). Evidencia completa en Unidad 2. |
| **Competencia General — Investigación e Innovación** | Sílabo Big Data, sección III, 10% de la nota final | Transversal, no técnica. |

El criterio de "Sustentación" en Unidad 1 y Unidad 2 **no evidencia ninguna de las tres técnicas** — los criterios técnicos de cada unidad ya incluyen su verificación en vivo, así que la Sustentación evidencia únicamente la Competencia General (ver la columna `CE / Nivel` de cada `uN-producto.md`). El criterio de cierre de Unidad 3 es distinto: el propio sílabo lo funde con el cierre de las tres competencias, por eso sí lleva esas etiquetas — misma corrección aplicada en la matriz de DIST.

## 2. Matriz consolidada

**Tabla 1. CE042 — Construye Dataset, Nivel 3 (evidencia técnica)**

| Unidad | Sesión | Criterio de rúbrica | Peso en la unidad | Umbral de logro | Última medición |
|---|---|---|---:|---|---|
| U1 | S5 | 1. Arquitectura Big Data seleccionada y justificada | 12% | ≥70% en B o superior | Pendiente — primera cohorte 2026-2 |
| U1 | S5 | 2. Uso correcto de Spark/PySpark | 16% | ≥70% en B o superior | Pendiente |
| U1 | S5 | 3. Datos cargados y particionados en formatos analíticos | 16% | ≥70% en B o superior | Pendiente |
| U1 | S5 | 4. Pipeline batch reproducible | 12% | ≥70% en B o superior | Pendiente |
| U1 | S5 | 6. Evidencias técnicas y documentación de ejecución (apoyo) | 8% | ≥70% en B o superior | Pendiente |
| U3 | S15 | 1. Integración end-to-end (integración, junto con CE043/CE044) | 20% | ≥70% en B o superior | Pendiente — S6-S14 aún no dictadas |
| U3 | S15 | 6. Sustentación técnica (cierre, junto con CE043/CE044) | 25% | ≥70% en B o superior | Pendiente |

**Tabla 2. CE043 — Genera Modelos, Nivel 3 (evidencia técnica, en dos mitades)**

| Unidad | Sesión | Criterio de rúbrica | Peso en la unidad | Umbral de logro | Última medición |
|---|---|---|---:|---|---|
| U1 | S5 | 5. Primer componente ML distribuido (primera mitad) | 16% | ≥70% en B o superior | Pendiente |
| U2 | S12 | 7. Series de tiempo o inferencia en streaming (segunda mitad, completa la competencia) | 12% | ≥70% en B o superior | Pendiente — S6-S11 aún no dictadas |
| U3 | S15 | 1. Integración end-to-end (integración, junto con CE042/CE044) | 20% | ≥70% en B o superior | Pendiente |
| U3 | S15 | 6. Sustentación técnica (cierre, junto con CE042/CE044) | 25% | ≥70% en B o superior | Pendiente |

**Tabla 3. CE044 — Analiza y Define Estrategias, Nivel 3 (evidencia técnica)**

| Unidad | Sesión | Criterio de rúbrica | Peso en la unidad | Umbral de logro | Última medición |
|---|---|---|---:|---|---|
| U2 | S12 | 6. Salidas BI/ML distribuidas (evidencia completa) | 12% | ≥70% en B o superior | Pendiente |
| U3 | S15 | 1. Integración end-to-end (integración, junto con CE042/CE043) | 20% | ≥70% en B o superior | Pendiente |
| U3 | S15 | 6. Sustentación técnica (cierre, junto con CE042/CE043) | 25% | ≥70% en B o superior | Pendiente |

**Tabla 4. Competencia General — Investigación e Innovación (evidencia transversal)**

| Unidad | Sesión | Criterio de rúbrica | Peso en la unidad | Umbral de logro | Última medición |
|---|---|---|---:|---|---|
| U1 | S5 | 7. Sustentación | 20% | ≥70% en B o superior | Pendiente |
| U2 | S12 | 8. Sustentación | 20% | ≥70% en B o superior | Pendiente |
| U3 | S15 | 6. Sustentación técnica (también CG, ver Tabla 1-3) | 25% | ≥70% en B o superior | Pendiente |

**Criterios sin Nivel propio (infraestructura de apoyo, no aparecen en las tablas 1-4):** U2 criterios 1-5 (Kafka empresarial, Kafka IoT, streaming Spark, observabilidad, costos) y U3 criterios 2-5 (DataOps, documentación, hardening, demo) — hacen posible generar la evidencia de nivel, pero no son en sí mismos evidencia de una competencia. Siguen calificándose en la rúbrica de unidad; solo no entran en esta matriz de trazabilidad.

## 3. Umbral de logro

Mismo criterio que DIST: **≥70% de estudiantes con nivel B (15/20) o superior** por criterio, como umbral inicial uniforme antes de tener datos históricos propios.

## 4. Cómo se recolecta la medición (pendiente de implementar)

Igual que en DIST — ver [`matriz-competencias.md` de DIST, sección 4](https://github.com/262dist/pagatu/blob/main/docs/proyecto-sello/matriz-competencias.md#4-c%C3%B3mo-se-recolecta-la-medici%C3%B3n-pendiente-de-implementar) para el detalle. Falta un registro por estudiante y criterio, separado de la rúbrica, que alimente esta matriz.

## 5. Cierre del ciclo de mejora (pendiente de primera medición)

| Semestre medido | Criterio bajo umbral | Resultado | Acción tomada | Semestre de aplicación |
|---|---|---|---|---|
| — | — | — | — | — |
