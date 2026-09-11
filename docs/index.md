# Análisis de tráfico de red del campus universitario

Proyecto Sello del curso **Big Data · lambda26** — equipo **LLSW3** (sección GU).

Sistema Big Data de arquitectura **Lambda** para caracterizar el tráfico de red del campus universitario UPeU Juliaca a partir de flujos capturados con **Suricata** (dataset propio, ~400 000 registros, 83 columnas, estilo NetFlow/CICFlowMeter). El sistema combina una capa **batch** (histórico de flujos, procesado con PySpark y Spark MLlib) y una capa de **velocidad** (flujos publicados en vivo vía Kafka, contenido de Unidad 2) para responder una sola pregunta central:

> ¿Cómo caracterizar el comportamiento de los flujos de tráfico de red del campus — volumen, duración, tipo de servicio y dirección dominante — para anticipar la carga esperada de la red y detectar patrones que se aparten de lo habitual, apoyando la gestión de capacidad y la vigilancia del equipo de TI?

## Equipo

| Integrante | Dimensión (U1 batch + U2 streaming) |
|---|---|
| Nick Saim Mayta Jara | Volumen de tráfico por flujo (`bytes_per_s`) — arquitectura Lambda y coordinación técnica |
| Jhan Logan Ramos Quispe | Duración del flujo de red (`flow_duration`) — Streaming / Kafka |
| Henyelrey Lucio Garcia Chura | Tipo de servicio del flujo vs. catálogo IANA — Batch / Spark y fuentes externas |
| David Romero Nina | Dirección dominante del flujo (`down_up_ratio`) — BI / ML |

Las ocho dimensiones (dos por integrante) convergen en un único tablero de "salud y comportamiento del tráfico del campus", con el detalle completo declarado en el [Brief técnico-analítico](proyecto-sello/brief.md) (hito S2).

## Contenido del sitio

- [Brief técnico-analítico del Proyecto Sello](proyecto-sello/brief.md) — declaración del sistema Big Data, dominio, arquitectura Lambda y las 8 fichas de dimensión.
- [Informe — Unidad 1](productos/Informe_Unidad_1.md) — pipeline batch de ETL distribuido, resultados consolidados y salidas analíticas del Producto U1.
- [Aplicación de CRISP-DM al proyecto](productos/Aplicacion_de_CRISPDM_al_Proyecto.md) — cómo se aplicó la metodología CRISP-DM en los cuatro notebooks del equipo.
- **Contribución por integrante** — notebook y evidencia individual de cada dimensión U1:
    - [Nick](contribuidores/u1_producto_nick.md)
    - [Henyelrey](contribuidores/u1_producto_henyelrey.md)
    - [Jhan](contribuidores/u1_producto_jhan.md)
    - [David](contribuidores/u1_producto_david.md)

## Alcance

Este proyecto cubre el monitoreo (histórico, y en Unidad 2 en tiempo real) de cuatro variables de comportamiento del tráfico de red del campus — volumen, duración, tipo de servicio y dirección dominante — con modelos predictivos U1 por integrante y, en Unidad 2, un tablero Grafana unificado.

No incluye la clasificación definitiva de ataques/intrusiones ni el etiquetado de seguridad del tráfico (alcance aparte del trabajo de tesis de Nick, framework NIDS basado en Mamba-3), no reemplaza a Suricata ni a ningún IDS/IPS de producción, y no incluye bloqueo o actuación automática sobre el tráfico.
