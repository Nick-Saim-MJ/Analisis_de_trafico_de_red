# Kafka

Cluster Kafka local para los laboratorios de streaming de lambda26 — el primer
componente que hay que levantar antes de tocar `uso-microserv/` o
`uso-rapido/`, porque ambos dependen de la red Docker que este `compose.yml`
crea.

## Requisitos

- Docker Desktop corriendo.

## Servicios y versiones

| Servicio | Imagen | URL/Puerto |
|---|---|---|
| Kafka broker | `apache/kafka:4.3.1` | interno `kafka:9092`, desde el host `localhost:49092` |
| Kafka UI | `ghcr.io/kafbat/kafka-ui:v1.5.0` | `http://localhost:48085` |
| Kafka Exporter | `danielqsj/kafka-exporter:v1.10.0` | `http://localhost:49308/metrics` |

Las tres imágenes van con versión fija, no `latest` — si alguien levanta este
`compose.yml` meses después, obtiene exactamente lo mismo que se probó al
escribir esta guía. `kafka-ui` usa `ghcr.io/kafbat/kafka-ui`, no
`provectuslabs/kafka-ui`: el proyecto original (Provectus) no saca una versión
real hace más de dos años — el mismo equipo continúa el desarrollo activo bajo
Kafbat.

## Uso

Desde esta carpeta:

```powershell
docker compose up -d
```

Desde la raíz del repositorio:

```powershell
docker compose -f kafka/compose.yml up -d
```

Contenedores esperados:

```powershell
docker compose ps
```

```text
lambda26-kafka
lambda26-kafka-ui
lambda26-kafka-exporter
```

## Verificar que está arriba

Abre Kafka UI y confirma que el clúster `lambda26` aparece **conectado**
(punto verde), sin ningún topic todavío — es normal, nadie ha publicado nada
todavía:

```text
http://localhost:48085
```

Verifica también el exportador de métricas (debe responder texto plano, no un
error de conexión):

```powershell
Invoke-RestMethod -Method Get -Uri "http://localhost:49308/metrics"
```

## Red compartida

```text
lambda26-kafka-net
```

`uso-microserv/ec-orden-ms`, `uso-microserv/ec-pago-ms` y
`uso-rapido/ec-eventos-py` se conectan a esta red como servicios *externos*
(`networks: - lambda26-kafka-net`, con `external: true`) — por eso Kafka
**siempre tiene que estar arriba primero**: si intentas levantar cualquiera de
esos tres sin haber corrido este `compose.yml` antes, Docker Compose falla con
un error de red no encontrada. `obs/` (Prometheus + Grafana, todavía no
existe en este repositorio) se unirá a esta misma red más adelante en el
curso, para scrapear `kafka-exporter`.

## Cambiar a Kafka Debezium

Si necesitas trabajar CDC/Debezium, reemplaza este stack por
`kafka-debezium/`. El comando `down` elimina los contenedores del stack Kafka
moderno, pero no borra las imágenes Docker.

Desde la raíz del repositorio:

```powershell
docker compose -f kafka/compose.yml down
docker compose -f kafka-debezium/compose.yml up -d
```
