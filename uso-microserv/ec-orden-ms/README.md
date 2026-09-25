# ec-orden-ms

Microservicio Spring Boot que registra órdenes en PostgreSQL y publica un
evento `orden.creada` en Kafka por cada una — el productor real del flujo de
eventos empresariales (S06).

## Requisitos

- Java 21 instalado (`winget install --id EclipseAdoptium.Temurin.21.JDK --exact`).
  No hace falta instalar Maven aparte: el proyecto trae su propio Maven
  Wrapper (`mvnw.cmd`).
- Docker Desktop corriendo.
- Kafka arriba (`kafka/`, ver su propio README) — este microservicio se
  conecta a `localhost:49092` en DEV, y a la red `lambda26-kafka-net` en
  Docker (PROD local).

## Stack

Spring Boot **4.1.1**, Java **21**, `spring-boot-starter-webmvc`, Spring for
Apache Kafka, Spring Data JPA, PostgreSQL, Lombok, springdoc-openapi 3.1.0
(Swagger). Paquete base: `pe.edu.upeu.ec.orden`.

## Servicios y puertos

| Servicio | URL/Puerto |
|---|---|
| App | `http://localhost:49021` |
| PostgreSQL (DEV) | `localhost:49020` |
| Kafka (interno Docker) | `kafka:9092` |

`49020` (Postgres) y `49021` (la app) son puertos distintos a propósito — si
alguna vez terminan con el mismo número, uno de los dos no puede arrancar
(compiten por el mismo puerto en el host).

Contenedores (nombres sin `-ms` — ese sufijo ya lo lleva el `artifactId`, no
hace falta repetirlo):

- `lambda26-ec-orden`
- `lambda26-postgres-ec-orden` (PROD) / `lambda26-postgres-ec-orden-dev` (DEV)

## Uso — DEV (recomendado para desarrollar)

Levanta Kafka primero (ver `kafka/README.md`), luego solo Postgres para este
servicio:

```powershell
docker compose -f compose-dev.yml up -d
```

Verifica que la base de datos está lista:

```powershell
docker exec -it lambda26-postgres-ec-orden-dev psql -U ecom -d db_ec_orden_ms -c "\dt"
```

Ejecuta la aplicación (perfil `dev` activo por defecto, `application.yml`):

```powershell
.\mvnw.cmd spring-boot:run
```

## Uso — Docker completo (app + Postgres)

```powershell
docker compose up -d --build
```

## Probar

Crear una orden:

```powershell
Invoke-RestMethod -Method Post -Uri "http://localhost:49021/ordenes" `
  -ContentType "application/json" `
  -Body '{"usuarioId":1,"total":100}'
```

Listar órdenes:

```powershell
Invoke-RestMethod -Method Get -Uri "http://localhost:49021/ordenes"
```

Swagger:

```text
http://localhost:49021/swagger-ui/index.html
```

## Qué esperar

El `POST` responde `201` con la orden guardada (`estado: "PENDIENTE"`). En la
consola donde corre `.\mvnw.cmd spring-boot:run` aparece una línea de log
`component=producer ... status=published`, con `partition`/`offset` reales —
confírmalo también en Kafka UI (`http://localhost:48085`), topic
`orden-eventos`. Si el envío a Kafka falla, la orden queda igual guardada en
PostgreSQL (no hay una transacción real que una ambos sistemas) — revisa el
log, ahí sale `status=error` con el motivo.

Este servicio publica en el topic `orden-eventos`; `ec-pago-ms` lo consume.
