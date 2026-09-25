# ec-pago-ms

Microservicio Spring Boot que consume `orden.creada` desde Kafka, procesa un
pago simulado (regla determinista: montos menores a 1000 se aprueban) y
publica el resultado (`pago.aprobado`/`pago.rechazado`) — el consumidor y
segundo productor del flujo de eventos empresariales (S06).

## Requisitos

- Java 21 instalado (`winget install --id EclipseAdoptium.Temurin.21.JDK --exact`).
  No hace falta instalar Maven aparte: el proyecto trae su propio Maven
  Wrapper (`mvnw.cmd`).
- Docker Desktop corriendo.
- Kafka arriba (`kafka/`, ver su propio README).
- **`ec-orden-ms` corriendo y publicando** — sin órdenes creadas, este
  servicio no tiene nada que consumir (ver `uso-microserv/ec-orden-ms/README.md`).

## Stack

Spring Boot **4.1.1**, Java **21**, `spring-boot-starter-webmvc`, Spring for
Apache Kafka (con `ErrorHandlingDeserializer` — un mensaje malformado no
tumba el consumer, queda registrado en el log), Spring Data JPA, PostgreSQL,
Lombok, springdoc-openapi 3.1.0 (Swagger). Paquete base:
`pe.edu.upeu.ec.pago`.

## Servicios y puertos

| Servicio | URL/Puerto |
|---|---|
| App | `http://localhost:49031` |
| PostgreSQL (DEV) | `localhost:49030` |
| Kafka (interno Docker) | `kafka:9092` |

Contenedores:

- `lambda26-ec-pago`
- `lambda26-postgres-ec-pago` (PROD) / `lambda26-postgres-ec-pago-dev` (DEV)

## Uso — DEV (recomendado para desarrollar)

Con Kafka y `ec-orden-ms` ya corriendo, levanta solo Postgres para este
servicio:

```powershell
docker compose -f compose-dev.yml up -d
```

Verifica que la base de datos está lista:

```powershell
docker exec -it lambda26-postgres-ec-pago-dev psql -U ecom -d db_ec_pago_ms -c "\dt"
```

Ejecuta la aplicación:

```powershell
.\mvnw.cmd spring-boot:run
```

## Uso — Docker completo (app + Postgres)

```powershell
docker compose up -d --build
```

## Probar

Con `ec-orden-ms` corriendo, crea una orden (dispara todo el flujo):

```powershell
Invoke-RestMethod -Method Post -Uri "http://localhost:49021/ordenes" `
  -ContentType "application/json" `
  -Body '{"usuarioId":2,"total":150}'
```

Repite con `total` mayor o igual a `1000` para forzar `pago.rechazado` — la
regla es determinista, no depende del azar.

Consultar pagos:

```powershell
Invoke-RestMethod -Method Get -Uri "http://localhost:49031/pagos"
```

Endpoint de salud:

```text
GET http://localhost:49031/pagos/saludo
```

## Qué esperar

En la consola de `ec-pago-ms` aparecen tres líneas por cada orden procesada:
`component=consumer` (con `latencyMs`, el tiempo entre que `ec-orden-ms`
publicó y este servicio lo recibió), `component=processor` (con el
`estadoPago` decidido) y `component=producer` (confirmando la publicación en
`pago-eventos`, con `partition`/`offset` reales). Verifica en Kafka UI
(`http://localhost:48085`) el topic `pago-eventos` y el consumer group
`ec-pago-ms-group`, con su *lag* en `orden-eventos` idealmente en `0`.

Este servicio consume `orden-eventos` y publica `pago-eventos`.
