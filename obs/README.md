# obs

Observabilidad del pipeline de streaming (S9): Prometheus scrapeando
`kafka-exporter` (`kafka/`), y Grafana mostrando esas métricas en un tablero
real — brokers arriba, *exporter* arriba, y el *lag* del consumer group de
`uso-atmos`.

## Requisitos

- Kafka corriendo (`kafka/`, ver su propio README) — este stack se une a la
  red externa `lambda26-kafka-net`, así que falla al levantarse si Kafka no
  está arriba primero.

## Servicios y versiones

| Servicio | Imagen | URL/Puerto |
|---|---|---|
| Prometheus | `prom/prometheus:v3.14.0` | `http://localhost:49090` |
| Grafana | `grafana/grafana:11.4.0` | `http://localhost:43000` (admin/admin) |

Mismas versiones que usa `pagatu` (DIST) para estas dos piezas — reusar la
versión ya descargada en vez de fijar una distinta evita bajar imágenes
nuevas sin necesidad (ver S9, 3.1).

## Uso

Desde esta carpeta:

```powershell
docker compose up -d
```

Desde la raíz del repositorio:

```powershell
docker compose -f obs/compose.yml up -d
```

Contenedores esperados:

```powershell
docker compose ps
```

```text
lambda26-prometheus
lambda26-grafana
```

## Verificar que está arriba

**Prometheus** — `Status → Targets`, ambos en estado `UP`:

```text
http://localhost:49090
```

**Grafana** — entra con `admin`/`admin`. El *datasource* **Prometheus** ya
viene provisto automáticamente (`grafana/provisioning/datasources/`), marcado
como predeterminado — no hace falta crearlo a mano.

```text
http://localhost:43000
```

## Red compartida

```text
lambda26-kafka-net
```

Se une a la misma red externa que ya crea `kafka/compose.yml`, para poder
resolver `kafka-exporter:9308` por nombre de servicio Docker — no por
`localhost` (desde adentro del contenedor de Prometheus, `localhost` es el
propio Prometheus, no el *exporter*).

## Persistencia

`lambda26_prometheus_data` y `lambda26_grafana_data` son volúmenes nombrados:
los datos y los tableros sobreviven a un `docker compose down` normal (sin
`-v`). Si necesitas empezar de cero (por ejemplo, para rehacer el tablero de
S9 desde cero), bórralos explícitamente:

```powershell
docker compose down -v
```
