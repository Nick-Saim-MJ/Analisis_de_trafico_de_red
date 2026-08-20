# Laboratorio PySpark

Entorno local para ejecutar los notebooks del curso con Spark y Jupyter.

## Clonar

```bash
git clone https://github.com/262bigdata/lambda26.git
cd lambda26/pyspark
```

## Carpetas

- `notebooks/`: cuadernos fuente ejecutables.
- `data/`: archivos de entrada para las practicas.
- `artifacts/`: resultados generados por Spark, como salidas Parquet,
  checkpoints y modelos.
- `Dockerfile`: imagen base del laboratorio.

## Uso

Desde `lambda26/pyspark`:

```powershell
docker compose up -d
```

La integración con Kafka (módulo `kafka/` y el override
`pyspark/compose.kafka.yml`) todavía no existe en este repositorio: se crea
recién en la Unidad 2 (S6), cuando el curso pasa de batch a streaming. Hasta
entonces, este entorno corre solo (PySpark + Jupyter, sin Kafka).

Luego abre JupyterLab:

```text
http://localhost:4488/lab?token=sintoken
```

Tambien puedes entrar a Jupyter Notebook:

```text
http://localhost:4488/?token=sintoken
```

Los notebooks quedan montados dentro del contenedor en:
```text
/opt/notebooks
```

Los datos quedan disponibles en:

```text
/opt/data
```

Los artefactos generados se escriben en:

```text
/opt/artifacts
```

### Alternativa con imagen oficial de PySpark + Jupyter

Tambien puedes levantar un entorno PySpark directamente con la imagen oficial
[`jupyter/pyspark-notebook`](https://hub.docker.com/r/jupyter/pyspark-notebook). Diferencia de peso: la imagen
personalizada de `lambda26` pesa ~1.9 GB, mientras que `jupyter/pyspark-notebook`
pesa ~6.9 GB — considera tu espacio en disco antes de elegir esta alternativa:

```yaml
# compose.yml
services:
    pyspark:
        image: jupyter/pyspark-notebook
        ports:
            - 4489:8888
            - 4041:4040
        environment:
            - JUPYTER_TOKEN=sintoken
        volumes:
            - ./:/home/jovyan
```

Puertos distintos a los de `compose.yml` (4488/4040) para poder levantar ambos entornos sin conflicto si hiciera falta.

Para levantar este entorno:

```powershell
docker compose up -d
```

Luego accede a JupyterLab:

```text
http://localhost:4489/lab?token=sintoken
```

O usa Jupyter Notebook:

```text
http://localhost:4489/?token=sintoken
```

## Notebooks

La carpeta `notebooks/` empieza vacía: los cuadernos se crean progresivamente,
uno por sesión, a medida que el curso avanza (verificación mínima en S1,
ETL y formatos analíticos en S2-S3, ML distribuido en S4, streaming e
inferencia desde S8-S10).
