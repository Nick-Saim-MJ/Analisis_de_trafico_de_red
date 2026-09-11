# Laboratorio PySpark — Producto U1 (equipo LLSW3)

Entorno local para ejecutar los notebooks del Producto U1 (análisis de tráfico de red del campus) con Spark y Jupyter.

## Clonar

```bash
git clone https://github.com/Nick-Saim-MJ/Analisis_de_trafico_de_red.git
cd Analisis_de_trafico_de_red/pyspark
```

## Carpetas

`pyspark/` separa la **configuración** (lo que arma el entorno) del
**contenido** (lo que el equipo produce):

- `Dockerfile`, `compose.yml`, `README.md`: solo se usan desde el host, para
  levantar el contenedor — no se montan dentro de él, así que nunca aparecen
  en el explorador de archivos de Jupyter.
- `data/`, `producto/`, `artifacts/`: las tres carpetas de contenido, cada
  una montada por separado dentro del contenedor (`compose.yml`) en
  `/opt/data`, `/opt/producto` y `/opt/artifacts` respectivamente.

Contenido de cada una:

- `data/`: el dataset real del proyecto, `TRCU.csv` (~400 000 flujos de red
  capturados con Suricata, ~154 MB). **No se versiona** (ver `.gitignore`,
  supera el límite duro de GitHub de 100MB) — se copia manualmente aquí
  antes de levantar el laboratorio.
- `producto/`: los cuatro notebooks del Producto U1, uno por integrante
  (`u1_producto_nick.ipynb`, `u1_producto_jhan.ipynb`,
  `u1_producto_henyelrey.ipynb`, `u1_producto_david.ipynb`), cada uno con
  las 5 fases de CRISP-DM sobre su propia dimensión (ver el
  [Brief técnico-analítico](../docs/proyecto-sello/brief.md)).
- `artifacts/`: las salidas generadas al ejecutar los notebooks — Parquet
  particionado y modelo MLlib guardado, en una subcarpeta por integrante
  (`artifacts/nick/`, `artifacts/jhan/`, etc.). Tampoco se versiona.

## Uso

Desde `pyspark/`, copia el dataset real a `data/TRCU.csv` (no viene en el
repositorio) y levanta el laboratorio:

```powershell
docker compose up -d
```

La integración con Kafka todavía no existe en este repositorio: se crea en
Unidad 2, cuando el proyecto pasa de batch a streaming (dimensiones U2 del
brief). Hasta entonces, este entorno corre solo (PySpark + Jupyter, sin Kafka).

Luego abre JupyterLab:

```text
http://localhost:4488/lab?token=sintoken
```

Tambien puedes entrar a Jupyter Notebook:

```text
http://localhost:4488/?token=sintoken
```

**Nota:** el `Dockerfile` arranca el contenedor con `jupyter notebook`, no `jupyter lab` — igual puedes entrar a `/lab` porque `notebook` 7.x viene construido sobre el mismo servidor de JupyterLab y sirve ambas interfaces (`/lab` y `/tree`) desde el mismo proceso. No es necesario cambiar el comando para usar `/lab`.

Spark UI queda en:

```text
http://localhost:4042
```

**Nota:** Spark usa el puerto 4040 por defecto, pero `compose.yml` lo expone en tu máquina como `4042` (`"4042:4040"`) — dentro del contenedor sigue siendo 4040, solo cambia el puerto con el que accedes desde el navegador. Esto evita choques con otros servicios que puedan estar usando el 4040 en tu máquina.

`data/`, `producto/` y `artifacts/` se montan por separado dentro del
contenedor (`compose.yml`), en `/opt/data`, `/opt/producto` y
`/opt/artifacts`. Por eso cada notebook referencia sus rutas como
`/opt/data/TRCU.csv` y `/opt/artifacts/<integrante>/...`, sin necesidad de
ajustarlas al agregar contenido nuevo. Los archivos de configuración
(`Dockerfile`, `compose.yml`, etc.) se quedan fuera de estos mounts a
propósito, para que no aparezcan en el explorador de archivos de Jupyter.

### Alternativa con imagen oficial de PySpark + Jupyter

Tambien puedes levantar un entorno PySpark directamente con la imagen oficial
[`jupyter/pyspark-notebook`](https://hub.docker.com/r/jupyter/pyspark-notebook). Diferencia de peso: la imagen
personalizada de este proyecto pesa ~1.9 GB, mientras que `jupyter/pyspark-notebook`
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

Puertos distintos a los de `compose.yml` (4488/4042) para poder levantar ambos entornos sin conflicto si hiciera falta.

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

Los cuatro notebooks de `producto/` cubren las 5 fases de CRISP-DM
(comprensión del negocio → datos → preparación → modelado → evaluación)
sobre la dimensión U1 propia de cada integrante, todos sobre el mismo
dataset (`data/TRCU.csv`). El despliegue en streaming (Fase 6, dimensión
U2 de cada integrante) es contenido de Unidad 2. Ver el detalle de cada
dimensión en el [Informe de Unidad 1](../docs/productos/Informe_Unidad_1.md)
y en la [Aplicación de CRISP-DM al proyecto](../docs/productos/Aplicacion_de_CRISPDM_al_Proyecto.md).
