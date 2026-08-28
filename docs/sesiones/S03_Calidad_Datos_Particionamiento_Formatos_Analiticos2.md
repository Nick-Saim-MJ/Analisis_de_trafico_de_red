# S03 - Tratamiento de DataFrames, calidad y formatos analíticos

## 1. Propósito

Esta guía desarrolla, de forma práctica, los contenidos del PDF **desde “3. Tratamiento de DataFrames” en adelante**: filtrado, orden, duplicados, valores faltantes, escritura, particionamiento y optimización.

Al finalizar podrás construir un flujo reproducible que:

1. lea datos con un esquema controlado;
2. filtre y ordene sin perder trazabilidad;
3. diagnostique duplicados antes de eliminarlos;
4. trate nulos según reglas de negocio;
5. escriba una salida Parquet particionada;
6. compruebe la lectura y el *partition pruning*;
7. use caché solo cuando aporte valor.

> Principio de trabajo: limpiar datos no significa borrar todo lo incómodo. Cada modificación debe responder a una regla explícita y debe poder medirse antes y después.

## 2. Preparación del laboratorio

### 2.1 Estructura esperada

```text
BigData/
├── Origen/
│   ├── customers.csv
│   ├── articles.csv
│   └── transactions.parquet
├── artifacts/
└── compose.yml
```

### 2.2 Sesión de Spark e importaciones

```python
from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from pyspark.storagelevel import StorageLevel

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("S03-calidad-particionamiento")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

ORIGEN_DATOS = "./Origen"
ARTIFACTS = "./artifacts/s03"
```

### 2.3 Lectura con esquema explícito

El esquema explícito evita que Spark interprete de manera diferente una misma columna entre ejecuciones. Los identificadores y códigos postales se conservan como texto para no perder ceros iniciales.

```python
customer_schema = StructType([
    StructField("customer_id", StringType(), nullable=True),
    StructField("FN", DoubleType(), nullable=True),
    StructField("Active", DoubleType(), nullable=True),
    StructField("club_member_status", StringType(), nullable=True),
    StructField("fashion_news_frequency", StringType(), nullable=True),
    StructField("age", IntegerType(), nullable=True),
    StructField("postal_code", StringType(), nullable=True),
])

df_customers = (
    spark.read
    .option("header", True)
    .schema(customer_schema)
    .csv(f"{ORIGEN_DATOS}/customers.csv")
)

df_customers.printSchema()
print(f"Filas: {df_customers.count():,}")
print(f"Columnas: {len(df_customers.columns)}")
df_customers.show(5, truncate=False)
```

Antes de continuar, valida que existan las columnas requeridas:

```python
columnas_requeridas = {
    "customer_id",
    "FN",
    "Active",
    "club_member_status",
    "fashion_news_frequency",
    "age",
    "postal_code",
}

faltantes = columnas_requeridas - set(df_customers.columns)
if faltantes:
    raise ValueError(f"Faltan columnas obligatorias: {sorted(faltantes)}")
```

## 3. Tratamiento de DataFrames

### 3.1 Filtrado con `filter()` y `where()`

`filter()` y `where()` son equivalentes. Ambos reciben una expresión SQL o una condición construida con columnas.

```python
# Expresión SQL
df_customers.filter("age > 30").show(5)
df_customers.where("club_member_status = 'ACTIVE'").show(5)

# API de columnas: preferible para condiciones dinámicas o compuestas
df_customers.filter(F.col("age") > 30).show(5)

df_customers.filter(
    F.col("age").between(25, 35)
    & F.col("club_member_status").eqNullSafe("ACTIVE")
).show(5)

df_customers.filter(
    F.col("fashion_news_frequency").isin("Regularly", "Monthly")
).show(5)
```

Nota: `between(25, 35)` incluye ambos extremos. La expresión `(age > 25) & (age < 35)` no lo hace.

#### Nulos y contenido textual

```python
df_customers.filter(F.col("club_member_status").isNotNull()).show(5)
df_customers.filter(F.col("fashion_news_frequency").isNull()).show(5)

df_customers.filter(F.col("postal_code").startswith("28")).show(5)
df_customers.filter(F.col("postal_code").contains("56")).show(5)
df_customers.filter(F.col("postal_code").endswith("00")).show(5)
```

En filtros compuestos usa `&`, `|` y `~`, con cada comparación entre paréntesis. Los operadores de Python `and`, `or` y `not` no operan sobre columnas de Spark.

### 3.2 Orden con `orderBy()` y `sort()`

`sort()` es un alias de `orderBy()`.

```python
df_customers.orderBy("age").show(5)
df_customers.orderBy(F.col("age").desc_nulls_last()).show(5)

df_customers.orderBy(
    F.col("club_member_status").asc_nulls_last(),
    F.col("age").desc_nulls_last(),
).show(5)

df_customers.sort(
    F.length("postal_code").desc(),
    F.col("postal_code").asc(),
).show(5)
```

Un DataFrame no tiene un orden global garantizado salvo que se aplique `orderBy()` al resultado que se va a mostrar o guardar. Ordenar provoca un *shuffle* global, por lo que debe evitarse si el orden no es realmente necesario.

## 4. Duplicados: diagnosticar antes de eliminar

“Duplicado” depende de la clave de negocio. Dos filas pueden ser idénticas, compartir un `customer_id` o pertenecer al mismo `product_code`; no son la misma pregunta.

### 4.1 Filas completamente repetidas

```python
filas_originales = df_customers.count()
filas_distintas = df_customers.distinct().count()

print(f"Duplicados exactos: {filas_originales - filas_distintas:,}")

df_sin_duplicados_exactos = df_customers.dropDuplicates()
```

`distinct()` y `dropDuplicates()` sin argumentos son equivalentes.

### 4.2 Duplicados por clave

```python
duplicados_customer_id = (
    df_customers
    .filter(F.col("customer_id").isNotNull())
    .groupBy("customer_id")
    .count()
    .filter(F.col("count") > 1)
    .orderBy(F.col("count").desc())
)

duplicados_customer_id.show(20, truncate=False)
```

Excluir los identificadores nulos evita agrupar todas las ausencias como si fueran una misma entidad.

### 4.3 Deduplicación determinista con ventana

`dropDuplicates(["customer_id"])` conserva una fila cualquiera por identificador. Cuando importa cuál sobrevive, usa una ventana y define un desempate total.

```python
ventana_cliente = Window.partitionBy("customer_id").orderBy(
    F.col("age").desc_nulls_last(),
    F.col("postal_code").asc_nulls_last(),
)

df_customers_deduplicado = (
    df_customers
    .withColumn("_fila", F.row_number().over(ventana_cliente))
    .filter(F.col("_fila") == 1)
    .drop("_fila")
)
```

En producción, el criterio habitual sería una marca temporal de actualización, por ejemplo `updated_at DESC`, más una segunda columna estable para resolver empates.

### 4.4 Variantes de producto no son duplicados

```python
df_articles = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(f"{ORIGEN_DATOS}/articles.csv")
)

descripciones_repetidas = (
    df_articles
    .filter(F.col("detail_desc").isNotNull())
    .groupBy("detail_desc")
    .agg(
        F.count("*").alias("filas"),
        F.countDistinct("article_id").alias("articulos_distintos"),
    )
    .filter(F.col("filas") > 1)
    .orderBy(F.col("filas").desc())
)

descripciones_repetidas.show(10, truncate=False)
```

Una descripción repetida no implica que la fila esté duplicada: varios `article_id` pueden representar variantes válidas. Si el objetivo fuera obtener un representante por producto, la clave de negocio sería `product_code`:

```python
ventana_producto = Window.partitionBy("product_code").orderBy(
    F.col("article_id").asc()
)

df_un_articulo_por_producto = (
    df_articles
    .withColumn("_fila", F.row_number().over(ventana_producto))
    .filter(F.col("_fila") == 1)
    .drop("_fila")
)
```

## 5. Datos faltantes

### 5.1 Perfil de nulos

Cuenta nulos y calcula su porcentaje en una única agregación:

```python
total = df_customers.count()

perfil_nulos = df_customers.agg(*[
    F.sum(F.col(c).isNull().cast("long")).alias(c)
    for c in df_customers.columns
])

perfil_nulos.show(vertical=True, truncate=False)

porcentaje_nulos = df_customers.agg(*[
    F.round(
        100.0 * F.sum(F.col(c).isNull().cast("long")) / F.lit(total),
        2,
    ).alias(c)
    for c in df_customers.columns
])

porcentaje_nulos.show(vertical=True, truncate=False)
```

En datos textuales conviene revisar también cadenas vacías y espacios, porque no cuentan como `NULL`:

```python
df_customers.filter(
    F.col("customer_id").isNull()
    | (F.trim(F.col("customer_id")) == "")
).count()
```

### 5.2 Imputación con reglas de negocio

```python
df_customers_imputado = df_customers.na.fill({
    "FN": 0.0,
    "Active": 0.0,
    "club_member_status": "UNKNOWN",
    "fashion_news_frequency": "NONE",
})
```

No se rellena `age` con `0`: cero sería un dato válido pero falso. La edad puede conservarse nula, descartarse para un análisis que la requiera o imputarse con una técnica estadística documentada.

### 5.3 Eliminación selectiva

```python
df_customers_valido = df_customers_imputado.na.drop(
    subset=["customer_id"]
)
```

Evita `df.na.drop()` sin argumentos salvo que la regla exija que todas las columnas estén completas: elimina cualquier fila con al menos un nulo y puede descartar gran parte del dataset.

### 5.4 Controles posteriores

```python
antes = df_customers.count()
despues = df_customers_valido.count()

print(f"Filas antes: {antes:,}")
print(f"Filas después: {despues:,}")
print(f"Filas eliminadas: {antes - despues:,}")

assert df_customers_valido.filter(F.col("customer_id").isNull()).count() == 0
```

## 6. Escritura en formatos analíticos

### 6.1 CSV, JSON y Parquet

```python
muestra = df_customers_valido.limit(1000)

(
    muestra.write
    .mode("overwrite")
    .option("header", True)
    .csv(f"{ARTIFACTS}/muestra_csv")
)

muestra.write.mode("overwrite").json(f"{ARTIFACTS}/muestra_json")
muestra.write.mode("overwrite").parquet(f"{ARTIFACTS}/muestra_parquet")
```

Spark escribe directorios con uno o más archivos `part-*`, no un único archivo. Parquet suele ser la mejor salida analítica porque es columnar, conserva tipos, admite compresión y permite *predicate pushdown*.

### 6.2 Escritura Parquet particionada

Una buena columna de partición tiene cardinalidad baja o moderada y aparece con frecuencia en filtros. No uses identificadores únicos como `customer_id`: producirían demasiadas carpetas pequeñas.

```python
salida_particionada = f"{ARTIFACTS}/customers_particionado"

(
    df_customers_valido
    .repartition("club_member_status")
    .write
    .mode("overwrite")
    .partitionBy("club_member_status")
    .parquet(salida_particionada)
)
```

`partitionBy()` define la organización física en carpetas. `repartition()` reorganiza los datos en memoria antes de escribir. No debe asumirse que `repartition(4)` garantiza exactamente cuatro archivos dentro de cada carpeta: el resultado depende de la distribución, las claves de partición y la planificación de Spark.

### 6.3 `coalesce()` frente a `repartition()`

| Operación | Produce *shuffle* | Puede aumentar particiones | Uso habitual |
|---|---:|---:|---|
| `coalesce(n)` | normalmente no | no | reducir archivos al final de un flujo pequeño |
| `repartition(n)` | sí | sí | redistribuir de manera más uniforme |
| `repartition(*cols)` | sí | sí | reunir claves antes de una operación o escritura |

`coalesce(1)` fuerza un cuello de botella y solo es razonable para resultados pequeños destinados a intercambio manual.

## 7. Lectura y verificación

### 7.1 Leer la salida

```python
df_verificacion = spark.read.parquet(salida_particionada)

df_verificacion.printSchema()
print(f"Filas leídas: {df_verificacion.count():,}")

assert df_verificacion.count() == df_customers_valido.count()
```

Spark reconstruye `club_member_status` a partir de las carpetas creadas por `partitionBy()`.

### 7.2 Verificar *partition pruning*

```python
df_active = df_verificacion.filter(
    F.col("club_member_status") == "ACTIVE"
)

df_active.explain(mode="formatted")
```

En el plan físico busca `PartitionFilters`. Esto demuestra *partition pruning*: Spark descarta carpetas completas que no cumplen el filtro.

No es lo mismo que `PushedFilters`:

- `PartitionFilters` evita leer particiones físicas completas.
- `PushedFilters` delega filtros al lector Parquet para reducir los datos leídos dentro de los archivos seleccionados.

## 8. Evaluación diferida, caché y persistencia

Las transformaciones como `filter()`, `select()` y `withColumn()` son diferidas. Spark crea un plan y solo lo ejecuta cuando aparece una acción como `count()`, `show()`, `collect()` o `write`.

### 8.1 Caché con propósito

Usa caché si el mismo resultado costoso será reutilizado en varias acciones:

```python
df_customers_valido = df_customers_valido.persist(
    StorageLevel.MEMORY_AND_DISK
)

# La primera acción materializa la persistencia.
df_customers_valido.count()

df_customers_valido.groupBy("club_member_status").count().show()
df_customers_valido.select("age").summary().show()

df_customers_valido.unpersist()
```

No caches automáticamente todo DataFrame. La persistencia consume memoria y disco; si el resultado se usa una sola vez, suele añadir costo sin beneficio.

## 9. Pipeline integrado

El siguiente bloque resume el flujo completo sin esconder las decisiones principales:

```python
df_salida = (
    df_customers
    .filter(
        F.col("customer_id").isNotNull()
        & (F.trim(F.col("customer_id")) != "")
    )
    .na.fill({
        "FN": 0.0,
        "Active": 0.0,
        "club_member_status": "UNKNOWN",
        "fashion_news_frequency": "NONE",
    })
)

ventana = Window.partitionBy("customer_id").orderBy(
    F.col("age").desc_nulls_last(),
    F.col("postal_code").asc_nulls_last(),
)

df_salida = (
    df_salida
    .withColumn("_fila", F.row_number().over(ventana))
    .filter(F.col("_fila") == 1)
    .drop("_fila")
)

(
    df_salida
    .repartition("club_member_status")
    .write
    .mode("overwrite")
    .partitionBy("club_member_status")
    .parquet(f"{ARTIFACTS}/customers_curado")
)
```

## 10. Evidencia mínima del laboratorio

El notebook o informe debe mostrar:

1. esquema y cantidad inicial de filas;
2. dos filtros y dos órdenes distintos;
3. diagnóstico de duplicados exactos y por clave;
4. criterio determinista de deduplicación;
5. conteo y porcentaje de nulos;
6. reglas de imputación y filas eliminadas;
7. escritura Parquet particionada;
8. lectura con conteo reconciliado;
9. plan físico con `PartitionFilters`;
10. liberación de la persistencia, si fue utilizada.

## 11. Preguntas de reflexión

1. ¿Por qué una descripción repetida no demuestra por sí sola que dos artículos sean duplicados?
2. ¿Qué riesgo introduce `dropDuplicates(["customer_id"])` si no existe un criterio para elegir la fila conservada?
3. ¿Por qué rellenar `age` con cero puede ser peor que conservar el nulo?
4. ¿Qué diferencia práctica existe entre `partitionBy()`, `repartition()` y `coalesce()`?
5. ¿Cómo distingues `PartitionFilters` de `PushedFilters` en un plan de ejecución?
6. ¿Cuándo la caché reduce el tiempo total y cuándo solo consume recursos?

## 12. Lista de verificación técnica

- [ ] El esquema está definido o validado explícitamente.
- [ ] Los identificadores se conservan como texto.
- [ ] Cada eliminación de filas tiene una regla y un conteo antes/después.
- [ ] La deduplicación usa una clave de negocio y un desempate determinista.
- [ ] Los nulos se tratan por columna, no con una regla indiscriminada.
- [ ] La columna de partición tiene cardinalidad razonable.
- [ ] La salida fue leída de vuelta y reconciliada.
- [ ] El plan confirma *partition pruning*.
- [ ] La caché se libera con `unpersist()`.

