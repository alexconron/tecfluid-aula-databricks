# Databricks notebook source
# Paso previo: en Jobs & Pipelines, abre la ingesta original de Drive.
# Confirma archivo y pestaña KAM v2, tarea de carga y tabla Delta Bronze de salida.
# Anota la ejecución exitosa y la versión Delta que escribió.
from pyspark.sql import functions as F, Window
import re

bronze = "<catalogo>.<esquema_bronze>.<tabla_kam_v2>"
version = None  # Número de versión Delta que corresponde al corte de Drive.
if "<" in bronze or version is None:
    raise ValueError("Completa el nombre de Bronze y la versión del corte.")
datos = spark.read.option("versionAsOf", version).table(bronze)
datos.printSchema()
display(datos.limit(20))
print("Filas Bronze:", datos.count())

# COMMAND ----------

# Escribe etiquetas reales de Bronze. Elige un importe obligatorio y una clave
# que, según negocio, deba ser única. La Silver del ejercicio proyecta estos campos.
claves = ["<campo de la clave>"]  # Agrega campos si la clave es compuesta.
campo_importe = "<columna de importe>"
decimal = ","  # "," para 1.200,50 o "." para 1,200.50; confirmar en el origen.
if not claves or len(set(claves)) != len(claves) or campo_importe in claves:
    raise ValueError("Revisa los campos distintos que componen la clave y el importe.")
if set(claves + [campo_importe]) - set(datos.columns) or decimal not in (",", "."):
    raise ValueError("Revisa las columnas de Bronze y el separador decimal.")

def columna(nombre):
    return F.col("`" + nombre.replace("`", "``") + "`")

nombres = [f"clave_{i+1}" for i in range(len(claves))]
base = datos.select(
    *[F.trim(columna(c).cast("string")).alias(n) for c, n in zip(claves, nombres)],
    columna(campo_importe).cast("string").alias("importe_original")
)
print("Campos de la clave:", dict(zip(nombres, claves)))

# COMMAND ----------

# Conserva el texto original. Si Bronze ya guarda un tipo numérico, Spark lo
# representa con punto decimal al convertirlo a texto.
texto = F.trim(F.col("importe_original"))
miles = "." if decimal == "," else ","
d, m = re.escape(decimal), re.escape(miles)
patron = rf"^[+-]?(?:[0-9]+|[0-9]{{1,3}}(?:{m}[0-9]{{3}})+)(?:{d}[0-9]{{1,2}})?$"
normalizado = F.regexp_replace(F.regexp_replace(texto, m, ""), d, ".")
tipado = (base.withColumn("normalizado", F.when(texto.rlike(patron), normalizado))
    .withColumn("importe", F.expr("try_cast(normalizado AS DECIMAL(18,2))"))
    .withColumn("filas_por_clave", F.count("*").over(Window.partitionBy(*nombres))))
clave_vacia = F.lit(False)
for nombre in nombres:
    clave_vacia = clave_vacia | F.col(nombre).isNull() | (F.col(nombre) == "")
revision = tipado.withColumn("motivo", F.concat_ws(" / ",
    F.when(clave_vacia, "clave vacía"),
    F.when(texto.isNull() | (texto == ""), "importe vacío"),
    F.when((texto != "") & F.col("importe").isNull(), "formato o precisión inválidos"),
    F.when(F.col("filas_por_clave") > 1, "clave repetida: revisar")
))
validas = revision.where(F.col("motivo") == "").select(*nombres, "importe")
observadas = revision.where(F.col("motivo") != "")
display(observadas.select(*nombres, "importe_original", "motivo").limit(50))
assert datos.count() == validas.count() + observadas.count()
print("Válidas:", validas.count(), "Para revisar:", observadas.count())

# COMMAND ----------

# Ejecuta tras revisar los controles y el total de un período y moneda con Drive.
# Elige un destino nuevo dentro del esquema Silver de capacitación.
silver = "<catalogo_de_clase>.<silver_de_clase>.<tabla_personal>"
if "<" in silver or silver.lower() == bronze.lower():
    raise ValueError("Completa un destino Silver de capacitación distinto del origen.")
if not validas.take(1):
    raise ValueError("Revisa los diagnósticos antes de publicar una Silver vacía.")
validas.write.format("delta").mode("errorifexists").saveAsTable(silver)
salida = spark.table(silver)
salida.printSchema()
assert salida.count() == validas.count()
display(salida.limit(20))
