# Databricks notebook source
# Clase 2 · Ejecutar en orden. La muestra ficticia permite ensayar antes de recibir el archivo.
from pyspark.sql import functions as F

ruta_csv = ""  # Pega aquí la ruta del CSV de la pestaña v2 en el volumen de práctica.
if ruta_csv:
    datos = (spark.read.option("header", True).option("multiLine", True)
             .option("escape", '"').option("mode", "FAILFAST").csv(ruta_csv))
else:
    datos = spark.createDataFrame([
        ("2026-10", "A", "101", "100.50"),
        ("2026-10", "A", "101", "100.50"),
        ("2026-10", "B", "102", ""),
        ("2026-10", "B", "103", "1.200,50"),
        ("2026-10", "C", "104", "0"),
    ], ["periodo", "kam", "documento", "importe"])
print("Origen:", ruta_csv or "MUESTRA FICTICIA")
print("Filas:", datos.count(), "Columnas:", len(datos.columns))
print(datos.columns)
display(datos.limit(10))

# COMMAND ----------

# Con el CSV real, usa los nombres exactos y acuerda qué identifica una fila.
clave = ["periodo", "kam", "documento"]
importe = "importe"
def columna(nombre):
    return F.col("`" + nombre.replace("`", "``") + "`")

faltantes = set(clave + [importe]) - set(datos.columns)
if faltantes:
    raise ValueError(f"Revisa los encabezados antes de continuar: {sorted(faltantes)}")

grupos = datos.groupBy(*[columna(c) for c in clave]).count()
display(grupos.where("count > 1").orderBy(F.desc("count")))

# COMMAND ----------

# El punto decimal sin separadores de miles es la convención de este ensayo.
# Revisa la convención real con el dueño del dato antes de normalizar importes.
texto = F.trim(columna(importe))
revision = datos.withColumn("control_formato", F.when(texto.isNull() | (texto == ""), "vacío")
    .when(texto.rlike(r"^[+-]?[0-9]+([.][0-9]{1,2})?$"), "formato esperado")
    .otherwise("revisar formato"))
display(revision.groupBy("control_formato").count())
display(revision.where("control_formato <> 'formato esperado'").limit(20))

# COMMAND ----------

# Registra el total visible en Drive para el mismo rango y sin contar el encabezado.
# Si hay diferencias, revisa filtros, filas de totales y la clave antes de descartar filas.
print("Filas después de validar:", revision.count())
