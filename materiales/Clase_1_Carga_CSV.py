# Databricks notebook source
# MAGIC %md
# MAGIC # Clase 1 · Del CSV a una tabla
# MAGIC
# MAGIC Subamos el CSV a un volumen de práctica, copiemos su ruta y elijamos el esquema de capacitación. Después usaremos este notebook como tarea de un job creado durante la sesión.

# COMMAND ----------

# Completar en clase con la ruta que muestra Databricks y el esquema acordado.
archivo_origen = "/Volumes/<catalogo>/<esquema>/<volumen>/ordenes_trabajo.csv"
esquema_practica = "<catalogo>.<esquema_de_practica>"
tabla_destino = f"{esquema_practica}.tecfluid_clase_1_ordenes_demo"

if "<" in archivo_origen or "<" in esquema_practica:
    raise ValueError("Completa la ruta del CSV y el esquema de práctica antes de ejecutar.")

# COMMAND ----------

# La primera línea del CSV contiene los nombres de las columnas.
ordenes = spark.read.option("header", True).option("mode", "FAILFAST").csv(archivo_origen)
display(ordenes)

# COMMAND ----------

# Usar un esquema dedicado a la capacitación. Cada ejecución reemplaza
# la tabla de este ejercicio para que repetir el mismo archivo no duplique filas.
ordenes.write.format("delta").mode("overwrite").saveAsTable(tabla_destino)

# COMMAND ----------

resultado = spark.table(tabla_destino)
print(f"Tabla: {tabla_destino}")
print(f"Filas guardadas: {resultado.count()}")
display(resultado.filter("orden_id = 'OT-1007'"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Revisemos el resultado
# MAGIC
# MAGIC - ¿La tabla tiene las mismas 10 órdenes que el CSV?
# MAGIC - ¿La orden OT-1007 conserva el equipo BOM-04 y el estado Pendiente?
# MAGIC - En el historial del job, ¿qué ejecución corresponde a la carga que acabamos de revisar?
