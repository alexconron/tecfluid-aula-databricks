# Databricks notebook source
# MAGIC %md
# MAGIC # Clase 3 · Una consulta de SAP Perú
# MAGIC Deja tu YAML junto a este notebook. Busca el nombre de tu tabla en `sources` y lee el SELECT antes de ejecutarlo.
# MAGIC
# MAGIC La práctica usa 12 líneas ficticias. Puedes cambiar el límite y repetir el ejercicio sin abrir una conexión a SAP. Si falta PyYAML, ejecuta `%pip install pyyaml` en una celda.

# COMMAND ----------

from contextlib import closing
from pathlib import Path
import re
import sqlite3

import yaml

persona = "nicolas"  # nicolas, sebastian o nelson
carpeta = Path(__file__).resolve().parent if "__file__" in globals() else Path.cwd()
ruta = carpeta / f"sap_{persona}.yml"
if not ruta.is_file():
    raise FileNotFoundError(f"Busca tu YAML y cambia carpeta por su ruta de Workspace. Ruta usada: {ruta}")

# COMMAND ----------

config = yaml.safe_load(ruta.read_text(encoding="utf-8"))
fuente, = config["sources"]
consulta = fuente.get("query", "")
if not isinstance(consulta, str) or not consulta.strip():
    raise ValueError("Falta query dentro de sources. Revisa el nombre del campo y la sangría.")
if fuente.get("activo", True) is not True:
    raise ValueError("Esta fuente debe estar activa. Revisa el valor de activo.")

bronze = config["bronze"]
nombre = fuente["nombre"]
if (bronze["catalog"], bronze["schema"], nombre) != ("capacitacion", "bronze", f"sap_peru_oc_{persona}"):
    raise ValueError("Revisa tu nombre de tabla y el catálogo y esquema de práctica.")
destino = f"{bronze['catalog']}.{bronze['schema']}.{nombre}"
limite_sql = re.search(r"\bLIMIT\s+([0-9]+)\s*;?\s*$", consulta, re.IGNORECASE)
if not limite_sql or not 1 <= int(limite_sql[1]) <= 12:
    raise ValueError("Termina la consulta de práctica con LIMIT y un número entre 1 y 12.")
limite = int(limite_sql[1])
print(f"Destino previsto: {destino}")
print(consulta)

# COMMAND ----------

with closing(sqlite3.connect(":memory:")) as muestra:
    muestra.execute("ATTACH DATABASE ':memory:' AS SAP_PERU_DEMO")
    muestra.execute("CREATE TABLE SAP_PERU_DEMO.LINEAS_OC (DocNum INTEGER, LineNum INTEGER, ItemCode TEXT)")
    filas = [(100 + i // 2, i % 2, f"ART-{i + 1:02}") for i in range(12)]
    muestra.executemany("INSERT INTO SAP_PERU_DEMO.LINEAS_OC VALUES (?, ?, ?)", filas)
    muestra.execute("PRAGMA query_only = ON")
    resultado = muestra.execute(consulta)
    columnas = [col[0] for col in resultado.description]
    muestra_local = resultado.fetchall()

print(" | ".join(columnas))
for fila in muestra_local:
    print(*fila, sep=" | ")
print(f"Líneas de la muestra: {len(muestra_local)}")
assert len(muestra_local) == limite, "Revisa el límite y las filas del ejercicio."

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tu turno
# MAGIC Sebastián, cambia el límite a 3. Nelson, cámbialo a 7. Guarda tu YAML, vuelve a ejecutar y muestra el documento y la línea del último registro. Con el orden original deberían ser 101/0 y 103/0, respectivamente.
# MAGIC
# MAGIC Prueben un error a la vez en su propia copia y corrijan antes de seguir.
# MAGIC
# MAGIC 1. Sebastián, agrega un espacio delante de `activo` sin mover `query`. ¿En qué línea avisa el lector de YAML? Vuelve a alinear los dos campos.
# MAGIC 2. Nelson, cambia `query` por `consulta`. El archivo abre, pero el notebook pide `query`. Recupera ese nombre y comprueba que vuelven a salir siete filas.
