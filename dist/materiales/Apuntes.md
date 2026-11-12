# Clase 1 · Del archivo a la tabla

Una carga toma datos de un **origen**, como un archivo CSV, y los guarda en una **tabla de destino**. El **proceso de carga** define cómo se hace. Cada **ejecución** permite revisar cuándo se realizó y cómo terminó. Después se comprueba que la tabla contenga los datos que se esperaban.

## Ejercicio

Usaremos un ejemplo ficticio de órdenes de trabajo. Cada persona hará el recorrido completo cuando le toque compartir pantalla.

1. Abre `ordenes_trabajo.csv`. Identifica qué representa una fila y qué columna identifica una orden.
2. Abre la ejecución indicada en clase. Anota su fecha y estado.
3. Abre la tabla de destino y anota su nombre.
4. En el primer turno, compara la cantidad de filas de la tabla de práctica con la cantidad de órdenes del CSV. En el segundo, busca OT-1007 y compara su equipo y estado con el CSV.
5. Explica qué evidencia viste y qué revisarías si hubiera una diferencia.

| Qué anotar | Tu respuesta |
|---|---|
| Archivo y columna que identifica cada orden | |
| Fecha y estado de la ejecución | |
| Nombre de la tabla de destino | |
| Dato comprobado y resultado | |

## Caso simulado para practicar sin acceso

En este escenario, el proceso **Carga de órdenes de trabajo** leyó `ordenes_trabajo.csv` el 10 de noviembre de 2026 a las 09:00. El estado de la ejecución fue **Correcta** y la tabla de destino se llama `ordenes_trabajo_demo`.

El resumen simulado de la tabla muestra **9 filas**. La orden **OT-1007** figura con equipo **BOM-04** y estado **Pendiente**. Compara estos datos con el CSV. ¿Qué comprobación coincide? ¿Cuál requiere investigar? ¿Qué información pedirías para identificar la orden faltante?
