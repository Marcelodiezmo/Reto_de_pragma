# Insertar datos en bd mysql
## Reto:
Crear un pipeline de datos, que permita obtener información y/o estadísticas  de los datos que se va cargando en micro Batches, 
es decir,A  medida que los datos son cargados y alojados a la base de datos, realiza un seguimiento de  : recuento,promedio minimo y maximo

## Comprobación de resultados:
• Cargar los archivos excepto"validation.csv"

• Imprime el valor actual de las estadísticas en ejecución.

• Realiza una consulta en la base de datos del: recuento total de
filas, valor promedio, valor mínimo y valor máximo para el campo
“price”.

• Ejecuta el archivo “validation.csv” a través de todo el pipeline y muestra el valor de las estadísticas en ejecución.

• Realice una nueva consulta en la base de datos después de cargar
“validation.csv”, para observar cómo cambiaron los valores del:
recuento total de filas, valor promedio, valor mínimo y valor máximo
para el campo “price”.


# Pasos: Insertar datos en bd mysql

1. Instalar mysql ingresando en este link https://dev.mysql.com/downloads/file/?id=516927
2. Tener workbench como ambiente de entorno para hacer validaciones
3. Tener presente la informacion para poder conectarse al servidor ( username, password, port), esto se puede verificar en workbench
4. Crear un ambiente : python3 -m venv tutorial-env 
5. Activar ambiente : python activate 
6. En python instalar paquetes agregados en el requeriments.txt  ( pip install -r requirements.txt  )
7. ejecutar pytest para pruebas : en la terminal colocar: pytest e indicara si las funciones de testeo son satisfactorios

## Instalacion
```bash
pip install -r requirements.txt
```

## Usar Pytest
Pytest es un marco de pruebas, se puede usar para realizar pruebas unitarias.

```bash
cd RETO_PRAGMA/Scripts/Pytest
```
## Salida
```bash
test_file.py::test_query PASSED                                                         test_file.py::test_conexion PASSED  
```     


## Ejemplo 
* En el  Script llamado "Insertdb.py", en el main,  estan las variables necesarias para la ejecucion
```bash
    # Nombre que queda en la tabla de base de  datos
    name_table = 'transaccion'
    # Cantidad de registros a insertar por iteracion
    microbatch = 10
    # nombres de los csv a leer
    path_name_transaccion_csv = '/2012-*.csv'
    path_name_validacion_csv = '/validation*.csv'
```
### Nota:
    los archivos csv deben tener la misma estructura de datos

* En el Script llamado "Sql_queries.py" esta el query que llamaremos y utilizaremos para realizar la consulta en la base de datos
```bash
QUERY='''select
count(price) as conteo,
max(price) as maximo,
min(price) as minimo,
avg(price) as promedio
from  pragma.transaccion
'''
```

# Salida
Se espera las siguientes salidas : 

Antes de insertar la informacion  de validation_csv
### result_query 
```bash
conteo	maximo	minimo	promedio
1502	  100	 0	     55.5699
```

Despues de insertar la informacion  de validation_csv
###  result_query_validation
```bash
conteo	maximo	minimo	promedio
 1510	100	      0	      55.496
```
Dataframe con todas las estadisticas de cada insercion

### result_df_control
```bash

contador iteracion	maximo minimo media	 tipo
10	          1	     97	    14.0  63.00	..2012-1
20	          2	     97	    0.0	  55.25 ..2012-2
22	          3	     97  	0.0   54.22	..2012-3

```

### Ejecucion
```bash
.\reto_pragma\Scripts\Insertdb.py
```
















# reto_Pragma
