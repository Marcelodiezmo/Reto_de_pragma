QUERY='''select
count(price) as conteo,
max(price) as maximo,
min(price) as minimo,
avg(price) as promedio
from  pragma.transaccion
'''