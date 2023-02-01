import Insertdb 
from sql_queries import QUERY
import pandas as pd
from sqlalchemy import text



def test_query():
    
    """Permite hacer consultas a la base de datos"""
    result_query=Insertdb.execute_query(QUERY)
    assert type(result_query)==str,"Borrar tabla para la insercion de datos"


    
def test_conexion():
    sqlEngine, dbConnection=Insertdb.conn()
    assert sqlEngine!=None and dbConnection!=None,"Conexion fallida"




  