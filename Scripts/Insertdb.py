import glob
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text
from sql_queries import QUERY
from typing import Optional, Tuple





def conn():
    """CONEXION A LA BASE DE DATOS

    Returns:
        sqlEngine (sqlalchemy.engine.base.Engine) : Sirve para utilizarlo como conexion y asi, guardar informacion a la base de datos
        dbConnection (sqlalchemy.engine.base.Connection) :Sirve para utilizarlo como conexion y asi, leer informacion de la base datos
    """
    try:
        cadena_conexion = "mysql+pymysql://root:1234@localhost:3306/pragma"
        sqlEngine = create_engine(cadena_conexion)
        dbConnection = sqlEngine.connect()
        print(dbConnection)
    except BaseException:
        sqlEngine=None
        dbConnection=None
        print("No se conectò")
    return sqlEngine, dbConnection


def insert_data(name_table: str,
               microbatch: int,
               cvs_name: str,
               iteracion:int=0,
               df_control: Optional[pd.DataFrame] = False,
               contador: int = 0,
               suma: int = 0) -> Tuple[pd.DataFrame,
                                       int,
                                       int]:
                                   
    """INSERCION A LA BASE DE DATOS
                    PARAMETROS:

    Args:   name_table (int) :  Nombre de la tabla que se llamara en bd.
            microbatch (int) : Cantidad de registros a insertar por iteracion.
            cvs_name (str) :  /Nombre de archivos cvs
            df_control (Optional[pd.DataFrame] = False): dataframe que contiene cada informacion por iteracion (media,maximo y minimo)
            contador (int): conteo acumulado por cada insercion
            suma (int): suma acumulada por cada insercion
    Returns:
    pd.DataFrame: Dataframe con los resultados de las estadisticas
    Suma (int): Suma acumulada por cada insercion 
    contador: Conteo acumulado por cada insercion

    """

    print(contador, suma)

    if isinstance(df_control, bool):

        df_control = pd.DataFrame()

    mini = None
    maxi = None
    sqlEngine, _ = conn()
    path = "../files"
    csv_files = sorted(glob.glob(path + cvs_name))
    for i in range(len(csv_files)):
        for chunk in pd.read_csv(csv_files[i], chunksize=microbatch):

            chunk["price"] = chunk["price"].fillna(0)
            # insercion en base de datos
            chunk.to_sql(name=name_table, con=sqlEngine, if_exists="append")

            #maximo
            max_mini = chunk["price"].max()
            if maxi is None:
                maxi = max_mini
            if max_mini > maxi:
                maxi = max_mini

            #minimo
            min_mini = chunk["price"].min()
            if mini is None:
                mini = min_mini
            if min_mini < mini:
                mini = min_mini
            
            
            contador = chunk["price"].count() + contador
            suma = chunk["price"].sum() + suma
            #media
            promedio = suma / contador

            iteracion = iteracion + 1

            if "validation" in csv_files[0]:
                if maxi < df_control["maximo"].max():
                    maxi = df_control["maximo"].max()

                if mini > df_control["minimo"].min():
                    mini = df_control["minimo"].min()

           
            print(f"Filas cargadas : {contador}")
            print(f"El maximo es: {maxi}")
            print(f"El minimo es: {mini}")
            print(f"La media es de:{promedio}")
            df_control = fn_df_control(
                contador,
                maxi,
                mini,
                promedio,
                iteracion,
                csv_files[i],
                df_control)
    return df_control, suma, contador,iteracion



def fn_df_control(
        contador:int,
        maxi: int,
        minimo: int,
        media: float,
        iteracion: int,
        tipo: str,
        df_control: pd.DataFrame) -> pd.DataFrame:
    """Creando el dataframe donde estará todo el control
        de las estadisticas de cada  actualizacion

    Args:
        contador (int): conteo acumulado del campo "price" por cada insercion
        maxi (int): Maximo acumalado por cada iteracion
        minimo (int): Minimo acumalado por cada iteracion
        media (float): promedio acumulado por cada iteracion
        iteracion (int): cantidad acumulada de iteraciones
        tipo (str): en que csv proviene dicha estadistica
        df_control (pd.DataFrame): Dataframe que sirve para ver el control de todas las estadisticas


    Returns:
        pd.DataFrame: Dataframe que sirve para ver el control de todas las estadisticas historicamente
    """
 
    df = pd.DataFrame(data={"contador":[contador],
                            "iteracion": [iteracion],
                            "maximo": [maxi],
                            "minimo": [minimo],
                            "media": [media],
                            "tipo": [tipo]})

    df_control = pd.concat([df_control, df])
    df_control = df_control.reset_index(drop=True)
    return df_control


def execute_query(query: str) -> pd.DataFrame:
    """Ejecuta una query a la base de datos.

    Recibe un string que corresponde a una query a la base 
    de datos, ejecuta dicha query y retorna los resultados en un
    dataframe.

    Args:
        query (str): El texto de la query que se desea ejecutar.

    Returns:
        pd.DataFrame o string: Un dataframe con los resultados del query o un string con el error si falla.
    """

    _, dbConnection = conn()
    try:
        resultQuery = pd.read_sql(text(query), dbConnection)
        dbConnection.close()
    except Exception as e:
        resultQuery=str(e)
    return  resultQuery



if __name__ == "__main__":

    # Nombre que queda en la tabla de base de  datos
    name_table = "transaccion"
    # Cantidad de registros a insertar por iteracion
    microbatch = 4
    # nombres de los csv a leer
    path_name_transaccion_csv = "/2012-*.csv"
    path_name_validacion_csv = "/validation*.csv"

    # consulta a base de datos
    result_df_control, suma, contador,iteracion = insert_data(
        name_table, microbatch, path_name_transaccion_csv)


    result_query = execute_query(QUERY)
    result_df_control,_, _, _ = insert_data(name_table, microbatch, path_name_validacion_csv,iteracion, result_df_control, contador, suma)
    result_query_validation = execute_query(QUERY)
