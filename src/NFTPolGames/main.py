from dataweb import DataWeb
from database import DataBase
import pandas as pd
import datetime

 
def main():
    df = pd.DataFrame()
    dataweb = DataWeb()
    database = DataBase()
    df = dataweb.obtener_datos()
    df = dataweb.convertir_numericos(df)
    print("*************** imprecion de los datos obtenidos ************************")
    print(df.shape)
    print(df.head())
    df.to_csv("src/NFTPolGames/static/csv/data_web.csv", index=False) #/workspaces/bigdata_2025_1_2/src/edu_bigdata/static/csv
    nombre_tabla = "matic_analisis"
    database.insert_data(df,nombre_tabla)
    print("*************** Insertar los datos obtenidos en la base datos tabla: {}*********".format(nombre_tabla))
    print(df.shape)
    print(df.head())
    df_2 = database.read_data(nombre_tabla)
    print(df_2.shape)
    print(df_2.head())
     # Preparando los datos para la actualización
    fecha_actual = datetime.date.today().strftime('%Y-%m-%d')
    nuevos_datos = {
        'fecha': fecha_actual,
        'abrir': 1.23,
        'max': 1.25,
        'min': 1.20,
        'cerrar': 1.24,
        'cierre_ajustado': 1.24,
        'volumen': 1000000
    }
    condicion_actualizacion = "fecha = '2025-05-15'"  # Ejemplo de condición, ajusta según necesites

    # Realizando la operación de actualización
    database.update_data(nom_table=nombre_tabla, data=nuevos_datos, condition=condicion_actualizacion)
    print(f"*************** Actualización de datos en la tabla: {nombre_tabla} *********")

    # Leyendo los datos actualizados para verificar
    df_actualizado = database.read_data(nombre_tabla)
    print("*************** Datos actualizados de la tabla: {} *********".format(nombre_tabla))
    print(df_actualizado.tail())

    df_ordenado = database.read_data(nombre_tabla).sort_values(by='fecha', ascending=True)
    if not df_ordenado.empty:
        fecha_primer_dato = df_ordenado.iloc[0]['fecha']
        condicion_eliminar = f"fecha = '{fecha_primer_dato}'"
        database.delete_data(nom_table=nombre_tabla, condition=condicion_eliminar)
        print(f"*************** Eliminando el primer dato (basado en fecha: {fecha_primer_dato}) de la tabla: {nombre_tabla} *********")
    else:
         print(f"*************** La tabla: {nombre_tabla} está vacía, no hay datos para eliminar. *********")


if __name__ == "__main__":
    main()
