import os
import datetime
import sqlite3
import pandas as pd


class DataBase:
    def __init__(self):
        self.db_name = "src/NFTPolGames/static/db/matic_pol_historical.sqlite"
    
    # CRUD C = create(insert) R= read U = update DF= Delete
    # CRUD C = create(insert) R= read U = update DF= Delete
    def insert_data(self,df = pd.DataFrame(),nom_table="dolar_analisis"):
        try:
            df = df.copy()
            conn = sqlite3.connect(self.db_name)
            df.to_sql(name=nom_table,con=conn,if_exists='replace') # sobreescriba , inserte al final, actualizacion datos
            conn.close()
        except Exception as errores:
            print("error al guradar los datos")
    
    def read_data(self,nom_table=""):
        df=pd.DataFrame()
        try:
            if len(nom_table)>0:
                conn = sqlite3.connect(self.db_name)
                query= "select * from {}".format(nom_table)
                df = pd.read_sql_query(sql=query,con=conn)
                print("*************** consulta base datos tabla: {}*********".format(query))
                conn.close
                return df
        except Exception as errores:
            print("error al obtener los datos")
            return df
            
    def update_data(self, nom_table="", data={}, condition=""):
        try:
            if len(nom_table) > 0 and len(data) > 0 and len(condition) > 0:
                conn = sqlite3.connect(self.db_name)
                cursor = conn.cursor()
                set_values = ", ".join([f"{key} = ?" for key in data.keys()])
                query = f"UPDATE {nom_table} SET {set_values} WHERE {condition}"
                cursor.execute(query, tuple(data.values()))
                conn.commit()
                print(f"*************** Datos actualizados en la tabla: {nom_table} con la condición: {condition}*********")
                cursor.close()
                conn.close()
            else:
                print("Error: Nombre de tabla, datos a actualizar y condición son obligatorios para la actualización.")
        except Exception as errores:
            print("Error al actualizar los datos:", errores)
    
    def delete_data(self, nom_table="", condition=""):
        try:
            if len(nom_table) > 0 and len(condition) > 0:
                conn = sqlite3.connect(self.db_name)
                cursor = conn.cursor()
                query = f"DELETE FROM {nom_table} WHERE {condition}"
                cursor.execute(query)
                conn.commit()
                print(f"*************** Datos eliminados de la tabla: {nom_table} con la condición: {condition}*********")
                cursor.close()
                conn.close()
            else:
                print("Error: Nombre de tabla y condición son obligatorios para la eliminación.")
        except Exception as errores:
            print("Error al eliminar los datos:", errores)