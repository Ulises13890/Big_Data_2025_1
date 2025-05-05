import os
import datetime
import sqlite3
import pandas as pd


class DataBase:
    def __init__(self):
        self.db_name = "src/NFTPolGames/static/db/matic_pol_historical.db"

        self.create_database()
    
    def create_database(self):
        try:
            self.conn = sqlite3.connect(self.db_name)
            self.cursor = self.conn.cursor()

        except Exception as err:
            print("error al crear la base de datos")
            # df.sql(self.conn,nom_table)

    def close_database(self):
        self.conn.close()
