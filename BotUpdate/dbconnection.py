import pandas as pd
import pyodbc
import os
import time
import json
CONFIG_PATH = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = '\\'.join([CONFIG_PATH, 'config.json'])

class connection():
    def __init__(self, user):
        # print(user)
        with open(CONFIG_PATH) as config_file:
            config = json.load(config_file)
            config = config[user]
            config_file.close()
        self.username = config['username']
        self.password = config['password']
        self.server = config['server']
        self.database = config['database']
        self.connection = ""

    def connect_to_server(self):
        """
        this is what my method does
        """
        server = self.server
        database = self.database
        username = self.username
        password = self.password
        while True:
            try:
                connection = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
                break
            except:
                print("Retrying DB connection")
                time.sleep(2)
        return connection
    
    def disconnect(self):
        self.connection.close()

    def get_key(self, query):
        """
        Gets key from newly created item
        """
        data = pd.read_sql(query, self.connection)
        return data

    def Read(self, query):
        """
        Executes a read SQL for any given query, returns empty/none if query is invalid
        """
        self.connection = self.connect_to_server()
        data = pd.read_sql(query, self.connection)

        return data

    def Execute(self, query):
        """
        Executes a query, optional to add values for inserts
        """
        primary_key = 0
        try:
            self.connection = self.connect_to_server()
            cursor = self.connection.cursor()
            data = cursor.execute(query)
            cursor.commit()
            #pulls the primary key from the value created
            try:
                query = """
                SELECT @@IDENTITY
                """
                primary_key = self.get_key(query)
            except Exception as e:
                print(e)
                print("Could not return identity, returning 0")

            cursor.close()
            self.connection.close()
            return primary_key
        except Exception as e:
            print(f"Encountered an exception while executing .. {e}")
            print("Returning 0 for the primary key...")
            return primary_key
