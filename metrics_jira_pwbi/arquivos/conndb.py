import pyodbc

class conndb:
    def __init__(self):
        self.db_path = f"C:\Users\C13427Q\OneDrive - EXPERIAN SERVICES CORP\Documents\metricas\db\metrics_serasa_uat.accdb"
        self.connection = self.connect()

    def connect(self):
        try:
            connection = pyodbc.connect(
                r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' + self.db_path
            )
            print("Conexão estabelecida com sucesso!")
            return connection
        except pyodbc.Error as e:
            print(f"Erro ao conectar ao banco de dados: {e}")
            return None

    def close(self):
        if self.connection:
            self.connection.close()
            print("Conexão fechada.")

    def execute_query(self, query):
        if not self.connection:
            print("Conexão não estabelecida.")
            return None
        cursor = self.connection.cursor()
        try:
            cursor.execute(query)
            self.connection.commit()
            print("Consulta executada com sucesso!")
        except pyodbc.Error as e:
            print(f"Erro ao executar a consulta: {e}")
        finally:
            cursor.close()

    def fetch_data(self, query):
        if not self.connection:
            print("Conexão não estabelecida.")
            return None
        cursor = self.connection.cursor()
        try:
            cursor.execute(query)
            rows = cursor.fetchall()
            return rows
        except pyodbc.Error as e:
            print(f"Erro ao buscar dados: {e}")
            return None
        finally:
            cursor.close()


