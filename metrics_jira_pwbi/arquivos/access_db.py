import pyodbc

class AccessDB:
    def __init__(self):
        self.db_path = r"C:\Users\C13427Q\OneDrive - EXPERIAN SERVICES CORP\Documents\metricas\db\metrics_serasa_uat.accdb"
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
            return None
        cursor = self.connection.cursor()
        try:
            cursor.execute(query)
            self.connection.commit()
        except pyodbc.Error as e:
            print(f"Erro ao executar a consulta: {e}")
        finally:
            cursor.close()

    def fetch_data(self, query):
        if not self.connection:
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

    def fetch_data_one(self, query):
        if not self.connection:
            return None
        cursor = self.connection.cursor()
        try:
            cursor.execute(query)
            rows = cursor.fetchone()
            return rows
        except pyodbc.Error as e:
            print(f"Erro ao buscar dados: {e}")
            return None
        finally:
            cursor.close()

    def grava_dados(self, query):
        if not self.connection:
            print('Erro na gravação')
            return None
        cursor = self.connection.cursor()
        try:
            cursor.execute(query)
            self.connection.commit()
        except pyodbc.Error as e:
            print(f"Erro ao gravar os dados: {e}")
            return None