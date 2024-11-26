from sqlite3 import Error
import sqlite3
import psycopg2
from abc import ABC, abstractmethod
import pandas
# pip install psycopg2

class DataClient(ABC):
    @abstractmethod
    def get_connection(self):
        pass

    @abstractmethod
    def create_mebel_table(self):
        pass

    @abstractmethod
    def get_items(self, price_from=0, price_to=10000):
        url = "https://www.kufar.by/l/mebel.csv"
        DataFrame=pandas.read_csv(url)
        print(DataFrame)

    @abstractmethod
    def insert(self, link, price, description):
        with open('https://www.kufar.by/l/mebel.csv', 'a') as f:
            DataFrame=pandas.to_csv(f, header=False)
            print(DataFrame)

    def run_test(self):
        self.get_connection()
        self.create_mebel_table()
        items = self.get_items(price_from=0, price_to=1000)
        for item in items:
            print(item)


class Sqlite3Client(DataClient):
    DB_NAME = "kufar.db"
    def get_connection(self):
        try:
            conn = sqlite3.connect(self.DB_NAME)
            return conn
        except Error:
            print(Error)

    def create_mebel_table(self, conn):
        cursor_object = conn.cursor()
        cursor_object.execute(
            """
                CREATE TABLE if not exists mebel
                (
                    id integer PRIMARY KEY autoincrement,
                    link text,
                    price integer, 
                    description text
                )
            """
        )
        conn.commit()

    def get_items(self, conn, price_from=0, price_to=10000):
        cursor = conn.cursor()
        cursor.execute(f'SELECT * FROM mebel WHERE price >= {price_from} and price <= {price_to}')
        return cursor.fetchall()

    def insert(self, conn, link, price, description):
        cursor = conn.cursor()
        cursor.execute(f"INSERT INTO mebel (link, price, description) VALUES ({link}', '{price}', '{description}')")
        conn.commit()

# data_client = PostgresClient()
# data_client = Sqlite3Client()
# data_client = DataClient()
DataClient.run_test()