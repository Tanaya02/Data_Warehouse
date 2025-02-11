import mysql.connector

class WarehouseConfig:
    def __init__(self):
        self.config = {
            'host': 'localhost',
            'user': 'root',
            'password': '###',
            'database': 'data_warehouse_sakila'
        }
        self.connection = self.create_connection()

    def create_connection(self):
        """Establish connection to MySQL using mysql.connector"""
        return mysql.connector.connect(**self.config)