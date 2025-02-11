import mysql.connector
from db_config import db_config

class DBConnection:
    def __init__(self):
        self.db_config = db_config
        self.conn = None
        self.cursor = None

    def connect_to_db(self):
        """Establish a connection to the MySQL database."""
        try:
            self.conn = mysql.connector.connect(**self.db_config)
            self.cursor = self.conn.cursor()
            print("Connected to the database.")
        except mysql.connector.Error as e:
            print(f"Error connecting to database: {e}")

    def close_db_connection(self):
        """Close the connection to the database."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        print("Database connection closed.")