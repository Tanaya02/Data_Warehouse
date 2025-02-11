import json
import random
from faker import Faker
from datetime import datetime
import mysql.connector

import mysql
from config.db_connection import DBConnection
fake = Faker()

class DataGenerator:
    def __init__(self, config_file='tables_config.json'):
        self.db_connection = DBConnection()
        self.tables = self.load_table_names(config_file)

    def load_table_names(self, config_file):
        """Load table names from a JSON configuration file."""
        try:
            with open(config_file, 'r') as file:
                config_data = json.load(file)
                return config_data.get("tables", [])
        except FileNotFoundError:
            print(f"Error: {config_file} not found.")
            return []
        except json.JSONDecodeError:
            print(f"Error: Failed to decode JSON from {config_file}.")
            return []

    def connect_to_db(self):
        """Establish a connection to the MySQL database."""
        self.db_connection.connect_to_db()

        # Check if the connection and cursor are valid
        if not self.db_connection.conn or not self.db_connection.cursor:
            print("Failed to connect to the database or cursor is not initialized.")
            raise Exception("Database connection failed.")

    def get_existing_values(self, table_name, column_name):
        """Fetch distinct values from the referenced table to ensure valid foreign keys."""
        self.db_connection.cursor.execute(f"SELECT DISTINCT {column_name} FROM {table_name}")
        results = self.db_connection.cursor.fetchall()
        return [row[0] for row in results] if results else []

    def generate_random_value(self, col_name, data_type, fk_values=None):
        """Generate realistic random values while ensuring foreign keys are valid."""
        if fk_values:  # If foreign key values exist, choose one
            return random.choice(fk_values)

        if "int" in data_type:
            return random.randint(1, 100)
        elif "float" in data_type or "decimal" in data_type:
            return round(random.uniform(1.0, 5.0), 2)
        elif "char" in data_type or "varchar" in data_type:
            if "name" in col_name.lower():
                return fake.name()
            elif "first_name" in col_name.lower():
                return fake.first_name()
            elif "last_name" in col_name.lower():
                return fake.last_name()
            elif "email" in col_name.lower():
                return fake.email()
            elif "phone" in col_name.lower():
                return fake.phone_number()
            elif "address" in col_name.lower():
                return fake.address()
            elif "country" in col_name.lower():
                return fake.country()
            elif "city" in col_name.lower():
                return fake.city()
            else:
                return fake.word()
        elif "text" in data_type:
            return fake.sentence()
        elif "bool" in data_type:
            return random.choice([True, False])
        elif "date" in data_type and "datetime" not in data_type and "timestamp" not in data_type:
            return fake.date_this_decade()
        elif "datetime" in data_type or "timestamp" in data_type:
            return datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        else:
            return None

    def get_foreign_key_constraints(self, table_name):
        """Fetch foreign key constraints for the table."""
        self.db_connection.cursor.execute(f"""
            SELECT COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE TABLE_NAME = '{table_name}' AND REFERENCED_TABLE_NAME IS NOT NULL
        """)
        return self.db_connection.cursor.fetchall()

    def get_column_info(self, table_name):
        """Fetch column names and their data types from the table."""
        self.db_connection.cursor.execute(f"DESCRIBE {table_name}")
        return self.db_connection.cursor.fetchall()

    def generate_data_for_table(self, table_name, num_records):
        """Generate and insert meaningful random data for any given table while handling foreign keys."""
        self.connect_to_db()

        # Get column names and data types
        columns_info = self.get_column_info(table_name)
        column_names = [col[0] for col in columns_info]
        column_types = {col[0]: col[1] for col in columns_info}

        # Get foreign key constraints
        foreign_keys = {}
        fk_constraints = self.get_foreign_key_constraints(table_name)

        for col_name, ref_table, ref_column in fk_constraints:
            foreign_keys[col_name] = self.get_existing_values(ref_table, ref_column)

        # Detect primary key column and find last inserted ID
        primary_key = None
        start_id = None
        for col in columns_info:
            if "PRI" in col:
                primary_key = col[0]
                break

        if primary_key:
            self.db_connection.cursor.execute(f"SELECT MAX({primary_key}) FROM {table_name}")
            result = self.db_connection.cursor.fetchone()
            start_id = (result[0] + 1) if result[0] is not None else 1  # Start from next ID

        # Generate and insert records
        for _ in range(num_records):
            values = []
            for col_name in column_names:
                col_type = column_types[col_name]

                if col_name == primary_key and start_id is not None:
                    values.append(start_id)
                    start_id += 1
                else:
                    values.append(self.generate_random_value(col_name, col_type, foreign_keys.get(col_name)))

            placeholders = ", ".join(["%s"] * len(column_names))
            query = f"INSERT INTO {table_name} ({', '.join(column_names)}) VALUES ({placeholders})"

            print(f"Executing query: {query}")
            print(f"Values: {tuple(values)}")

            try:
                self.db_connection.cursor.execute(query, tuple(values))
                self.db_connection.conn.commit()
                print("Record inserted successfully.")
            except mysql.connector.Error as e:
                print(f"Error inserting record with values {values}: {e}")
                continue

        self.db_connection.close_db_connection()
        print(f"Inserted {num_records} records into {table_name}.")

# Example usage
data_generator = DataGenerator(config_file='tables_config.json')  # Pass config file
for table_name in data_generator.tables:
    data_generator.generate_data_for_table(table_name, 11)
