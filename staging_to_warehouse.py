from Config_files.staging_level_config import StagingLevelConfig
from Config_files.warehouse_config import WarehouseConfig
from helper_classes.shift_to_warehouse import ShiftToWarehouse
from helper_classes.operations_handler import Operations
import jsonify
import mysql.connector
import time
from datetime import datetime
import re  
import json




############### DATA WAREHOUSE CREATION ####################

class StagingToWarehouse:

    def __init__(self):
        self.staging_config = StagingLevelConfig() 
        # self.app = staging_config.app   
        self.staging_conn = self.staging_config.connection
        self.cur = self.staging_conn.cursor()

        self.warehouse_config = WarehouseConfig()
        self.warehouse_conn = self.warehouse_config.connection


    # FOR 1 SPECIFIC TABLE
    # Function to copy table schema and data to another database
    def copy_all_tables(self):

        staging_db = "staging_level_sakila"
        warehouse_db = "data_warehouse_sakila"
        warehouse_obj = ShiftToWarehouse()
    
        with open("input_files/sakila_data.json", "r") as file:
            data = json.load(file)
            tables = data.get("tables", [])
    
        for table in tables:
            table_name = table
            print(f"Copying table: {table}")
    
            try:
                result = warehouse_obj.shift_to_warehouse(self.cur, self.warehouse_conn, table_name, staging_db, warehouse_db)
                print(result)
    
                return f'Table and data successfully copied to the {warehouse_db} database.'
            
            except mysql.connector.errors.OperationalError as e:
                print(f"Retry failed for table {table_name} with error: {e}")
                time.sleep(5)  # Wait before retrying
    
            except Exception as e:
                print(f"Error: {e}")
                return "Error occurred while copying table and data."
        
        self.cur.close()
        self.staging_conn.close()
        self.warehouse_conn.close()
    


    def copy_cdc(self,date):
        self.cur.execute("SELECT DATABASE()")  
        db_name = self.cur.fetchone()[0]
        print("Connected to Database:", db_name)
    

        staging_db = "staging_level_sakila"
        warehouse_db = "data_warehouse_sakila"
        warehouse_obj = ShiftToWarehouse()
    
        with open("input_files/demo.json", "r") as file:
            data = json.load(file)
            tables = data.get("tables", [])
    
        for table in tables:
            table_name = table[0]
            update_column = table[1]
            # table_name = table
            print(f"Copying table: {table_name}")
    
            try:

                warehouse_obj.shift_to_warehouse(self.cur, self.warehouse_conn, table_name, staging_db, warehouse_db, date, update_column)
    
                return f'Table and data successfully copied to the {warehouse_db} database.'
            
            except mysql.connector.errors.OperationalError as e:
                print(f"Retry failed for table {table_name} with error: {e}")
                time.sleep(5)  # Wait before retrying
    
            except Exception as e:
                print(f"Error: {e}")
                return "Error occurred while copying table and data."
        
        self.cur.close()
        self.staging_conn.close()
        self.warehouse_conn.close()
    
    
    

def index():
    obj = StagingToWarehouse()
    return obj.copy_all_tables()
    # return obj.copy_cdc('2025-02-10')


if __name__ == '__main__':
    index()