from Config_files.base_db_config import BaseDbConfig
from Config_files.staging_level_config import StagingLevelConfig
from helper_classes.create_and_insert import create_and_insert_class
import mysql.connector
from datetime import datetime
import pandas as pd
import jsonify
import time
import json


class BaseToStaging():

    def __init__(self):
        self.base_config = BaseDbConfig() 
        self.base_conn = self.base_config.connection
        self.cur = self.base_conn.cursor()

        self.staging_config = StagingLevelConfig()
        self.staging_conn = self.staging_config.connection

        
    #for generic update_column extracted from json file
    def copy_table(self, date):
        # cur = base_conn.cursor()
        # cur = mysql.connection.cursor()
        # staging_config = StagingLevelConfig()
        # staging_conn = staging_config.mysql.connect
        base_db = "sakila"
        staging_db = "staging_level_sakila_1"
        obj = create_and_insert_class()

        with open("input_files/demo.json", "r") as file:
            data = json.load(file)
            tables = data.get("tables", []) 

            
        for table in tables:
            table_name = table[0]
            update_column = table[1]
            # table_name = "country"
            print(f"Copying table: {table_name}")
    
            for attempt in range(1):   
                try:
                    # staging_conn = staging_config.mysql.connect
                    # staging_level_conn = staging_config.mysql
                    obj.create_and_insert(self.cur, self.staging_conn, table_name, base_db, staging_db, date, update_column)
                    # break  # Exiting loop after first successful insert
    
                except mysql.connector.errors.OperationalError as e:
                    print(f"Retry failed for table {table_name} with error: {e}")
                    time.sleep(5)  # Wait before retrying
                except Exception as e:
                    print(f"Error while copying table {table_name}: {e}")
                    break

        self.cur.close()
        self.staging_conn.close()

        return 'Table has been successfully copied to the staging_level database.'


    # To Copy ALL TABLES
    def copy_all_tables(self):
        base_db = "sakila"
        staging_db = "staging_level_sakila"
        obj = create_and_insert_class()

        with open("input_files/sakila_data.json", "r") as file:
            data = json.load(file)
            tables = data.get("tables", []) 

            
        for table in tables:
            table_name = table
            print(f"Copying table: {table}")

            for attempt in range(1):   
                try:
                    # staging_conn = staging_config.mysql
                    obj.create_and_insert(self.cur, self.staging_conn, table_name, base_db, staging_db)
                    # break  # Exiting loop after first successful insert

                except mysql.connector.errors.OperationalError as e:
                    print(f"Retry failed for table {table_name} with error: {e}")
                    time.sleep(5)  # Wait before retrying
                except Exception as e:
                    print(f"Error while copying table {table_name}: {e}")
                    break

        self.cur.close()
        self.staging_conn.close()

        return 'All tables and data successfully copied to the staging_level database.'


    #to copy after a particular date
    def copy_all_tables(self, date):
        
        base_db = "sakila"
        staging_db = "staging_level_sakila"
        obj = create_and_insert_class()

        
        try:
            with open("input_files/demo.json", "r") as file:
                data = json.load(file)
                tables = data.get("tables", []) 

            for table in tables:
                table_name = table[0]
                update_column = table[1]
                # table_name = table

                print(f"Copying table: {table_name}")

                for attempt in range(1):
                    try:
                        # staging_conn = staging_config.mysql
                        obj.create_and_insert(self.cur, self.staging_conn, table_name, base_db, staging_db, date, update_column)
                        break
    
                    except mysql.connector.errors.OperationalError as e:
                        print(f"Retry failed for table {table_name} with error: {e}")
                        time.sleep(5)  
                    except Exception as e:
                        print(f"Error while copying table {table_name}: {e}")
                        break
    
            self.cur.close()
            self.staging_conn.close()

            return 'All tables and data successfully copied to the staging_level database.'

        except mysql.connector.errors.OperationalError as e:
            # print(f"MySQL Error: {e}")
            # return "MySQL operational error occurred."
            print(f"Operational Error: {e}")

        except mysql.connector.Error as e:
            print(f"MySQL Error: {e}")

        except Exception as e:
            print(f"Error: {e}")
            return "Error occurred while copying tables and data."

    
    def copy_to_csv(self, table_name):

        query = f"SELECT * FROM {table_name}"
        self.cur.execute(query)
        
        rows = self.cur.fetchall()
        df = pd.DataFrame(rows)

        df.to_csv(f"{table_name}.csv", index=False)
        
        self.base_conn.close()        
        return json.dumps({"message": f"Data successfully exported to {table_name}.csv"})




def index():
    obj_1 = BaseToStaging()
    # return obj_1.copy_table('2025-02-06')
    # return obj_1.copy_all_tables()
    # return obj_1.copy_all_tables('2025-02-06')
    return obj_1.copy_to_csv("actor")


if __name__ == '__main__':
    index()