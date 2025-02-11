from .operations_handler import Operations
import json

class ShiftToWarehouse():
    def shift_to_warehouse(self, cur, data_warehouse_conn, table_name, staging_db, warehouse_db):
        # Fetch the table schema (CREATE TABLE statement)
        cur.execute(f"SHOW CREATE TABLE {staging_db}.{table_name}")
        create_table_statement = cur.fetchone()[1]  # The schema is in the second column

        # Replace database name and reconstruct CREATE TABLE statement
        create_table_statement = create_table_statement.replace(f'CREATE TABLE `{table_name}`', f'CREATE TABLE `{warehouse_db}`.`{table_name}`')

        # Find the position of the closing parenthesis and modify the statement
        closing_parenthesis_index = create_table_statement.rfind(')')

        if closing_parenthesis_index != -1:
            create_table_statement = (
            create_table_statement[:closing_parenthesis_index] + """,
            `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            `updated_at` TIMESTAMP NULL DEFAULT NULL,
            `deleted_at` TIMESTAMP NULL DEFAULT NULL
             )""" + create_table_statement[closing_parenthesis_index + 1:]
            )

        # print(create_table_statement)

        # Create a cursor for the target database
        target_cur = data_warehouse_conn.cursor()

        # Create the table in 'staging_level'
        target_cur.execute(create_table_statement)
        
        # Now, copy the data from the source table to the target table
        cur.execute(f"SELECT * FROM {staging_db}.{table_name}")
        rows = cur.fetchall()

        # Insert data into the target table
        for row in rows:
            placeholders = ', '.join(['%s'] * len(row))  # Create placeholder for each column
            insert_query = f"INSERT INTO {warehouse_db}.{table_name} VALUES ({placeholders}, CURRENT_TIMESTAMP, NULL, NULL)"
            target_cur.execute(insert_query, row)


        # Commit the changes and close the cursors
        data_warehouse_conn.commit()
        data_warehouse_conn.close()

        target_cur.close()

        return f'Table and data successfully copied to the {warehouse_db} database.'

    #For CDC
    def shift_to_warehouse(self, cur, data_warehouse_conn, table_name, staging_db, warehouse_db, date, update_column):

        # Step 1: Dynamically get primary key column name
        cur.execute(f"SHOW KEYS FROM {staging_db}.{table_name} WHERE Key_name = 'PRIMARY'")
        primary_key_row = cur.fetchone()
        primary_key_column = primary_key_row[4] if primary_key_row else None  # The column name is in index 4
        
        if not primary_key_column:
            raise ValueError(f"No primary key found for table {table_name}")
        
        # Now, copy the data from the source table to the target table
        query = f"SELECT * FROM {staging_db}.{table_name} WHERE {update_column} > %s"
        cur.execute(query, (date,)) 


        rows = cur.fetchall()
        # print(rows)

        target_cur = data_warehouse_conn.cursor()
        target_cur.execute("SELECT DATABASE()")   
        db_name = target_cur.fetchone()[0]
        print("Connected to Database:", db_name)
        
        # Step 2: Iterate through rows and check for conflicts
        for row in rows:
            # Fetch existing entry based on primary key
            check_query = f"SELECT * FROM {warehouse_db}.{table_name} WHERE {primary_key_column} = %s"
            # print(check_query, row[0])
            target_cur.execute(check_query, (row[0],))  # Since primary key is first column in row
            existing_entry = target_cur.fetchone()
        
            if existing_entry:
                print(f"Entry with primary key {row[0]} already exists. Updating history table.")
                obj_op = Operations()
                history_table = f"{table_name}_history_table"
                
                # Convert row to dictionary and then JSON
                column_names = [desc[0] for desc in cur.description]  # Fetch column names dynamically
                row_dict = dict(zip(column_names, row))
                json_data = json.dumps(row_dict, default=str)  # Convert to JSON
                json_data = json.loads(json_data)
                print(json_data)

        
                result = obj_op.upsert(json_data, target_cur, table_name, history_table, data_warehouse_conn)
                return result


            else:
                placeholders = ', '.join(['%s'] * len(row))  
                insert_query = f"INSERT INTO {warehouse_db}.{table_name} VALUES ({placeholders}, CURRENT_TIMESTAMP, NULL, NULL)"
                target_cur.execute(insert_query, row)
                # print(f"Inserted new entry: {row}")
         


        # Commit the changes and close the cursors
        data_warehouse_conn.commit()
        data_warehouse_conn.close()

        target_cur.close()


        return f'Table and data successfully copied to the {warehouse_db} database.'


