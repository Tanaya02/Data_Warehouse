import jsonify 
import json 
import re
import json
import traceback
from datetime import datetime

class Operations:
    def __init__(self):
        pass

    def upsert(self, data, target_cur, table_name, history_table, warehouse_conn):
        try:
            if isinstance(data, str):
                print("Data is a string, converting to dictionary...")
                data = json.loads(data) 
            
            keys = list(data.keys())            

            id_column = keys[0]  # Get the first key
            id_value = data[id_column]  # Get the value corresponding to the first key


            # Fetch existing row from the table
            select_query = f"SELECT * FROM {table_name} WHERE {id_column} = %s"
            # print(select_query)
            target_cur.execute(select_query, (id_value,))
            existing_row = target_cur.fetchone()
            print("existing row", existing_row)
        
            if existing_row:
                # Fetch column names dynamically
                column_names = [desc[0] for desc in target_cur.description]
        
                # Convert existing row to a dictionary
                existing_data = dict(zip(column_names, existing_row))
        
                # Compare values column-wise to find the first mismatch
                updated_column = None
                for col in keys[1:]:  # Exclude the primary key
                    if existing_data.get(col) != data[col]:  # Check for mismatch
                        updated_column = col
                        break  # Stop at the first mismatch
        
                if updated_column:
                    updated_value = data[updated_column]
                    print(f"Mismatch found in column: {updated_column} (New: {updated_value}, Old: {existing_data[updated_column]})")
                else:
                    print("No changes detected.")
            else:
                print(f"No existing record found for {id_column} = {id_value}")
        



                     # Step 1: Check if the history table exists, create it if not
            target_cur.execute(f"SHOW TABLES LIKE '{history_table}'")
            history_table_exists = target_cur.fetchone()

            if not history_table_exists:
                self.create_history_table(target_cur, table_name, history_table, warehouse_conn)


            # Step 1: Retrieve the existing row before updation
            select_query = f"SELECT * FROM {table_name} WHERE {id_column} = %s"
            target_cur.execute(select_query, (id_value,))
            row = target_cur.fetchone()

            #### CONSTRUCT AND INSERT OLD ENTRY INTO HISTORY_TABLE ####
            if row:  
                # Step 2: Get column names dynamically
                # print("Table", table_name)
                target_cur.execute(f"DESCRIBE {table_name}")
                column_names = [col[0] for col in target_cur.fetchall()]
                # print("Column Names:", column_names)

                updated_at_index = column_names.index('updated_at') if 'updated_at' in column_names else None

                row = list(row) 
                row[updated_at_index] = datetime.now()  # Replace update(None) with the current timestamp

                # Step 3: Construct INSERT query for history table
                placeholders = ', '.join(['%s'] * len(row)) 
                columns = ', '.join(column_names) 

                insert_query = f"INSERT INTO {history_table} ({columns}) VALUES ({placeholders})"

                print(insert_query)

                target_cur.execute(insert_query, row)

            ### PERFORM UPDATE IN ORIGINAL TABLE ###
            query = f"""
            UPDATE {table_name}
            SET {updated_column} = %s,
            updated_at = CURRENT_TIMESTAMP
            WHERE {id_column} = %s
            """           
            target_cur.execute(query, (updated_value, id_value))

            warehouse_conn.commit()
            warehouse_conn.close()

            # return jsonify({"message": f"{table_name} updated successfully!"}), 200
            return json.dumps({"message": f"{table_name} updated successfully!"}), 200
        except Exception as e:
            print(f"Error: {e}")
            traceback.print_exc()  
            # return jsonify({"error": str(e)}), 500
            return json.dumps({"error": str(e)}), 500



    def update_table(self, data, target_cur, table_name, history_table, warehouse_conn):
        try:
            keys = list(data.keys())

            id_column = keys[0]  # Get the first key
            id_value = data[id_column]  # Get the value corresponding to the first key

            updated_column = keys[1]
            updated_value = data[updated_column]

                     # Step 1: Check if the history table exists, create it if not
            target_cur.execute(f"SHOW TABLES LIKE '{history_table}'")
            history_table_exists = target_cur.fetchone()

            if not history_table_exists:
                self.create_history_table(target_cur, table_name, history_table, warehouse_conn)


            # Step 1: Retrieve the existing row before updation
            select_query = f"SELECT * FROM {table_name} WHERE {id_column} = %s"
            target_cur.execute(select_query, (id_value,))
            row = target_cur.fetchone()

            #### CONSTRUCT AND INSERT OLD ENTRY INTO HISTORY_TABLE ####
            if row:  
                # Step 2: Get column names dynamically
                # print("Table", table_name)
                target_cur.execute(f"DESCRIBE {table_name}")
                column_names = [col[0] for col in target_cur.fetchall()]
                # print("Column Names:", column_names)

                updated_at_index = column_names.index('updated_at') if 'updated_at' in column_names else None

                row = list(row) 
                row[updated_at_index] = datetime.now()  # Replace update(None) with the current timestamp

                # Step 3: Construct INSERT query for history table
                placeholders = ', '.join(['%s'] * len(row)) 
                columns = ', '.join(column_names) 

                insert_query = f"INSERT INTO {history_table} ({columns}) VALUES ({placeholders})"

                print(insert_query)

                target_cur.execute(insert_query, row)

            ### PERFORM UPDATE IN ORIGINAL TABLE ###
            query = f"""
            UPDATE {table_name}
            SET {updated_column} = %s,
            updated_at = CURRENT_TIMESTAMP
            WHERE {id_column} = %s
            """           
            target_cur.execute(query, (updated_value, id_value))

            warehouse_conn.commit()
            warehouse_conn.close()

            return json.dumps({"message": f"{table_name} updated successfully!"}), 200
        except Exception as e:
            return json.dumps({"error": str(e)}), 500



    def delete_table(self, data, target_cur, table_name, history_table, warehouse_conn):
            id_column = next(iter(data))  # Get the first key
            id_value = data[id_column]  # Get the value corresponding to the first key

            target_cur.execute(f"SHOW TABLES LIKE '{history_table}'")
            history_table_exists = target_cur.fetchone()

            ####### CREATING THE HISTORY TABLE IF IT DOES NOT EXIST ######### 
            if not history_table_exists:
                self.create_history_table(target_cur, table_name, history_table, warehouse_conn)

            ####### PERFORMING THE OPERATION ########  
            # Step 1: Retrieve the existing row before deletion
            select_query = f"SELECT * FROM {table_name} WHERE {id_column} = %s"
            target_cur.execute(select_query, (id_value,))
            row = target_cur.fetchone()

            # print("Row Data:", row)
 
            #### CONSTRUCT AND INSERT OLD ENTRY INTO HISTORY_TABLE ####
            if row:  # Ensure the record exists
                # Step 2: Get column names dynamically
                target_cur.execute(f"DESCRIBE {table_name}")
                column_names = [col[0] for col in target_cur.fetchall()]

                deleted_at_index = column_names.index('deleted_at') if 'deleted_at' in column_names else None

                row = list(row)  # Convert to list to modify the element
                row[deleted_at_index] = datetime.now()  # Replace None with the current timestamp
                # print("Row Data:", row)

                # Step 3: Construct INSERT query for history table
                placeholders = ', '.join(['%s'] * len(row)) # Placeholder for values
                columns = ', '.join(column_names)  # Column names

                insert_query = f"INSERT INTO {history_table} ({columns}) VALUES ({placeholders})"
                print("Insert Query:", insert_query)  # Debugging
                
                target_cur.execute(insert_query, row)

            ### PERFORM DELETION OPERATION IN ORIGINAL TABLE ###
                # Step 4: Delete the record from the main table
                delete_query = f"DELETE FROM {table_name} WHERE {id_column} = %s"
                target_cur.execute(delete_query, (id_value,))

                # Commit the changes
                warehouse_conn.commit()
            
            warehouse_conn.close()
            return json.dumps({"message": f"{table_name} deleted and copied to history table successfully!"}), 200



    def create_history_table(self, target_cur, table_name, history_table, warehouse_conn):
        # Get column names and types dynamically from the original table
        target_cur.execute(f"SHOW CREATE TABLE {table_name}")
        create_table_query = target_cur.fetchone()[1]

        print(create_table_query)


        # Modify query to create a history table
        create_history_table_query = create_table_query.replace(
            f"CREATE TABLE `{table_name}`", 
            f"CREATE TABLE `{history_table}`"
        )
        # Remove any foreign key constraints and indexes referring to them
        create_history_table_query = "\n".join([
            line for line in create_history_table_query.split("\n") 
            if "FOREIGN KEY" not in line and "KEY" not in line
        ])
        # Remove `AUTO_INCREMENT` from the column definition
        create_history_table_query = re.sub(r"AUTO_INCREMENT\s*", "", create_history_table_query)

        # Remove `AUTO_INCREMENT=...` setting from table options (at the end of the query)
        create_history_table_query = re.sub(r"AUTO_INCREMENT=\d+\s*", "", create_history_table_query)

        # Remove any misplaced `=number` at the end of the query (e.g., `=161`)
        create_history_table_query = re.sub(r"=\s*\d+\s*", "", create_history_table_query)

     
        # Remove trailing comma before closing parenthesis
        create_history_table_query = re.sub(r",\s*\)", ")", create_history_table_query)

        print(create_history_table_query)

        target_cur.execute(create_history_table_query)
        warehouse_conn.commit()
 