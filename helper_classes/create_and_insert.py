

class create_and_insert_class():

    def create_and_insert(self, curr_cur, staging_conn, table_name, base_db, staging_db):
        # Fetch the table schema (CREATE TABLE statement)
        curr_cur.execute(f"SHOW CREATE TABLE {table_name}")
        create_table_statement = curr_cur.fetchone()[1]  # The schema is in the second column

        # Change the CREATE TABLE statement to create the table in 'staging_level' database
        create_table_statement = create_table_statement.replace(f'`{base_db}`', f'`{staging_db}`')

        # Create a cursor for the target database
        target_cur = staging_conn.cursor()

        target_cur.execute(f"USE {staging_db};")
        # # Create the table in 'staging_level'
        target_cur.execute(create_table_statement)

        curr_cur.execute(f"SELECT * FROM {base_db}.{table_name}")

        rows = curr_cur.fetchall()

        # Insert data into the target table
        for row in rows:
            placeholders = ', '.join(['%s'] * len(row))  # Create placeholder for each column
            insert_query = f"INSERT INTO {staging_db}.{table_name} VALUES ({placeholders})"
            # print(f"Values: {row}")  # Print actual row data
            target_cur.execute(insert_query, row)
            # print(insert_query)
    

        # Commit the changes and close the cursors
        staging_conn.commit()
        staging_conn.close()
        target_cur.close()

        print(f"Table {table_name} successfully copied.")

    #insert into staging AFTER A PARTICULAR DATE without generic column
    def create_and_insert(self, curr_cur, staging_conn, table_name, base_db, staging_db, date):

        # Create a cursor for the target database
        target_cur = staging_conn.cursor()

        target_cur.execute(f"USE {staging_db};")
        # # Create the table in 'staging_level'
        # target_cur.execute(create_table_statement)


        query = f"SELECT * FROM {base_db}.{table_name} WHERE last_update > %s"
        curr_cur.execute(query, (date,))

        rows = curr_cur.fetchall()

        # Insert data into the target table
        for row in rows:
            placeholders = ', '.join(['%s'] * len(row))  # Create placeholder for each column
            insert_query = f"INSERT INTO {staging_db}.{table_name} VALUES ({placeholders})"
            print(f"Values: {row}")  # Print actual row data
            target_cur.execute(insert_query, row)
            # print(insert_query)


        # Commit the changes and close the cursors
        staging_conn.commit()
        staging_conn.close()
        target_cur.close()

        print(f"Table {table_name} successfully copied.")

    # Using generic last_update column name
    def create_and_insert(self, curr_cur, staging_conn, table_name, base_db, staging_db, date, update_column):

        # Create a cursor for the target database
        target_cur = staging_conn.cursor()

        target_cur.execute(f"USE {staging_db};")
        # # Create the table in 'staging_level'
        # target_cur.execute(create_table_statement)


        query = f"SELECT * FROM {base_db}.{table_name} WHERE {update_column} > %s"
        curr_cur.execute(query, (date,))

        rows = curr_cur.fetchall()

        # Insert data into the target table
        for row in rows:
            placeholders = ', '.join(['%s'] * len(row))  # Create placeholder for each column
            insert_query = f"INSERT INTO {staging_db}.{table_name} VALUES ({placeholders})"
            print(f"Values: {row}")  # Print actual row data
            target_cur.execute(insert_query, row)
            # print(insert_query)


        # Commit the changes and close the cursors
        staging_conn.commit()
        staging_conn.close()
        target_cur.close()

        print(f"Table {table_name} successfully copied.")