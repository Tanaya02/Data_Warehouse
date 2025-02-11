import mysql.connector
from mysql.connector import Error
from typing import List, Dict

class TableMerger:
    def __init__(self, db_config: Dict[str, str]):
        """Initialize TableMerger with database configuration."""
        self.db_config = db_config
        self.conn = None
        self.cursor = None
        self.key_counter = None  # Will be initialized dynamically

    def connect(self):
        """Establish database connection."""
        try:
            self.conn = mysql.connector.connect(**self.db_config)
            self.cursor = self.conn.cursor()
        except Error as e:
            raise Exception(f"Error connecting to MySQL: {str(e)}")

    def close(self):
        """Close database connection."""
        if self.cursor:
            self.cursor.close()
        if self.conn and self.conn.is_connected():
            self.conn.close()

    def get_last_surrogate_key(self, target_table: str) -> int:
        """
        Retrieve the last Surrogate_Key from the target table.
        
        Args:
            target_table (str): The name of the merged table.
        
        Returns:
            int: Last surrogate key, or 300 if the table is empty.
        """
        self.cursor.execute(f"SELECT MAX(Surrogate_Key) FROM {target_table}")
        last_key = self.cursor.fetchone()[0]
        return last_key + 1 if last_key else 301  # Default start at 301 if table is empty

    def get_columns(self, table: str) -> List[str]:
        """
        Fetch column names from the given table.
        
        Args:
            table (str): Table name to retrieve column names from.
        
        Returns:
            list: A list of column names.
        """
        self.cursor.execute(f"DESCRIBE {table}")
        columns = self.cursor.fetchall()
        return [col[0] for col in columns]

    def record_exists(self, target_table: str, row: tuple, columns: List[str]) -> bool:
        """
        Check if a record already exists in the target table.
        
        Args:
            target_table (str): The target table name.
            row (tuple): The row data to check for duplication.
            columns (list): List of column names.
        
        Returns:
            bool: True if the record exists, False otherwise.
        """
        where_clause = " AND ".join([f"{col} = %s" for col in columns[1:]])  # Skip Surrogate_Key
        select_sql = f"SELECT COUNT(*) FROM {target_table} WHERE {where_clause}"
        self.cursor.execute(select_sql, row[1:])  # Skip Surrogate_Key in the check
        return self.cursor.fetchone()[0] > 0

    def merge_tables(self, source_tables: List[str], target_table: str) -> None:
        """
        Merge multiple tables while ensuring unique data and incrementing surrogate keys.
        
        Args:
            source_tables (list): List of source table names.
            target_table (str): Name of the target merged table.
        """
        try:
            self.connect()
            self.key_counter = self.get_last_surrogate_key(target_table)  # Get last surrogate key

            # Fetch columns from the first source table
            columns = self.get_columns(source_tables[0])

            # Ensure target table exists
            columns_def = [f"{col} VARCHAR(255)" for col in columns]
            create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS {target_table} (
                    Surrogate_Key INT PRIMARY KEY,
                    {', '.join(columns_def)}
                )
            """
            self.cursor.execute(create_table_sql)

            for table in source_tables:
                print(f"Processing table: {table}")

                # Fetch data from source table
                self.cursor.execute(f"SELECT {', '.join(columns)} FROM {table}")
                source_data = self.cursor.fetchall()

                for row in source_data:
                    if self.record_exists(target_table, row, columns):
                        print(f"Skipping duplicate record: {row}")
                        continue

                    # Use incremented surrogate key
                    next_key = self.key_counter
                    self.key_counter += 1  # Increment for next record

                    # Insert the record
                    insert_sql = f"""
                        INSERT INTO {target_table} 
                        (Surrogate_Key, {', '.join(columns)}) 
                        VALUES ({next_key}, {', '.join(['%s'] * len(columns))})
                    """
                    self.cursor.execute(insert_sql, row)

            self.conn.commit()
            print(f"Successfully merged {len(source_tables)} tables into {target_table}!")
        
        except Error as e:
            if self.conn:
                self.conn.rollback()
            raise Exception(f"Error during merge operation: {str(e)}")
        
        finally:
            self.close()

# Example usage
if __name__ == "__main__":
    db_config = {
        "host": "localhost",
        "user": "root",
        "password": "###",
        "database": "###"
    }

    try:
        merger = TableMerger(db_config)
        merger.merge_tables(
            source_tables=["Theater01", "Theater02"],
            target_table="Merged_Theater"
        )
    except Exception as e:
        print(f"Error: {str(e)}")
