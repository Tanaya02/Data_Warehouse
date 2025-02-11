from data_generator import DataGenerator

# Example usage
data_generator = DataGenerator(config_file='config/tables_config.json')  # Use the config file to get table names

# Loop through the table names in the config and generate data for each
for table_name in data_generator.tables:
    data_generator.generate_data_for_table(table_name, 11)
