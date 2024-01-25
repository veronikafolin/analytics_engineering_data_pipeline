from snowflake import connector


def load_data_to_snowflake(data, context):

    file_name = data['name']

    # Snowflake connection parameters
    snowflake_account = 'MACIBRH-XA80554'
    snowflake_user = 'veronikafolin4'
    snowflake_password = 'zyvpoz-Rigsam-0cojgu'
    snowflake_warehouse = 'transforming'
    snowflake_database = 'raw'
    snowflake_schema = 'stages'
    snowflake_stage = 'my_gcs_stage'

    # Snowflake connection
    connection = connector.connect(
        user=snowflake_user,
        password=snowflake_password,
        account=snowflake_account,
        warehouse=snowflake_warehouse,
        database=snowflake_database,
        schema=snowflake_schema
    )

    # Execute Snowflake COPY command to load data
    cursor = connection.cursor()

    if "orders" in file_name:
        command = f"COPY INTO orders FROM @{snowflake_stage}/{file_name}"
        cursor.execute(command)
    elif "lineitem" in file_name:
        command = f"COPY INTO lineitem FROM @{snowflake_stage}/{file_name}"
        cursor.execute(command)
    else:
        print("File name not recognised.")    
    
    cursor.close()
    connection.close()