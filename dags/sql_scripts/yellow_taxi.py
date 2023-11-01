from credentials import SF_ROLE, STORAGE_AWS_ROLE_ARN


warehouse = "AIRFLOW_TLC_WH"
database = "AIRFLOW_NYC_TLC"
schema = "Airflow_Yellow_Taxi"


passenger_count_table = "passenger_count_dim"
trip_distance_table = "trip_distance_dim"
pickup_location_table = "pickup_location_dim"
dropoff_location_table = "dropoff_location_dim"
datetime_dim_table = "datetime_dim"
rate_code_dim_table = "rate_code_dim"
payment_type_dim_table = "payment_type_dim"
fact_table = "fact_table"


storage_integration_name = "airflow_tlc_aws_s3_int"
bucket_data_directory = "s3://nyc-tlc-trip-data/yellow-taxi/"
external_stage_name = "airflow_tlc_aws_s3_stage"


create_warehouse_query = f"""
                           CREATE OR REPLACE WAREHOUSE {warehouse}
                           WITH
                           WAREHOUSE_TYPE = 'STANDARD'
                           WAREHOUSE_SIZE = 'X-SMALL'
                           INITIALLY_SUSPENDED = TRUE
                           SCALING_POLICY = 'STANDARD'
                           AUTO_SUSPEND = 600
                           COMMENT = 'creates a new virtual warehouse in the system'

                          """ 

create_database_query = f"""
                          CREATE OR REPLACE DATABASE {database}
                          DATA_RETENTION_TIME_IN_DAYS = 90
                          COMMENT = 'NYC TLC Database'

                         """ 

create_schema_query = f"""
                        USE DATABASE {database};
                        CREATE OR REPLACE SCHEMA {schema}
                        DATA_RETENTION_TIME_IN_DAYS = 90
                        COMMENT = 'Yellow Taxi'

                        """ 

create_passenger_count_dim = f"""
                               USE DATABASE {database};
                               USE SCHEMA {schema};
                               CREATE OR REPLACE TABLE {passenger_count_table} (
                                                                                 passenger_count_id INT PRIMARY KEY,
                                                                                 passenger_count INT
                                                                               )
                               STAGE_FILE_FORMAT = (TYPE = 'PARQUET')
                               COMMENT = 'create passenger count dimension'

                              """ 

create_trip_distance_dim = f"""
                             USE DATABASE {database};
                             USE SCHEMA {schema};
                             CREATE OR REPLACE TABLE {trip_distance_table} (
                                                                             trip_distance_id INT PRIMARY KEY,
                                                                             trip_distance FLOAT
                                                                           )
                             STAGE_FILE_FORMAT = (TYPE = 'PARQUET')
                             COMMENT = 'create trip distance dimension'

                            """

create_pickup_location_dim = f"""
                               USE DATABASE {database};
                               USE SCHEMA {schema};
                               CREATE OR REPLACE TABLE {pickup_location_table} (
                                                                                 pickup_location_id INT PRIMARY KEY,
                                                                                 PULocationID INT,
                                                                                 Borough TEXT,
                                                                                 Zone TEXT,
                                                                                 service_zone TEXT
                                                                               )
                               STAGE_FILE_FORMAT = (TYPE = 'PARQUET')
                               COMMENT = 'create pickup location dimension'

                              """

create_dropoff_location_dim = f"""
                                USE DATABASE {database};
                                USE SCHEMA {schema};
                                CREATE OR REPLACE TABLE {dropoff_location_table} (
                                                                                   dropoff_location_id INT PRIMARY KEY,
                                                                                   DOLocationID INT
                                                                                 )
                                STAGE_FILE_FORMAT = (TYPE = 'PARQUET')
                                COMMENT = 'create dropoff location dimension'

                                """

create_datetime_dim = f"""
                        USE DATABASE {database};
                        USE SCHEMA {schema};
                        CREATE OR REPLACE TABLE {datetime_dim_table} (
                                                                       datetime_id INT PRIMARY KEY,
                                                                       tpep_pickup_datetime	TIMESTAMP_NTZ,
                                                                       tpep_dropoff_datetime TIMESTAMP_NTZ
                                                                     )
                        STAGE_FILE_FORMAT = (TYPE = 'PARQUET')
                        COMMENT = 'create datetime dimension'

                        """

create_rate_code_dim = f"""
                         USE DATABASE {database};
                         USE SCHEMA {schema};
                         CREATE OR REPLACE TABLE {rate_code_dim_table} (
                                                                         rate_code_id INT PRIMARY KEY,
                                                                         RatecodeID INT,
                                                                         rate_code_name TEXT
                                                                       )
                         STAGE_FILE_FORMAT = (TYPE = 'PARQUET')
                         COMMENT = 'create rate code dimension'

                        """

create_payment_type_dim = f"""
                            USE DATABASE {database};
                            USE SCHEMA {schema};
                            CREATE OR REPLACE TABLE {payment_type_dim_table} (
                                                                               payment_type_id INT PRIMARY KEY,
                                                                               payment_type INT,
                                                                               payment_type_name TEXT
                                                                             )
                            STAGE_FILE_FORMAT = (TYPE = 'PARQUET')
                            COMMENT = 'create payment type dimension'

                            """

create_fact = f"""
                USE DATABASE {database};
                USE SCHEMA {schema};
                CREATE OR REPLACE TABLE {fact_table} (
                                                       VendorID INT PRIMARY KEY,
                                                       datetime_id INT REFERENCES datetime_dim (datetime_id),	
                                                       passenger_count_id	INT REFERENCES passenger_count_dim (passenger_count_id),
                                                       trip_distance_id INT REFERENCES trip_distance_dim (trip_distance_id),	
                                                       pickup_location_id INT REFERENCES pickup_location_dim (pickup_location_id),
                                                       dropoff_location_id INT REFERENCES dropoff_location_dim (dropoff_location_id),
                                                       rate_code_id INT REFERENCES rate_code_dim (rate_code_id),
                                                       payment_type_id INT REFERENCES payment_type_dim (payment_type_id)
                                                     )
                STAGE_FILE_FORMAT = (TYPE = 'PARQUET')
                COMMENT = 'create fact table'

                """

create_storage_integration = f"""
                              CREATE STORAGE INTEGRATION IF NOT EXISTS {storage_integration_name}
                              TYPE = EXTERNAL_STAGE
                              STORAGE_PROVIDER = 'S3'
                              STORAGE_AWS_ROLE_ARN = '{STORAGE_AWS_ROLE_ARN}'
                              ENABLED = TRUE
                              STORAGE_ALLOWED_LOCATIONS = ('{bucket_data_directory}')
                              COMMENT = 'create an aws storage integration for nyc taxi trip data'

                              """

grant_integration_access = f"""
                            USE DATABASE {database};
                            GRANT CREATE STAGE ON SCHEMA {schema} TO ROLE {SF_ROLE};
                            GRANT USAGE ON INTEGRATION {storage_integration_name} TO ROLE {SF_ROLE}

                            """

create_external_stage = f"""
                         USE SCHEMA {database}.{schema};
                         CREATE OR REPLACE STAGE {external_stage_name}
                         STORAGE_INTEGRATION = {storage_integration_name}
                         URL = '{bucket_data_directory}'
                         FILE_FORMAT = (TYPE = 'PARQUET')
                         COPY_OPTIONS = (ON_ERROR = 'ABORT_STATEMENT')
                         COMMENT = 'create an aws stage for nyc taxi trip data'

                         """

create_passenger_count_pipe = f"""
                               USE DATABASE {database};
                               USE SCHEMA {schema};
                               CREATE OR REPLACE PIPE passenger_count_dim_pipe
                               AUTO_INGEST = TRUE
                               COMMENT = 'Creates a passenger count dimension pipe'
                               AS
                               COPY INTO {database}.{schema}.{passenger_count_table}
                               FROM @{external_stage_name}/{passenger_count_table}
                               FILE_FORMAT = (TYPE = 'PARQUET')
                               MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE

                               """

create_trip_distance_pipe = f"""
                             USE DATABASE {database};
                             USE SCHEMA {schema};
                             CREATE OR REPLACE PIPE trip_distance_dim_pipe
                             AUTO_INGEST = TRUE
                             COMMENT = 'Creates a trip distance dimension pipe'
                             AS
                             COPY INTO {database}.{schema}.{trip_distance_table}
                             FROM @{external_stage_name}/{trip_distance_table}
                             FILE_FORMAT = (TYPE = 'PARQUET')
                             MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE

                             """

create_pickup_location_pipe = f"""
                               USE DATABASE {database};
                               USE SCHEMA {schema};
                               CREATE OR REPLACE PIPE pickup_location_dim_pipe
                               AUTO_INGEST = TRUE
                               COMMENT = 'Creates a pickup location dimension pipe'
                               AS
                               COPY INTO {database}.{schema}.{pickup_location_table}
                               FROM @{external_stage_name}/{pickup_location_table}
                               FILE_FORMAT = (TYPE = 'PARQUET')
                               MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE

                              """

create_dropoff_location_pipe = f"""
                                USE DATABASE {database};
                                USE SCHEMA {schema};
                                CREATE OR REPLACE PIPE dropoff_location_dim_pipe
                                AUTO_INGEST = TRUE
                                COMMENT = 'Creates a dropoff location dimension pipe'
                                AS
                                COPY INTO {database}.{schema}.{dropoff_location_table}
                                FROM @{external_stage_name}/{dropoff_location_table}
                                FILE_FORMAT = (TYPE = 'PARQUET')
                                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                              
                                """

create_datetime_pipe = f"""
                        USE DATABASE {database};
                        USE SCHEMA {schema};
                        CREATE OR REPLACE PIPE datetime_dim_pipe
                        AUTO_INGEST = TRUE
                        COMMENT = 'Creates a datetime dimension pipe'
                        AS
                        COPY INTO {database}.{schema}.{datetime_dim_table}
                        FROM @{external_stage_name}/{datetime_dim_table}
                        FILE_FORMAT = (TYPE = 'PARQUET')
                        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                              
                        """

create_rate_code_pipe = f"""
                         USE DATABASE {database};
                         USE SCHEMA {schema};
                         CREATE OR REPLACE PIPE rate_code_dim_pipe
                         AUTO_INGEST = TRUE
                         COMMENT = 'Creates a rate code dimension pipe'
                         AS
                         COPY INTO {database}.{schema}.{rate_code_dim_table}
                         FROM @{external_stage_name}/{rate_code_dim_table}
                         FILE_FORMAT = (TYPE = 'PARQUET')
                         MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                              
                         """

create_payment_type_pipe = f"""
                            USE DATABASE {database};
                            USE SCHEMA {schema};
                            CREATE OR REPLACE PIPE payment_type_dim_pipe
                            AUTO_INGEST = TRUE
                            COMMENT = 'Creates a payment type dimension pipe'
                            AS
                            COPY INTO {database}.{schema}.{payment_type_dim_table}
                            FROM @{external_stage_name}/{payment_type_dim_table}
                            FILE_FORMAT = (TYPE = 'PARQUET')
                            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                              
                            """

create_fact_pipe = f"""
                    USE DATABASE {database};
                    USE SCHEMA {schema};
                    CREATE OR REPLACE PIPE fact_table_pipe
                    AUTO_INGEST = TRUE
                    COMMENT = 'Creates a fact table pipe'
                    AS
                    COPY INTO {database}.{schema}.{fact_table}
                    FROM @{external_stage_name}/{fact_table}
                    FILE_FORMAT = (TYPE = 'PARQUET')
                    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                              
                    """