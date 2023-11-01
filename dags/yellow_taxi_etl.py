from sqlalchemy.util import deprecations
deprecations.SILENCE_UBER_WARNING = True
from airflow import DAG
from credentials import *
from pendulum import datetime
from sql_scripts.yellow_taxi import *
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


warehouse = "AIRFLOW_TLC_WH"

with DAG(dag_id="Nyc_Yellow_Taxi", schedule="@daily", start_date=datetime(2023, 10, 31, tz="Europe/Berlin"), catchup=False) as dag:
  create_warehouse = SQLExecuteQueryOperator(
                                              task_id="create_warehouse", 
                                              conn_id = "snowflake_default", 
                                              sql=create_warehouse_query
                                            )
  
  create_database = SQLExecuteQueryOperator(
                                             task_id="create_database", 
                                             conn_id ="snowflake_default", 
                                             sql=create_database_query,
                                             parameters={"role": SF_ROLE, "warehouse": warehouse}
                                           )
  
  create_schema = SQLExecuteQueryOperator(
                                           task_id="create_schema", 
                                           conn_id="snowflake_default", 
                                           sql=create_schema_query,
                                           parameters={"role": SF_ROLE, "warehouse": warehouse} 
                                         )
  
  create_passenger_count_table = SQLExecuteQueryOperator(
                                                          task_id="create_passenger_count_table", 
                                                          conn_id="snowflake_default",  
                                                          sql=create_passenger_count_dim, 
                                                          parameters={"role": SF_ROLE, "warehouse": warehouse} 
                                                        )
  
  create_trip_distance_table = SQLExecuteQueryOperator(
                                                        task_id="create_trip_distance_table", 
                                                        conn_id="snowflake_default", 
                                                        sql=create_trip_distance_dim, 
                                                        parameters={"role": SF_ROLE, "warehouse": warehouse} 
                                                      )
  
  create_pickup_location_table = SQLExecuteQueryOperator(
                                                          task_id="create_pickup_location_table", 
                                                          conn_id="snowflake_default",
                                                          sql=create_pickup_location_dim, 
                                                          parameters={"role": SF_ROLE, "warehouse": warehouse} 
                                                        )
  
  create_dropoff_location_table = SQLExecuteQueryOperator(
                                                           task_id="create_dropoff_location_table", 
                                                           conn_id="snowflake_default", 
                                                           sql=create_dropoff_location_dim, 
                                                           parameters={"role": SF_ROLE, "warehouse": warehouse} 
                                                         )
  
  create_datetime_table = SQLExecuteQueryOperator(
                                                   task_id="create_datetime_table", 
                                                   conn_id="snowflake_default",
                                                   sql=create_datetime_dim, 
                                                   parameters={"role": SF_ROLE, "warehouse": warehouse} 
                                                 )
  
  create_rate_code_table = SQLExecuteQueryOperator(
                                                    task_id="create_rate_code_table", 
                                                    conn_id="snowflake_default",
                                                    sql=create_rate_code_dim, 
                                                    parameters={"role": SF_ROLE, "warehouse": warehouse} 
                                                  )
  
  create_payment_type_table = SQLExecuteQueryOperator(
                                                       task_id="create_payment_type_table", 
                                                       conn_id="snowflake_default", 
                                                       sql=create_payment_type_dim, 
                                                       parameters={"role": SF_ROLE, "warehouse": warehouse} 
                                                     )
  
  create_fact_table = SQLExecuteQueryOperator(
                                               task_id="create_fact_table", 
                                               conn_id="snowflake_default",
                                               sql=create_fact, 
                                               parameters={"role": SF_ROLE, "warehouse": warehouse} 
                                             )
  
  create_external_storage = SQLExecuteQueryOperator(
                                                     task_id="create_external_storage_integration", 
                                                     conn_id="snowflake_default", 
                                                     sql=create_storage_integration, 
                                                     parameters={"role": SF_ROLE, "warehouse": warehouse} 
                                                   )
  
  grant_external_storage_access = SQLExecuteQueryOperator(
                                                           task_id="grant_privilege_to_external_storage_integration", 
                                                           conn_id="snowflake_default", 
                                                           sql=grant_integration_access, 
                                                           parameters={"role": SF_ROLE, "warehouse": warehouse} 
                                                         )
  
  create_external_stage = SQLExecuteQueryOperator(
                                                   task_id="create_external_stage", 
                                                   conn_id="snowflake_default", 
                                                   sql=create_external_stage, 
                                                   parameters={"role": SF_ROLE, "warehouse": warehouse} 
                                                 )
  
  load_passenger_count_table = SQLExecuteQueryOperator(
                                                        task_id="copy_data_into_passenger_count_table", 
                                                        conn_id="snowflake_default", 
                                                        sql=create_passenger_count_pipe, 
                                                        parameters={"role": SF_ROLE, "warehouse": warehouse}
                                                      )
  
  load_trip_distance_table = SQLExecuteQueryOperator(
                                                      task_id="copy_data_into_trip_distance_table", 
                                                      conn_id="snowflake_default", 
                                                      sql=create_trip_distance_pipe, 
                                                      parameters={"role": SF_ROLE, "warehouse": warehouse}
                                                    )
  
  load_pickup_location_table = SQLExecuteQueryOperator(
                                                        task_id="copy_data_into_pickup_location_table", 
                                                        conn_id="snowflake_default", 
                                                        sql=create_pickup_location_pipe, 
                                                        parameters={"role": SF_ROLE, "warehouse": warehouse}
                                                      )
  
  load_dropoff_location_table = SQLExecuteQueryOperator(
                                                         task_id="copy_data_into_dropff_location_table", 
                                                         conn_id="snowflake_default", 
                                                         sql=create_dropoff_location_pipe, 
                                                         parameters={"role": SF_ROLE, "warehouse": warehouse}
                                                       )
                                                     
  load_datetime_table = SQLExecuteQueryOperator(
                                                 task_id="copy_data_into_datetime_table", 
                                                 conn_id="snowflake_default",
                                                 sql=create_datetime_pipe, 
                                                 parameters={"role": SF_ROLE, "warehouse": warehouse}
                                               ) 

  load_rate_code_table = SQLExecuteQueryOperator(
                                                  task_id="copy_data_into_rate_code_table", 
                                                  conn_id="snowflake_default", 
                                                  sql=create_rate_code_pipe, 
                                                  parameters={"role": SF_ROLE, "warehouse": warehouse}
                                                )  

  load_payment_type_table = SQLExecuteQueryOperator(
                                                     task_id="copy_data_into_payment_type_table", 
                                                     conn_id="snowflake_default", 
                                                     sql=create_payment_type_pipe, 
                                                     parameters={"role": SF_ROLE, "warehouse": warehouse}
                                                   ) 

  load_fact_table = SQLExecuteQueryOperator(
                                             task_id="copy_data_into_fact_table", 
                                             conn_id="snowflake_default",
                                             sql=create_fact_pipe, 
                                             parameters={"role": SF_ROLE, "warehouse": warehouse}
                                           )      
  
  # set task dependencies
  create_warehouse >>\
  create_database >>\
  create_schema >>\
    [create_passenger_count_table, create_trip_distance_table, create_pickup_location_table, create_dropoff_location_table, create_datetime_table,\
      create_rate_code_table, create_payment_type_table] >>\
  create_fact_table >>\
  create_external_storage >>\
  grant_external_storage_access >>\
  create_external_stage >>\
  [load_passenger_count_table, load_trip_distance_table, load_pickup_location_table, load_dropoff_location_table, load_datetime_table,\
      load_rate_code_table, load_payment_type_table, load_fact_table]