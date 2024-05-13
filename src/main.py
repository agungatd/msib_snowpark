from snowflake.snowpark import Session
connection_parameters = {
  "account": "<your snowflake account>",
  "user": "<your snowflake user>",
  "password": "<your snowflake password>",
  "role": "<your snowflake role>",  # optional
  "warehouse": "<your snowflake warehouse>",  # optional
  "database": "<your snowflake database>",  # optional
  "schema": "<your snowflake schema>",  # optional
}  

new_session = Session.builder.configs(connection_parameters).create()

new_session.close()
