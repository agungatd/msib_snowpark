import os
from dotenv import load_dotenv
from snowflake.snowpark import Session
import snowflake.snowpark.functions as F

load_dotenv()

connection_parameters = {
  "account": "jd01396.ap-southeast-3.aws",
  "user": "agungatd",
  "password": os.environ.get('password')
}

new_session = Session.builder.configs(connection_parameters).create()
user = new_session.get_current_user()
print(user)

new_session.sql("USE ROLE ACCOUNTADMIN").collect()
new_session.sql("USE DATABASE TEST_DB").collect()
new_session.sql("USE WAREHOUSE COMPUTE_WH").collect()

diamonds_df = new_session.table("diamonds")
# print(diamonds_df.show(5))

diamonds_pdf = diamonds_df.to_pandas()

expression = F.lower(F.col("CUT"))
print(diamonds_df.select(expression).show(5))

new_session.close()
