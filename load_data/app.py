from collections import defaultdict
from snowflake.connector.pandas_tools import pd_writer
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
import os
import pandas as pd

dtypes = defaultdict(lambda: "str")

engine = create_engine(URL(
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE")
))

current_dir = os.getcwd()
data_dir = os.path.join(current_dir, "data")
for _, _, files in os.walk(data_dir):
    for f in files:
        filename = os.path.join(data_dir, f)
        name = f.split(".")[0]
        df = pd.read_csv(filename, dtype=dtypes)
        df.columns = df.columns.str.upper()
        try:
            df.to_sql(name, engine, if_exists="replace", index=False, method=pd_writer)
            print(f"{f} loaded...")
        except Exception as e:
            print(f"{f} not loaded...")
            print(e)
