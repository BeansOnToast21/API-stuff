import pandas as pd
import sqlalchemy

class CreateTables:
    def __init__(self) -> None:
        pass

    def execute(self, pandas_dataframes: dict, engine: sqlalchemy.Engine):
        print("Starting to write tables to SQL Server......")
        try:
            for key, value in pandas_dataframes.items():
                self.write_pandas_tables(value, key, engine)
        except Exception as e:
            print(f"Error writing pandas dataframes to sql. Error = {e}")

    def write_pandas_tables(self, pandas_table: pd.DataFrame, table_name: str, engine):
        print(f"Writing table {table_name} to SQL Server")
        pandas_table.to_sql(table_name, con=engine, if_exists='replace', index=False)