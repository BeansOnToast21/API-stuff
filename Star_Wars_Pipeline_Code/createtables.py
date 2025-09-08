import pandas as pd
import sqlalchemy
from sqlalchemy import text
class CreateTables:
    def __init__(self) -> None:
        pass

    def execute(self, pandas_dataframes: dict, engine: sqlalchemy.Engine):
        print("Starting to write tables to SQL Server......")
        try:
            self.__remove_foreign_keys(engine)
            for key, value in pandas_dataframes.items():
                self.__write_pandas_tables(value, key, engine)
        except Exception as e:
            print(f"Error writing pandas dataframes to sql. Error = {e}")

    def __write_pandas_tables(self, pandas_table: pd.DataFrame, table_name: str, engine):
        print(f"Writing table {table_name} to SQL Server")
        pandas_table.to_sql(table_name, con=engine, if_exists='replace', index=False)

    def __remove_foreign_keys(self, engine):
        try:
            with engine.begin() as conn:
                # remove Foreign key constraints
                conn.execute(text("ALTER TABLE dbo.Film_Character DROP CONSTRAINT  FK_Film_Character1 "))
                conn.execute(text("ALTER TABLE dbo.Film_Character DROP CONSTRAINT  FK_Film_Character2 "))

                conn.execute(text("ALTER TABLE dbo.Film_Planets DROP CONSTRAINT  FK_Film_Planet1"))
                conn.execute(text("ALTER TABLE dbo.Film_Planets DROP CONSTRAINT  FK_Film_Planet2 "))

                conn.execute(text("ALTER TABLE dbo.Film_Starships DROP CONSTRAINT  FK_Film_Starship1 "))
                conn.execute(text("ALTER TABLE dbo.Film_Starships DROP CONSTRAINT  FK_Film_Starship2 "))  

                conn.execute(text("ALTER TABLE dbo.Film_Vehicles DROP CONSTRAINT  FK_Film_Vehicle1 "))
                conn.execute(text("ALTER TABLE dbo.Film_Vehicles DROP CONSTRAINT  FK_Film_Vehicle2 "))

                conn.execute(text("ALTER TABLE dbo.Film_Species DROP CONSTRAINT  FK_Film_Species1 "))
                conn.execute(text("ALTER TABLE dbo.Film_Species DROP CONSTRAINT  FK_Film_Species2 "))   
                conn.commit()
        except Exception as e:
            print(f"Error removing foreign keys. See error : {e}")