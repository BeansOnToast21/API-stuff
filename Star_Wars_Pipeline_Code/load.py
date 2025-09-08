import urllib.parse
from sqlalchemy import create_engine, text

import createtables
import modifytables


class load:
    def __init__(self) -> None:
        server = r"(localdb)\MSSQLLocalDB"
        database = "starwars_db"

        raw = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            "Trusted_Connection=yes;"
            "TrustServerCertificate=yes;"
        )

        params = urllib.parse.quote_plus(raw)
        self.engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

    def execute(self, dictionary_of_pandas_tables):
        print("Starting Load phase of pipeline.........")
        createtables.CreateTables().execute(dictionary_of_pandas_tables, self.engine)
        modifytables.ModifyTables().execute(self.engine)

   
# No need to call commit() – it commits automatically at the end of the 'with' block
