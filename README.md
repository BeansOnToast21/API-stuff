# Star Wars Data Pipline

## Introduction

An ETL pipeline for the extraction of star wars film data into a SQL Server database for easy querying and data retrieval.

## Table of Contents

1. Set-up Instructions
2. Contributing
3. Contributers
4. Future Improvemets
5. ERD

## Set-up instructions/ user guide

1. Install folder Star_Wars_Pipeline_Code
2. Install any missing imports. The code relies on pyodbc, pandas, sqlalchemy, requests, json
   using pip install.
3. Check you have 'ODBC Driver 18 for SQL Server', if not install it (internet search for download).
4. Create an empty database on sql server named - "starwars_db"
5. Ensure your database is running under windows authentication.
5. In Load.py check that the server name is correct for your system, modify as required.
6. Open runstarwarspipeline.py and click run
7. Once complete acces the database to query it.

## Contributing

1. Fork the repository.
2. Create a new branch: `git checkout -b feature-name`.
3. Make your changes.
4. Push your branch: `git push origin feature-name`.
5. Create a pull request.

## Contributers
- Joe Hutchison
- Mick Mullen
- Nix Wilson

## Future Improvements

- Create and incorporate junction tables for People_Starships and People_Vehicles
- Create foreign keys and relationships for Planet - people and People - species

### ERD
![Alt Text - ERD](url to the image you want to include)

