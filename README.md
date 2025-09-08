# Star Wars Data Pipline

## Introduction

An ETL pipeline for the extraction of star wars film data into a SQL Server database for easy querying and data retrieval.

## Table of Contents

1. Set-up Instructions
2. Usage
3. Contributing
4. Contributers
5. Future Improvemets
6. ERD

## Set-up instructions/ user guide

1. Install folder Star_Wars_Pipeline_Code
2. Install any missing imports. The code relies on pyodbc, pandas, sqlalchemy, requests, json
   using pip install.
3. Check you have 'ODBC Driver 18 for SQL Server', if not install it (internet search for download).
4. Create an empty database on sql server named - "starwars_db"
5. Ensure your database is running under windows authentication.
<<<<<<< HEAD
6. In Load.py check that the server name is correct for your system, modify as required.
7. 7Open runstarwarspipeline.py and click run
=======
5. In Load.py check that the server name is correct for your system, modify as required.
   ![This is the section of code you may need to alter within Load.py](https://github.com/BeansOnToast21/API-stuff/blob/main/images/Server_set_up_in_python.PNG?raw=true)
7. Open runstarwarspipeline.py and click run
8. Once complete acces the database to query it.
>>>>>>> b090d4516bf101604b57077c62ada5bd6194e283

## usage

Use standard sql syntax to query the database:

SELECT s.*
FROM starships AS s
INNER JOIN Film_Starships fs ON s.StarshipID = fs.starship_id
INNER JOIN films f ON f.FilmID = fs.film_id
WHERE f.FilmID = 2;

brings up all the starships used in film 2

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
![Alt Text - ERD](https://github.com/BeansOnToast21/API-stuff/blob/main/Docs/Star_Wars_ERD.drawio%20(1).png?raw=true)

