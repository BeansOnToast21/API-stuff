import requests
import json
import pandas as pd


target_people = requests.get("https://swapi.info/api/people")
data1 = target_people.json()

target_films = requests.get("https://swapi.info/api/films")
data2 = target_films.json()
print(data2)

film_characters_junction = []

film_id_map = {}
film_counter = 1


for entry in data2:
    title = entry["title"]
    character_urls = entry["characters"]

    if title and character_urls:
        if title not in film_id_map:
            film_id_map[title] = film_counter
            film_counter += 1

        film_id = film_id_map[title]

        for url in character_urls:
            character_id = int(url.split("/")[-1])
            film_characters_junction .append({
                    "film_id": film_id,
                    "character_id": character_id })
#print(film_characters_junction)



film_planets_junction = []

film_id_map = {}
film_counter = 1


for entry in data2:
    title = entry["title"]
    planet_urls = entry["planets"]

    if title and planet_urls:
        if title not in film_id_map:
            film_id_map[title] = film_counter
            film_counter += 1

        film_id = film_id_map[title]

        for url in planet_urls:
            planet_id = int(url.split("/")[-1])
            film_planets_junction.append({
                    "film_id": film_id,
                    "planet_id": character_id })
#print(film_planets_junction)

film_starship_junction = []

film_id_map = {}
film_counter = 1


for entry in data2:
    title = entry["title"]
    starship_urls = entry["starships"]

    if title and planet_urls:
        if title not in film_id_map:
            film_id_map[title] = film_counter
            film_counter += 1

        film_id = film_id_map[title]

        for url in starship_urls:
            starship_id = int(url.split("/")[-1])
            film_starship_junction.append({
                    "film_id": film_id,
                    "planet_id": starship_id })

print(film_starship_junction)


film_vehicles_junction = []

film_id_map = {}
film_counter = 1


for entry in data2:
    title = entry["title"]
    vehicle_urls = entry["vehicles"]

    if title and planet_urls:
        if title not in film_id_map:
            film_id_map[title] = film_counter
            film_counter += 1

        film_id = film_id_map[title]

        for url in vehicle_urls:
            vehicle_id = int(url.split("/")[-1])
            film_vehicles_junction.append({
                    "film_id": film_id,
                    "vehicle_id": vehicle_id })

print(film_vehicles_junction)

film_species_junction = []

film_id_map = {}
film_counter = 1


for entry in data2:
    title = entry["title"]
    species_urls = entry["species"]

    if title and species_urls:
        if title not in film_id_map:
            film_id_map[title] = film_counter
            film_counter += 1

        film_id = film_id_map[title]

        for url in species_urls:
            species_id = int(url.split("/")[-1])
            film_species_junction.append({
                    "film_id": film_id,
                    "species_id": species_id })




#print(film_species_junction)

df = pd.DataFrame(film_species_junction)
print(df)



from sqlalchemy import create_engine

server = r'(localdb)\ProjectModels'
database = 'StarWarsDB'

connection_string = (
    f"mssql+pyodbc://@{server}/{database}"
    "?driver=ODBC+Driver+17+for+SQL+Server"
    "&trusted_connection=yes"
)

engine = create_engine(connection_string)


df.to_sql('Species_Film', con=engine, if_exists='replace', index=False)




