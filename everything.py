import requests
import pandas as pd
import json
from sqlalchemy import create_engine

# --- Helper to fetch all pages from SWAPI ---
def fetch_all(endpoint):
    url = f"https://swapi.dev/api/{endpoint}/"
    results = []
    while url:
        resp = requests.get(url, verify=False)  # SSL bypass for Windows if needed
        resp.raise_for_status()
        data = resp.json()
        results.extend(data["results"])
        url = data.get("next")
    return results

# --- Convert SWAPI JSON to clean DataFrame ---
def clean_df(records):
    df = pd.DataFrame(records)
    df.replace({"unknown": pd.NA, "n/a": pd.NA, "": pd.NA}, inplace=True)

    for col in df.columns:
        # Convert lists to comma-separated strings
        if df[col].apply(lambda x: isinstance(x, list)).any():
            df[col] = df[col].apply(lambda x: ",".join(x) if isinstance(x, list) else x)
        # Convert dicts to JSON strings
        elif df[col].apply(lambda x: isinstance(x, dict)).any():
            df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, dict) else x)

    # Drop rows where all columns are NA
    df.dropna(how='all', inplace=True)
    # Drop duplicate rows
    df.drop_duplicates(inplace=True)
    return df

# --- Extract IDs from comma-separated URLs ---
def extract_ids_from_urls(url_str):
    if pd.isna(url_str):
        return []
    urls = url_str.split(',')
    ids = [int(url.rstrip('/').split('/')[-1]) for url in urls]
    return ids

print("Fetching SWAPI data...")
df_characters = clean_df(fetch_all("people"))
df_planets     = clean_df(fetch_all("planets"))
df_films       = clean_df(fetch_all("films"))
df_species     = clean_df(fetch_all("species"))
df_vehicles    = clean_df(fetch_all("vehicles"))
df_starships   = clean_df(fetch_all("starships"))

print("Data fetched and cleaned.")

# --- Build film_id map (title -> id) ---
film_id_map = {title: idx+1 for idx, title in enumerate(df_films['title'])}

# --- Initialize lists to store junction records ---
film_characters_junction = []
film_planets_junction = []
film_starships_junction = []
film_vehicles_junction = []
film_species_junction = []

# --- Extract junction data ---
for _, film in df_films.iterrows():
    film_id = film_id_map.get(film['title'])
    
    # Characters
    character_ids = extract_ids_from_urls(film.get('characters'))
    for cid in character_ids:
        film_characters_junction.append({'film_id': film_id, 'character_id': cid})

    # Planets
    planet_ids = extract_ids_from_urls(film.get('planets'))
    for pid in planet_ids:
        film_planets_junction.append({'film_id': film_id, 'planet_id': pid})

    # Starships
    starship_ids = extract_ids_from_urls(film.get('starships'))
    for sid in starship_ids:
        film_starships_junction.append({'film_id': film_id, 'starship_id': sid})

    # Vehicles
    vehicle_ids = extract_ids_from_urls(film.get('vehicles'))
    for vid in vehicle_ids:
        film_vehicles_junction.append({'film_id': film_id, 'vehicle_id': vid})

    # Species
    species_ids = extract_ids_from_urls(film.get('species'))
    for spid in species_ids:
        film_species_junction.append({'film_id': film_id, 'species_id': spid})

# --- Convert junction lists to DataFrames ---
df_film_characters = pd.DataFrame(film_characters_junction).drop_duplicates()
df_film_planets = pd.DataFrame(film_planets_junction).drop_duplicates()
df_film_starships = pd.DataFrame(film_starships_junction).drop_duplicates()
df_film_vehicles = pd.DataFrame(film_vehicles_junction).drop_duplicates()
df_film_species = pd.DataFrame(film_species_junction).drop_duplicates()

# --- SQL Server connection setup ---
server = r'(localdb)\MSSQLLocalDB'
database = 'StarWarsDB'

connection_string = (
    f"mssql+pyodbc://{server}/{database}"
    "?driver=ODBC+Driver+17+for+SQL+Server"
    "&trusted_connection=yes"
)

engine = create_engine(connection_string)

print("Writing main tables to SQL Server...")

df_characters.to_sql('Characters', con=engine, if_exists='replace', index=False)
df_planets.to_sql('Planets', con=engine, if_exists='replace', index=False)
df_films.to_sql('Films', con=engine, if_exists='replace', index=False)
df_species.to_sql('Species', con=engine, if_exists='replace', index=False)
df_vehicles.to_sql('Vehicles', con=engine, if_exists='replace', index=False)
df_starships.to_sql('Starships', con=engine, if_exists='replace', index=False)

print("Writing junction tables to SQL Server...")

df_film_characters.to_sql('Film_Character', con=engine, if_exists='replace', index=False)
df_film_planets.to_sql('Film_Planet', con=engine, if_exists='replace', index=False)
df_film_starships.to_sql('Film_Starship', con=engine, if_exists='replace', index=False)
df_film_vehicles.to_sql('Film_Vehicle', con=engine, if_exists='replace', index=False)
df_film_species.to_sql('Film_Species', con=engine, if_exists='replace', index=False)

print("Done! All tables have been written without null-only rows or duplicates.")
