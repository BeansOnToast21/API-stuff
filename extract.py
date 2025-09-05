import requests
import pandas as pd

# --- Helper to fetch all pages from SWAPI ---
def fetch_all(endpoint):
    url = f"https://swapi.dev/api/{endpoint}/"
    results = []
    while url:
        resp = requests.get(url, verify=False)  # SSL bypass for Windows
        resp.raise_for_status()
        data = resp.json()
        results.extend(data["results"])
        url = data.get("next")
    return results

# --- Convert SWAPI JSON to clean DataFrame ---
def clean_df(records):
    df = pd.DataFrame(records)
    df.replace({"unknown": pd.NA, "n/a": pd.NA, "": pd.NA}, inplace=True)
    
    # Convert list columns to comma-separated strings for deduplication
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, list)).any():
            df[col] = df[col].apply(lambda x: ",".join(x) if isinstance(x, list) else x)
    
    df.drop_duplicates(inplace=True)
    return df

# --- Extract all SWAPI tables into DataFrames ---
df_characters = clean_df(fetch_all("people"))
df_planets     = clean_df(fetch_all("planets"))
df_films       = clean_df(fetch_all("films"))
df_species     = clean_df(fetch_all("species"))
df_vehicles    = clean_df(fetch_all("vehicles"))
df_starships   = clean_df(fetch_all("starships"))

# --- Quick summary to verify extraction ---
print("Extraction complete! Row counts per table:")
print("Characters:", len(df_characters))
print("Planets:", len(df_planets))
print("Films:", len(df_films))
print("Species:", len(df_species))
print("Vehicles:", len(df_vehicles))
print("Starships:", len(df_starships))


