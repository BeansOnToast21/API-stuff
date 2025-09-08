import pandas as pd
import json


class Transform:
    def __init__(self):
        self.pk_charactors_id_map = []
        self.pk_planets_id_map = []
        self.pk_species_id_map = []
        self.pk_vehicles_id_map = []
        self.pk_starships_id_map = []

    # Entry point for transform methods
    def execute(self, dict_of_json_files: dict):
        print("Starting Transformation stage ...........")
        self.cleaned = {}
        try:
            for key, value in dict_of_json_files.items():
                print(f"Cleaning dataframe {key}.")
                df = self.__clean_df(value)
                self.cleaned[f"{key}"] = pd.DataFrame(df)
            self.__create_primary_key_maps(self.cleaned["films"])
            self.__apply_primary_key_maps()
            self.__create_junction_tables(self.cleaned)
            return self.cleaned
        except Exception as e:
            print(f"Error in transformation. See error : {e} ")    

    # Clean the dataframes
    def __clean_df(self, records):
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
    
    # Create primary key maps from each tables stored page urls.
    def __create_primary_key_maps(self, df_films: pd.DataFrame):
        print("Creating primary key maps")
        self.pk_film_id_map = {title: idx+1 for idx, title in enumerate(df_films['title'])}

        for url in self.cleaned["people"]['url']:
            self.pk_charactors_id_map.extend(self.__extract_ids_from_urls(url))
        for url in self.cleaned["planets"]['url']:
            self.pk_planets_id_map.extend(self.__extract_ids_from_urls(url))
        for url in self.cleaned["species"]['url']:
            self.pk_species_id_map.extend(self.__extract_ids_from_urls(url))
        for url in self.cleaned["vehicles"]['url']:
            self.pk_vehicles_id_map.extend(self.__extract_ids_from_urls(url))
        for url in self.cleaned["starships"]['url']:
            self.pk_starships_id_map.extend(self.__extract_ids_from_urls(url))

        self.pk_charactors_id_map = list(set(self.pk_charactors_id_map))
        self.pk_planets_id_map = list(set(self.pk_planets_id_map))
        self.pk_species_id_map = list(set(self.pk_species_id_map))
        self.pk_vehicles_id_map = list(set(self.pk_vehicles_id_map))
        self.pk_starships_id_map = list(set(self.pk_starships_id_map))

    # add the primary keys as a new column at the start of the table
    def __apply_primary_key_maps(self):
        print("Adding primary keys to dataframes")
        self.cleaned["films"].insert(0, "FilmID", list(self.pk_film_id_map.values()), False)
        self.cleaned["people"].insert(0, "CharacterID", self.pk_charactors_id_map, False)
        self.cleaned["planets"].insert(0, "PlanetID", self.pk_planets_id_map, False)
        self.cleaned["species"].insert(0, "SpeciesID", self.pk_species_id_map, False)
        self.cleaned["vehicles"].insert(0, "VehicleID", self.pk_vehicles_id_map, False)
        self.cleaned["starships"].insert(0, "StarshipID", self.pk_starships_id_map, False)

    # Generate junction tables for Films to other tables from the main url references in films
    def __create_junction_tables(self, list_of_dataframes):
        print("Generating junction tables......")
        film_characters_junction = []
        film_planets_junction = []
        film_starships_junction = []
        film_vehicles_junction = []
        film_species_junction = []
        # --- Extract junction data ---
        for _, film in list_of_dataframes["films"].iterrows():
            film_id = self.pk_film_id_map.get(film['title'])
        # Characters
            character_ids = self.__extract_ids_from_urls(film.get('characters'))
            for cid in character_ids:
                if cid <= len(list_of_dataframes["people"]):
                    film_characters_junction.append({'film_id': film_id, 'character_id': cid})

        # Planets
            planet_ids = self.__extract_ids_from_urls(film.get('planets'))
            for pid in planet_ids:
                if pid <= len(list_of_dataframes["planets"]):
                    film_planets_junction.append({'film_id': film_id, 'planet_id': pid})        
        
        # Species
            species_ids = self.__extract_ids_from_urls(film.get('species'))
            for spid in species_ids:
                if spid <= len(list_of_dataframes["species"]):
                    film_species_junction.append({'film_id': film_id, 'species_id': spid})

        # Vehicles
            vehicle_ids = self.__extract_ids_from_urls(film.get('vehicles'))
            for vid in vehicle_ids:
                if vid <= len(list_of_dataframes["vehicles"]):
                    film_vehicles_junction.append({'film_id': film_id, 'vehicle_id': vid})

        # Starships
            starship_ids = self.__extract_ids_from_urls(film.get('starships'))
            for sid in starship_ids:
                if sid <= len(list_of_dataframes["starships"]):
                    film_starships_junction.append({'film_id': film_id, 'starship_id': sid})

# --- Convert junction lists to DataFrames and add to tables list---
        self.cleaned["Film_Character"] = pd.DataFrame(film_characters_junction).drop_duplicates()
        self.cleaned["Film_Planets"] = pd.DataFrame(film_planets_junction).drop_duplicates()
        self.cleaned["Film_Starships"] = pd.DataFrame(film_starships_junction).drop_duplicates()
        self.cleaned["Film_Vehicles"] = pd.DataFrame(film_vehicles_junction).drop_duplicates()
        self.cleaned["Film_Species"] = pd.DataFrame(film_species_junction).drop_duplicates()



    # --- Extract IDs from comma-separated URLs ---
    def __extract_ids_from_urls(self,url_str):
        # if pd.isna(url_str):
        #     return []
        urls = url_str.split(',')
        ids = [int(url.rstrip('/').split('/')[-1]) for url in urls]
        return ids

