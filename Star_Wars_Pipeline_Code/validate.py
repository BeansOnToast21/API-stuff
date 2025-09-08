class Validate:
    def __init__(self):
        pass
    
    validation_films = ["title", "episode_id", "opening_crawl", "director", "producer", "release_date", "characters", "planets", "starships", "vehicles", "species"]
    validation_people = ["name", "height", "mass", "hair_color", "skin_color", "eye_color", "birth_year", "Gender", "homeworld", "films", "species", "vehicles", "starships"]  
    validation_planets = ["name", "rotation_period", "orbital_period", "diameter", "climate", "gravity", "terrain", "surface_water", "population", "residents"]  
    validation_starships = ["name", "model", "manufacturer", "cost_in_credits", "length", "max_atmosphering_speed", "crew", "passengers", "cargo_capacity", "consumables", "hyperdrive_rating", "MGLT", "starship_class"]
    validation_vehicles = ["name", "model", "manufacturer", "cost_in_credits", "length", "max_atmosphering_speed", "crew", "passengers", "cargo_capacity", "consumables", "vehicle_class"]
    validation_species = ["name", "classification", "designation", "average_height", "skin_colors", "hair_colors", "eye_colors", "average_lifespan", "homeworld", "language"]


    def run_validate(self, data_list):
        for row in data_list:
            val = self.validate_data(row.keys(), row.values())


    def validate_data(self, data_dict, value) -> bool: 
        val= []
        if "films" in data_dict:
            val = self.validation_films
        if "charactors" in data_dict:
            val = self.validation_people
        missing_columns = [col for col in val if col not in value.columns]
        if missing_columns:
            print(f"Validation failed. Missing columns: {missing_columns}")
            return False
        print("Validation passed.")
        return True
