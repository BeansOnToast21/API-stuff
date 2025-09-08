import requests
import pandas as pd


class Extract:
    def __init__(self) -> None:
        pass


    def execute(self):
        print("Starting Extract phase of pipeline .......")

        endpoints = ["films", "people", "planets", "species", "vehicles", "starships"]
        all_json_files = {}
        for self.__end in endpoints:
            all_json_files[f"{self.__end}"] = pd.DataFrame(self.__fetch_all(self.__end))
        return all_json_files

    
    # --- Helper to fetch all pages from SWAPI ---
    def __fetch_all(self, endpoint):
        print(f"Fetching {endpoint} From SWAPI ......")

        url = f"https://swapi.dev/api/{endpoint}/"
        results = []
        while url:
            resp = requests.get(url, verify=False)  # SSL bypass for Windows if needed
            resp.raise_for_status()
            data = resp.json()
            results.extend(data["results"])
            url = data.get("next")
        return results
        
        


