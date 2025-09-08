import sqlalchemy
from sqlalchemy import text

class ModifyTables:
    def __init__(self) -> None:
        pass

    def execute(self, engine: sqlalchemy.Engine):
        self.__set_primary_keys(engine)

    def __set_primary_keys(self, engine: sqlalchemy.Engine):
        print("Writing main tables to SQL Server...")

        with engine.begin() as conn:
            conn.execute(text("ALTER TABLE dbo.films ALTER COLUMN FilmID BIGINT NOT NULL"))    
            conn.execute(text("ALTER TABLE dbo.people ALTER COLUMN CharacterID BIGINT NOT NULL"))   
            conn.execute(text("ALTER TABLE dbo.planets ALTER COLUMN PlanetID BIGINT NOT NULL"))  
            conn.execute(text("ALTER TABLE dbo.species ALTER COLUMN SpeciesID BIGINT NOT NULL"))
            conn.execute(text("ALTER TABLE dbo.vehicles ALTER COLUMN VehicleID BIGINT NOT NULL"))
            conn.execute(text("ALTER TABLE dbo.starships ALTER COLUMN StarshipID BIGINT NOT NULL"))

        print("Setting up primary keys........")
        try:
            with engine.begin() as conn:  # This starts a transaction automatically
                conn.execute(text("ALTER TABLE dbo.films ADD PRIMARY KEY (FilmID)"))
                conn.execute(text("ALTER TABLE dbo.people ADD PRIMARY KEY (CharacterID)"))
                conn.execute(text("ALTER TABLE dbo.planets ADD PRIMARY KEY(PlanetID)"))
                conn.execute(text("ALTER TABLE dbo.species ADD PRIMARY KEY (SpeciesID)"))
                conn.execute(text("ALTER TABLE dbo.vehicles ADD PRIMARY KEY (VehicleID)"))
                conn.execute(text("ALTER TABLE dbo.starships ADD PRIMARY KEY (StarshipID)"))
                conn.commit()
        except Exception as e:
            print(f"Failed to add primary keys. Primary Keys may already be set. See error : {e}")
        try:
            print("Adding foreign key constariants. This can be a long running process .........")

            with engine.begin() as conn:
                # Foreign key constraints
                conn.execute(text("ALTER TABLE dbo.Film_Character ADD CONSTRAINT FK_Film_Character1 FOREIGN KEY (film_id) REFERENCES dbo.films(FilmID)"))
                conn.execute(text("ALTER TABLE dbo.Film_Character ADD CONSTRAINT FK_Film_Character2 FOREIGN KEY (character_id) REFERENCES dbo.people (CharacterID)"))

                conn.execute(text("ALTER TABLE dbo.Film_Planets ADD CONSTRAINT FK_Film_Planet1 FOREIGN KEY (film_id) REFERENCES dbo.films(FilmID)"))
                conn.execute(text("ALTER TABLE dbo.Film_Planets ADD CONSTRAINT FK_Film_Planet2 FOREIGN KEY (planet_id) REFERENCES dbo.planets (PlanetID)"))

                conn.execute(text("ALTER TABLE dbo.Film_Starships ADD CONSTRAINT FK_Film_Starship1 FOREIGN KEY (film_id) REFERENCES dbo.films(FilmID)"))
                conn.execute(text("ALTER TABLE dbo.Film_Starships ADD CONSTRAINT FK_Film_Starship2 FOREIGN KEY (starship_id) REFERENCES dbo.starships (StarshipID)"))  

                conn.execute(text("ALTER TABLE dbo.Film_Vehicles ADD CONSTRAINT FK_Film_Vehicle1 FOREIGN KEY (film_id) REFERENCES dbo.films(FilmID)"))
                conn.execute(text("ALTER TABLE dbo.Film_Vehicles ADD CONSTRAINT FK_Film_Vehicle2 FOREIGN KEY (vehicle_id) REFERENCES dbo.vehicles (VehicleID)"))

                conn.execute(text("ALTER TABLE dbo.Film_Species ADD CONSTRAINT FK_Film_Species1 FOREIGN KEY (film_id) REFERENCES dbo.films(FilmID)"))
                conn.execute(text("ALTER TABLE dbo.Film_Species ADD CONSTRAINT FK_Film_Species2 FOREIGN KEY (species_id) REFERENCES dbo.species (SpeciesID)"))   
                conn.commit()
        except Exception as e:
            print(f"Failed to set foreign key constraints. Foreign keys may already be set. See error : {e}")
        print("Pipeline Completed")
        