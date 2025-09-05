IF EXISTS (SELECT name FROM sys.databases WHERE name = N'StarWarsDB')
BEGIN
    ALTER DATABASE StarWarsDB SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE StarWarsDB;
END
GO

-- Create the database
CREATE DATABASE StarWarsDB;
GO

USE StarWarsDB;
GO

-- Character table
CREATE TABLE Character (
    CharacterID INT IDENTITY(1,1),
    Name VARCHAR(100) NOT NULL,
    Height VARCHAR(10),
    Mass VARCHAR(10),
    Hair_Color VARCHAR(50),
    Skin_Color VARCHAR(50),
    Eye_Color VARCHAR(50),
    Birth_Year VARCHAR(20),
    Gender VARCHAR(20),
    PlanetID INT
);

-- Species table 
CREATE TABLE Species (
    SpeciesID INT IDENTITY(1,1),
    CharacterID INT,
    Name VARCHAR(100),
    Classification VARCHAR(100)
);

-- Planet table 
CREATE TABLE Planet (
    PlanetID INT IDENTITY(1,1),
    Name VARCHAR(100),
    Rotation_Period VARCHAR(10),
    Orbital_Period VARCHAR(10),
    Diameter VARCHAR(10),
    Climate VARCHAR(100),
    Gravity VARCHAR(50),
    Terrain VARCHAR(100),
    Surface_Water BIT,
    Population VARCHAR(50)
);

-- Vehicle table 
CREATE TABLE Vehicle (
    VehicleID INT IDENTITY(1,1),
    CharacterID INT,
    Name VARCHAR(100),
    Model VARCHAR(100)
);

-- Starship table 
CREATE TABLE Starship (
    StarshipID INT IDENTITY(1,1),
    CharacterID INT,
    Name VARCHAR(100),
    Model VARCHAR(100)
);

-- Film table
CREATE TABLE Film (
    FilmID INT IDENTITY(1,1),
    Title VARCHAR(255),
    Episode_ID INT,
    Opening_Crawl TEXT,
    Director VARCHAR(100),
    Producer VARCHAR(100),
    Release_Date DATE
);

-- Composite tables
CREATE TABLE Character_Film (
    CharacterID INT,
    FilmID INT
);

CREATE TABLE Planet_Film (
    PlanetID INT,
    FilmID INT
);

CREATE TABLE Species_Film (
    SpeciesID INT,
    FilmID INT
);

CREATE TABLE Vehicle_Film (
    VehicleID INT,
    FilmID INT
);

CREATE TABLE Starship_Film ( 
    StarshipID INT,
    FilmID INT
);

CREATE TABLE Starship_Characters (
    StarshipID INT,
    CharacterID INT
);

CREATE TABLE Vehicles_Characters (
    VehicleID INT,
    CharacterID INT
);