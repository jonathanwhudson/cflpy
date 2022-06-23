table_creation_queries = {}

table_creation_queries["leagues"] = '''
CREATE TABLE "leagues" (
    league_id int NOT NULL,
    league_name text NOT NULL,
    league_abbr text NOT NULL,
    PRIMARY KEY (league_id)
);'''

table_creation_queries["seasons"] = '''
CREATE TABLE "seasons" (
    league_id int NOT NULL,
    year int NOT NULL,
    type text NOT NULL,
    season_start date NOT NULL,
    season_end date NOT NULL,
    PRIMARY KEY (league_id, year, type),
    FOREIGN KEY (league_id) REFERENCES leagues(league_id)
);'''
