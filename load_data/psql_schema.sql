CREATE SCHEMA IF NOT EXISTS analysis;

DROP TABLE IF EXISTS analysis.appearances CASCADE;
CREATE TABLE analysis.appearances (
    game_id INT,
    player_id INT,
    appearance_id VARCHAR(32) PRIMARY KEY,
    player_club_id INT,
    player_name VARCHAR(128),
    competition_id VARCHAR(10),
    yellow_cards INT,
    red_cards INT,
    goals INT,
    assists INT,
    minutes_played INT,
    type VARCHAR(20),
    position VARCHAR(64),
    number INT,
    team_captain BOOLEAN
);

DROP TABLE IF EXISTS analysis.clubs CASCADE;
CREATE TABLE  analysis.clubs (
    club_id INT PRIMARY KEY,
    club_name VARCHAR(128),
    competition_id VARCHAR(10),
    squad_size INT,
    average_age DECIMAL(4,2),
    foreigners_percentage DECIMAL(5,2),
    stadium_name VARCHAR(128),
    name VARCHAR(128)
);

DROP TABLE IF EXISTS analysis.players CASCADE;
CREATE TABLE analysis.players (
    player_id INT PRIMARY KEY,
    player_name VARCHAR(128),
    current_club_name VARCHAR(128),
    date_of_birth VARCHAR(60),
    position VARCHAR(64),
    hometown VARCHAR(128),
    foot VARCHAR(16) NULL,
    height_in_cm INT NULL,
    image_url TEXT,
    market_value_in_eur INT NULL,    -- Added NULL to allow null values
    highest_market_value_in_eur INT NULL    -- Added NULL to allow null values
);
DROP TABLE IF EXISTS analysis.games CASCADE;
CREATE TABLE  analysis.games (
  game_id INT,
  competition_id VARCHAR(10),
  season INT,
  round VARCHAR(50),
  date DATE,
  home_club_id INT,
  away_club_id INT,
  home_club_goals INT,
  away_club_goals INT,
  home_club_position INT,
  away_club_position INT,
  home_club_manager_name VARCHAR(128),
  away_club_manager_name VARCHAR(128),
  stadium VARCHAR(128),
  attendance INT,
  referee VARCHAR(128),
  url TEXT,
  home_club_formation VARCHAR(64),
  away_club_formation VARCHAR(64),
  home_club_name VARCHAR(128),
  away_club_name VARCHAR(128),
  aggregate VARCHAR(10),
  competition_type VARCHAR(50)
);

DROP TABLE IF EXISTS analysis.transfer CASCADE;
CREATE TABLE analysis.transfer (
  player_id INT,
  player_name VARCHAR(128),
  transfer_season VARCHAR(10),
  from_club_name VARCHAR(128),
  to_club_name VARCHAR(128),
  transfer_market_value_in_eur DECIMAL(15, 3),
  player_name_playerdf VARCHAR(128)
);