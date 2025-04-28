-- -- Tạo bảng leagues
-- DROP TABLE IF EXISTS leagues;
-- CREATE TABLE leagues (
--   leagueID int NOT NULL, 
--   name varchar(32), 
--   understatNotation varchar(32),
--   PRIMARY KEY (leagueID)
-- );

-- -- Tạo bảng teams
-- DROP TABLE IF EXISTS teams; 
-- CREATE TABLE teams (
--   teamID int NOT NULL, 
--   name varchar(32),
--   PRIMARY KEY (teamID)
-- );

-- -- Tạo bảng players
-- DROP TABLE IF EXISTS players;
-- CREATE TABLE players (
--   playerID int NOT NULL,
--   name varchar(32),
--   PRIMARY KEY (playerID)
-- );

-- -- Tạo bảng games
-- DROP TABLE IF EXISTS games; 
-- CREATE TABLE games (
--   gameID int NOT NULL, 
--   leagueID int NOT NULL,
--   season int, 
--   date timestamp, 
--   homeTeamID int NOT NULL, 
--   awayTeamID int NOT NULL, 
--   homeGoals int4, 
--   awayGoals int4, 
--   homeProbability float4, 
--   drawProbability float4, 
--   awayProbability float4, 
--   homeGoalsHalfTime int4, 
--   awayGoalsHalfTime int4, 
--   B365H float4, 
--   B365D float4, 
--   B365A float4, 
--   BWH float4, 
--   BWD float4, 
--   BWA float4, 
--   IWH float4, 
--   IWD float4, 
--   IWA float4, 
--   PSH float4, 
--   PSD float4, 
--   PSA float4, 
--   WHH float4, 
--   WHD float4, 
--   WHA float4, 
--   VCH float4, 
--   VCD float4, 
--   VCA float4, 
--   PSCH float4, 
--   PSCD float4, 
--   PSCA float4,
--   PRIMARY KEY (gameID)

-- );

-- -- Tạo bảng teamstats
-- DROP TABLE IF EXISTS teamstats; 
-- CREATE TABLE teamstats (
--   gameID int, 
--   teamID int, 
--   season int4, 
--   date datetime, 
--   location varchar(32), 
--   goals int4, 
--   xGoals float4, 
--   shots int4, 
--   shotsOnTarget int4, 
--   deep int4, 
--   ppda float4, 
--   fouls int4, 
--   corners int4, 
--   yellowCards float4, 
--   redCards int4, 
--   result varchar(32),
--   PRIMARY KEY (gameID, teamID)

-- );

-- -- Tạo bảng appearances
-- DROP TABLE IF EXISTS appearances;
-- CREATE TABLE appearances (
--   gameID int NOT NULL, 
--   playerID int NOT NULL, 
--   goals int4, 
--   ownGoals int4, 
--   shots int4, 
--   xGoals float4, 
--   xGoalsChain float4, 
--   xGoalsBuildup float4, 
--   assists int4, 
--   keyPasses int4, 
--   xAssists float4, 
--   position varchar(32), 
--   positionOrder int4, 
--   yellowCard int4, 
--   redCard int4, 
--   time int4, 
--   substituteIn int4, 
--   substituteOut int4, 
--   leagueID int NOT NULL,
--   PRIMARY KEY (gameID, playerID)

-- );

-- -- Tạo bảng shots
-- DROP TABLE IF EXISTS shots;
-- CREATE TABLE shots (
--   gameID int NOT NULL,
--   shooterID int NOT NULL,
--   assisterID int,
--   minute int,
--   situation varchar(32),
--   lastAction varchar(32),
--   shotType varchar(32),
--   shotResult varchar(32),
--   xGoal float4,
--   positionX float4,
--   positionY float4
 
-- );


DROP TABLE IF EXISTS players;

CREATE TABLE players (
  player_id INT PRIMARY KEY,
  first_name VARCHAR(64),
  last_name VARCHAR(64),
  name VARCHAR(128),
  last_season INT,
  current_club_id INT,
  player_code VARCHAR(64),
  country_of_birth VARCHAR(64),
  city_of_birth VARCHAR(64),
  country_of_citizenship VARCHAR(64),
  date_of_birth VARCHAR(60),
  sub_position VARCHAR(64),
  position VARCHAR(64),
  foot VARCHAR(16) NULL,
  height_in_cm INT NULL,
  contract_expiration_date VARCHAR(60) DEFAULT NULL,
  agent_name VARCHAR(128) DEFAULT NULL,
  image_url TEXT,
  url TEXT,
  current_club_domestic_competition_id VARCHAR(120),
  current_club_name VARCHAR(128),
  market_value_in_eur INT NULL,    -- Added NULL to allow null values
  highest_market_value_in_eur INT NULL    -- Added NULL to allow null values
);

DROP TABLE IF EXISTS transfers;

CREATE TABLE transfers (
  player_id INT,
  transfer_date DATE,
  transfer_season VARCHAR(10),
  from_club_id INT,
  to_club_id INT,
  from_club_name VARCHAR(128),
  to_club_name VARCHAR(128),
  transfer_fee DECIMAL(15, 3),
  market_value_in_eur DECIMAL(15, 3),
  player_name VARCHAR(128)
);

DROP TABLE IF EXISTS player_valuations;

CREATE TABLE player_valuations (
  player_id INT,
  date DATE,
  market_value_in_eur INT,
  current_club_id INT,
  player_club_domestic_competition_id VARCHAR(10)
);

DROP TABLE IF EXISTS games;

CREATE TABLE games (
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

DROP TABLE IF EXISTS game_lineups;

CREATE TABLE game_lineups (
  game_lineups_id VARCHAR(64) PRIMARY KEY,
  date DATE,
  game_id INT,
  player_id INT,
  club_id INT,
  player_name VARCHAR(128),
  type VARCHAR(20),
  position VARCHAR(64),
  number INT,
  team_captain BOOLEAN
);

DROP TABLE IF EXISTS games;
DROP TABLE IF EXISTS club_games;

CREATE TABLE club_games (
  game_id INT,
  club_id INT,
  own_goals INT,
  own_position INT DEFAULT NULL,
  own_manager_name VARCHAR(128) DEFAULT NULL,
  opponent_id INT,
  opponent_goals INT,
  opponent_position INT DEFAULT NULL,
  opponent_manager_name VARCHAR(128) DEFAULT NULL,
  hosting ENUM('Home', 'Away'),
  is_win BOOLEAN
);

DROP TABLE IF EXISTS clubs;

CREATE TABLE clubs (
  club_id INT PRIMARY KEY,
  club_code VARCHAR(64),
  name VARCHAR(128),
  domestic_competition_id VARCHAR(10),
  total_market_value VARCHAR(32), -- Value is empty in sample; can be modified to DECIMAL if format is known
  squad_size INT,
  average_age DECIMAL(4,2),
  foreigners_number INT,
  foreigners_percentage DECIMAL(5,2),
  national_team_players INT,
  stadium_name VARCHAR(128),
  stadium_seats INT,
  net_transfer_record VARCHAR(32), -- Can be parsed to numeric if you clean up formatting
  coach_name VARCHAR(128),
  last_season INT,
  filename VARCHAR(256),
  url TEXT
);


DROP TABLE IF EXISTS games;

CREATE TABLE games (
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

DROP TABLE IF EXISTS competitions;

CREATE TABLE competitions (
  competition_id VARCHAR(10) PRIMARY KEY,
  competition_code VARCHAR(64),
  name VARCHAR(128),
  sub_type VARCHAR(64),
  type VARCHAR(64),
  country_id INT,  -- May contain -1 for international competitions
  country_name VARCHAR(64),
  domestic_league_code VARCHAR(10),
  confederation VARCHAR(32),
  url TEXT,
  is_major_national_league enum ('true', 'false')
);

DROP TABLE IF EXISTS game_events;

CREATE TABLE game_events (
  game_event_id VARCHAR(32) PRIMARY KEY,
  date DATE,
  game_id INT,
  minute INT,
  type VARCHAR(32),
  club_id INT,
  player_id INT,
  description TEXT,
  player_in_id INT,
  player_assist_id INT
);

DROP TABLE IF EXISTS appearances;

CREATE TABLE appearances (
  appearance_id VARCHAR(32) PRIMARY KEY,
  game_id INT,
  player_id INT,
  player_club_id INT,
  player_current_club_id INT,
  date DATE,
  player_name VARCHAR(128),
  competition_id VARCHAR(10),
  yellow_cards INT,
  red_cards INT,
  goals INT,
  assists INT,
  minutes_played INT

);