-- Initialize the database. Create "flights" table.
BEGIN;

DROP TABLE IF EXISTS vg_sales,vg_critics,vg_critic_sales;

CREATE TABLE vg_sales (
  name varchar(255) NOT NULL,
  critic_score decimal NOT NULL,
  user_score decimal NOT NULL,
  total_shipped decimal NOT NULL,
  year integer NOT NULL,
  formatted_name varchar(255) NOT NULL
);

CREATE TABLE vg_critics (
  name varchar(255) NOT NULL,
  critic_positive integer NOT NULL,
  critic_neutral integer NOT NULL,
  critic_negative integer NOT NULL,
  metascore integer NOT NULL,
  user_positive integer NOT NULL,
  user_neutral integer NOT NULL,
  user_negative integer NOT NULL,
  metacritic_userscore decimal NOT NULL,
  metacritic_formatted_name varchar(255) NOT NULL,
  isPCPlatform bit NOT NULL,
  isX360Platform bit NOT NULL,
  isOtherPlatform bit NOT NULL,
  isERating bit NOT NULL,
  isTRating bit NOT NULL,
  isMRating bit NOT NULL,
  isOtherRating bit NOT NULL,
  isActionGenre bit NOT NULL,
  isActionAdventureGenre bit NOT NULL,
  isOtherGenre bit NOT NULL,
  isOnePlayerOnly bit NOT NULL,
  hasOnlineMultiplayer bit NOT NULL
);

CREATE TABLE vg_critic_sales (
  critic_score decimal NOT NULL,
  user_score decimal NOT NULL,
  total_shipped decimal NOT NULL,
  year integer NOT NULL,
  formatted_name varchar(255) NOT NULL,
  critic_positive integer NOT NULL,
  critic_neutral integer NOT NULL,
  critic_negative integer NOT NULL,
  metascore integer NOT NULL,
  user_positive integer NOT NULL,
  user_neutral integer NOT NULL,
  user_negative integer NOT NULL,
  metacritic_userscore decimal NOT NULL,
  isPCPlatform bit NOT NULL,
  isX360Platform bit NOT NULL,
  isOtherPlatform bit NOT NULL,
  isERating bit NOT NULL,
  isTRating bit NOT NULL,
  isMRating bit NOT NULL,
  isOtherRating bit NOT NULL,
  isActionGenre bit NOT NULL,
  isActionAdventureGenre bit NOT NULL,
  isOtherGenre bit NOT NULL,
  isOnePlayerOnly bit NOT NULL,
  hasOnlineMultiplayer bit NOT NULL
);

SELECT * FROM vg_critics LIMIT 10;
SELECT * FROM vg_sales LIMIT 10;

COMMIT;