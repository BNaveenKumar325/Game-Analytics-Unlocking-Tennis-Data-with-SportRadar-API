
-- schema_and_queries.sql
-- SportRadar Event Explorer: SQL schema and required analysis queries
-- DB: MySQL (ANSI SQL); adjust types if using PostgreSQL (e.g., SERIAL -> AUTO_INCREMENT replacement).

-- ========== TABLE CREATION ==========
-- 1) Categories
CREATE TABLE IF NOT EXISTS Categories (
    category_id VARCHAR(50) PRIMARY KEY,
    category_name VARCHAR(100) NOT NULL
) ENGINE=InnoDB;

-- 2) Competitions
CREATE TABLE IF NOT EXISTS Competitions (
    competition_id VARCHAR(50) PRIMARY KEY,
    competition_name VARCHAR(100) NOT NULL,
    parent_id VARCHAR(50) NULL,
    type VARCHAR(20) NOT NULL,
    gender VARCHAR(10) NOT NULL,
    category_id VARCHAR(50),
    FOREIGN KEY (category_id) REFERENCES Categories(category_id),
    FOREIGN KEY (parent_id) REFERENCES Competitions(competition_id)
) ENGINE=InnoDB;

-- 3) Complexes
CREATE TABLE IF NOT EXISTS Complexes (
    complex_id VARCHAR(50) PRIMARY KEY,
    complex_name VARCHAR(100) NOT NULL
) ENGINE=InnoDB;

-- 4) Venues
CREATE TABLE IF NOT EXISTS Venues (
    venue_id VARCHAR(50) PRIMARY KEY,
    venue_name VARCHAR(100) NOT NULL,
    city_name VARCHAR(100) NOT NULL,
    country_name VARCHAR(100) NOT NULL,
    country_code CHAR(3) NOT NULL,
    timezone VARCHAR(100) NOT NULL,
    complex_id VARCHAR(50),
    FOREIGN KEY (complex_id) REFERENCES Complexes(complex_id)
) ENGINE=InnoDB;

-- 5) Competitors
CREATE TABLE IF NOT EXISTS Competitors (
    competitor_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    country VARCHAR(100) NOT NULL,
    country_code CHAR(3) NOT NULL,
    abbreviation VARCHAR(10) NOT NULL
) ENGINE=InnoDB;

-- 6) Competitor_Rankings
CREATE TABLE IF NOT EXISTS Competitor_Rankings (
    rank_id INT AUTO_INCREMENT PRIMARY KEY,
    rank_pos INT NOT NULL,
    movement INT NOT NULL,
    points INT NOT NULL,
    competitions_played INT NOT NULL,
    competitor_id VARCHAR(50),
    ranking_date DATE NULL,
    FOREIGN KEY (competitor_id) REFERENCES Competitors(competitor_id)
) ENGINE=InnoDB;

-- Indexes to improve query performance
CREATE INDEX idx_competitions_category ON Competitions(category_id);
CREATE INDEX idx_venues_country ON Venues(country_code);
CREATE INDEX idx_rankings_competitor ON Competitor_Rankings(competitor_id);

-- ========== REQUIRED ANALYSIS QUERIES ==========

-- 1) List all competitions along with their category name
SELECT c.competition_id, c.competition_name, c.type, c.gender, cat.category_name
FROM Competitions c
LEFT JOIN Categories cat ON c.category_id = cat.category_id;

-- 2) Count the number of competitions in each category
SELECT cat.category_name, COUNT(*) AS competition_count
FROM Competitions c
LEFT JOIN Categories cat ON c.category_id = cat.category_id
GROUP BY cat.category_name;

-- 3) Find all competitions of type 'doubles'
SELECT * FROM Competitions WHERE LOWER(type) = 'doubles';

-- 4) Get competitions that belong to a specific category (example: 'ITF Men')
-- Replace 'ITF Men' with exact category_name or use category_id if preferred
SELECT c.* FROM Competitions c
JOIN Categories cat ON c.category_id = cat.category_id
WHERE cat.category_name = 'ITF Men';

-- 5) Identify parent competitions and their sub-competitions
SELECT parent.competition_id AS parent_id, parent.competition_name AS parent_name,
       child.competition_id AS child_id, child.competition_name AS child_name
FROM Competitions child
JOIN Competitions parent ON child.parent_id = parent.competition_id
ORDER BY parent.competition_name, child.competition_name;

-- 6) Analyze the distribution of competition types by category
SELECT cat.category_name, c.type, COUNT(*) AS count_by_type
FROM Competitions c
LEFT JOIN Categories cat ON c.category_id = cat.category_id
GROUP BY cat.category_name, c.type
ORDER BY cat.category_name, count_by_type DESC;

-- 7) List all competitions with no parent (top-level competitions)
SELECT * FROM Competitions WHERE parent_id IS NULL;

-- ================= Complex & Venues Queries =================

-- 1) List all venues along with their associated complex name
SELECT v.venue_id, v.venue_name, v.city_name, v.country_name, comp.complex_name
FROM Venues v
LEFT JOIN Complexes comp ON v.complex_id = comp.complex_id;

-- 2) Count the number of venues in each complex
SELECT comp.complex_name, COUNT(*) AS venue_count
FROM Venues v
LEFT JOIN Complexes comp ON v.complex_id = comp.complex_id
GROUP BY comp.complex_name;

-- 3) Get details of venues in a specific country (e.g., Chile)
SELECT * FROM Venues WHERE country_name = 'Chile' OR country_code = 'CHL';

-- 4) Identify all venues and their timezones
SELECT venue_id, venue_name, city_name, country_name, timezone FROM Venues;

-- 5) Find complexes that have more than one venue
SELECT comp.complex_name, COUNT(v.venue_id) AS venues_count
FROM Complexes comp
JOIN Venues v ON comp.complex_id = v.complex_id
GROUP BY comp.complex_name HAVING COUNT(v.venue_id) > 1;

-- 6) List venues grouped by country
SELECT country_name, COUNT(*) AS venues_in_country
FROM Venues GROUP BY country_name ORDER BY venues_in_country DESC;

-- 7) Find all venues for a specific complex (e.g., 'Nacional')
SELECT v.* FROM Venues v
JOIN Complexes comp ON v.complex_id = comp.complex_id
WHERE comp.complex_name = 'Nacional';

-- ================= Competitor Rankings Queries =================

-- 1) Get all competitors with their rank and points.
SELECT comp.competitor_id, comp.name, r.rank_pos, r.points, r.movement, r.competitions_played
FROM Competitors comp
LEFT JOIN Competitor_Rankings r ON comp.competitor_id = r.competitor_id
ORDER BY r.rank_pos;

-- 2) Find competitors ranked in the top 5
SELECT comp.competitor_id, comp.name, r.rank_pos, r.points
FROM Competitors comp
JOIN Competitor_Rankings r ON comp.competitor_id = r.competitor_id
WHERE r.rank_pos <= 5
ORDER BY r.rank_pos;

-- 3) List competitors with no rank movement (stable rank)
SELECT comp.competitor_id, comp.name, r.rank_pos, r.movement
FROM Competitors comp
JOIN Competitor_Rankings r ON comp.competitor_id = r.competitor_id
WHERE r.movement = 0;

-- 4) Get the total points of competitors from a specific country (e.g., Croatia)
SELECT SUM(r.points) AS total_points, comp.country, comp.country_code
FROM Competitors comp
JOIN Competitor_Rankings r ON comp.competitor_id = r.competitor_id
WHERE comp.country = 'Croatia' OR comp.country_code = 'HRV'
GROUP BY comp.country, comp.country_code;

-- 5) Count the number of competitors per country
SELECT comp.country, COUNT(*) AS competitor_count
FROM Competitors comp GROUP BY comp.country ORDER BY competitor_count DESC;

-- 6) Find competitors with the highest points in the current week
-- (Assumes ranking_date is populated; replace CURDATE() with the appropriate ranking date filter)
SELECT comp.competitor_id, comp.name, r.points, r.ranking_date
FROM Competitors comp
JOIN Competitor_Rankings r ON comp.competitor_id = r.competitor_id
WHERE r.ranking_date = (SELECT MAX(r2.ranking_date) FROM Competitor_Rankings r2)
ORDER BY r.points DESC LIMIT 10;

-- ========== END OF FILE ==========
