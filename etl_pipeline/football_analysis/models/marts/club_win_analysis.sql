WITH home_games AS (
    SELECT
        home_club_id AS club_id,
        home_club_name AS club_name,
        home_club_manager_name AS manager_name,
        season,
        CASE 
            WHEN home_club_goals > away_club_goals THEN 1
            ELSE 0
        END AS is_win,
        1 AS games_count
    FROM {{ source('warehouse', 'games') }}
    WHERE home_club_id IN ('281','418','11','131','31','583','631','27','148','985')
    -- AND season IN ('2023', '2024')
),

away_games AS (
    SELECT
        away_club_id AS club_id,
        away_club_name AS club_name,
        away_club_manager_name AS manager_name,
        season,
        CASE 
            WHEN away_club_goals > home_club_goals THEN 1
            ELSE 0
        END AS is_win,
        1 AS games_count
    FROM {{ source('warehouse', 'games') }}
    WHERE away_club_id IN ('281','418','11','131','31','583','631','27','148','985')
    -- AND season IN ('2023', '2024')

),

combined_games AS (
    SELECT * FROM home_games
    UNION ALL
    SELECT * FROM away_games
),

club_season_manager AS (
    SELECT DISTINCT
        club_id,
        club_name,
        season,
        manager_name
    FROM combined_games
),

win_stats AS (
    SELECT
        club_id,
        club_name,
        season,
        SUM(is_win) AS wins,
        SUM(games_count) AS total_games,
        ROUND((SUM(is_win)::numeric / SUM(games_count)) * 100, 2) AS win_rate
    FROM combined_games
    GROUP BY club_id, club_name, season
)

SELECT
    w.club_id,
    w.club_name,
    w.season,
    csm.manager_name,
    w.wins,
    w.total_games,
    w.win_rate
FROM win_stats w
JOIN club_season_manager csm ON 
    w.club_id = csm.club_id AND
    w.season = csm.season AND
    w.club_name = csm.club_name
ORDER BY w.season, w.wins DESC