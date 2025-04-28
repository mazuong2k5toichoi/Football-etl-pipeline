
WITH game_stats AS (
    SELECT
        g.season,
        g.competition_id,
        g.name,
        SUM(g.home_club_goals + g.away_club_goals) AS total_goals,
        AVG(g.attendance) AS avg_attendance,
        COUNT(*) AS total_games
    FROM {{ source('warehouse', 'games') }} g
    WHERE g.attendance IS NOT NULL
    GROUP BY g.season, g.competition_id, g.name
),

home_formations AS (
    SELECT
        season,
        competition_id,
        name,
        home_club_formation AS formation,
        COUNT(*) AS total_games,
        SUM(CASE WHEN home_club_goals > away_club_goals THEN 1 ELSE 0 END) AS wins
    FROM {{ source('warehouse', 'games') }}
    WHERE home_club_formation IS NOT NULL
    GROUP BY season, competition_id, name, home_club_formation
),

away_formations AS (
    SELECT
        season,
        competition_id,
        name,
        away_club_formation AS formation,
        COUNT(*) AS total_games,
        SUM(CASE WHEN away_club_goals > home_club_goals THEN 1 ELSE 0 END) AS wins
    FROM {{ source('warehouse', 'games') }}
    WHERE away_club_formation IS NOT NULL
    GROUP BY season, competition_id, name, away_club_formation
),

combined_formations AS (
    SELECT
        season,
        competition_id,
        name,
        formation,
        SUM(total_games) AS total_games,
        SUM(wins) AS total_wins,
        (SUM(wins) * 100.0 / SUM(total_games)) AS win_rate
    FROM (
        SELECT * FROM home_formations
        UNION ALL
        SELECT * FROM away_formations
    ) AS all_formations
    GROUP BY season, competition_id, name, formation
),

competition_match_counts AS (
    SELECT
        season,
        competition_id,
        name,
        COUNT(*) AS match_count
    FROM {{ source('warehouse', 'games') }}
    GROUP BY season, competition_id, name
),

ranked_formations AS (
    SELECT
        cf.season,
        cf.competition_id,
        cf.name,
        cf.formation,
        cf.win_rate,
        cf.total_games,
        cmc.match_count,
        ROW_NUMBER() OVER (PARTITION BY cf.season, cf.competition_id, cf.name ORDER BY cf.win_rate DESC) as rank
    FROM combined_formations cf
    JOIN competition_match_counts cmc
        ON cf.season = cmc.season
        AND cf.competition_id = cmc.competition_id
        AND cf.name = cmc.name
    WHERE 
        -- For competitions with many matches, require sufficient sample size
        (cmc.match_count >= 20 AND cf.total_games >= 10)
        OR
        -- For competitions with few matches, be more lenient
        (cmc.match_count < 20)
)

SELECT
    gs.season,
    gs.name,
    gs.total_goals,
    gs.avg_attendance,
    rf.formation AS best_formation,
    rf.win_rate AS best_formation_win_rate
FROM game_stats gs
LEFT JOIN ranked_formations rf
    ON gs.season = rf.season
    AND gs.competition_id = rf.competition_id
    AND gs.name = rf.name
    AND rf.rank = 1
ORDER BY gs.season DESC, gs.name