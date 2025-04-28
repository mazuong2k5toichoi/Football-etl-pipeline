

WITH transfers AS (
    SELECT *
    FROM {{ source('warehouse', 'transfer') }}
),

players AS (
    SELECT *
    FROM {{ source('warehouse', 'players') }}
)

SELECT 
    t.player_id,
    t.player_name,
    t.transfer_season,
    t.from_club_name,
    t.to_club_name,
    t.transfer_market_value_in_eur,
    p.current_club_name,
    p.date_of_birth,
    p.position,
    p.hometown,
    p.foot,
    p.height_in_cm,
    p.image_url,
    p.market_value_in_eur as current_market_value_in_eur,
    p.highest_market_value_in_eur,
    -- Add some additional calculated fields
    ROW_NUMBER() OVER (PARTITION BY t.player_id ORDER BY t.transfer_season) as transfer_order,
    LAG(t.transfer_market_value_in_eur) OVER (PARTITION BY t.player_id ORDER BY t.transfer_season) as previous_transfer_value,
    LEAD(t.transfer_market_value_in_eur) OVER (PARTITION BY t.player_id ORDER BY t.transfer_season) as next_transfer_value
FROM transfers t
LEFT JOIN players p 
    ON t.player_id = p.player_id
