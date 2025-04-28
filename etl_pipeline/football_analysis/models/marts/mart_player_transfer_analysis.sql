-- models/marts/mart_player_transfer_analysis.sql
WITH player_transfers AS (
    SELECT *
    FROM {{ ref('players_model') }}
),

transfer_metrics AS (
    SELECT 
        player_id,
        player_name,
        COUNT(*) as total_transfers,
        MIN(transfer_market_value_in_eur) as min_transfer_value,
        MAX(transfer_market_value_in_eur) as max_transfer_value,
        AVG(transfer_market_value_in_eur) as avg_transfer_value,
        MIN(transfer_season) as first_transfer_season,
        MAX(transfer_season) as last_transfer_season
    FROM player_transfers
    GROUP BY player_id, player_name
)

SELECT 
    pt.*,
    tm.total_transfers,
    tm.min_transfer_value,
    tm.max_transfer_value,
    tm.avg_transfer_value,
    tm.first_transfer_season,
    tm.last_transfer_season,
    -- Calculate value change
    CASE 
        WHEN previous_transfer_value IS NOT NULL 
        THEN transfer_market_value_in_eur - previous_transfer_value 
        ELSE NULL 
    END as value_change,
    -- Calculate value change percentage
    CASE 
        WHEN previous_transfer_value IS NOT NULL AND previous_transfer_value != 0
        THEN ((transfer_market_value_in_eur - previous_transfer_value) / previous_transfer_value) * 100
        ELSE NULL 
    END as value_change_percentage
FROM player_transfers pt
LEFT JOIN transfer_metrics tm 
    ON pt.player_id = tm.player_id