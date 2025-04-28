with home_games as (
    select
        season,
        name as competition_name,
        round,
        home_club_id as club_id,
        home_club_name as club_name,
        count(*) as home_games_played,
        sum(case when home_club_goals > away_club_goals then 1 else 0 end) as home_wins,
        sum(case when home_club_goals = away_club_goals then 1 else 0 end) as home_draws,
        sum(case when home_club_goals < away_club_goals then 1 else 0 end) as home_losses,
        sum(home_club_goals) as home_goals_for,
        sum(away_club_goals) as home_goals_against
    from {{ source('warehouse', 'games') }}
    where season between '2021' and '2024'
    group by season, name, round, home_club_id, home_club_name
),

away_games as (
    select
        season,
        name as competition_name,
        round,
        away_club_id as club_id,
        away_club_name as club_name,
        count(*) as away_games_played,
        sum(case when away_club_goals > home_club_goals then 1 else 0 end) as away_wins,
        sum(case when away_club_goals = home_club_goals then 1 else 0 end) as away_draws,
        sum(case when away_club_goals < home_club_goals then 1 else 0 end) as away_losses,
        sum(away_club_goals) as away_goals_for,
        sum(home_club_goals) as away_goals_against
    from {{ source('warehouse', 'games') }}
    where season between '2021' and '2024'
    group by season, name, round, away_club_id, away_club_name
),

combined as (
    select
        coalesce(h.season, a.season) as season,
        coalesce(h.competition_name, a.competition_name) as competition_name,
        coalesce(h.round, a.round) as round,
        coalesce(h.club_id, a.club_id) as club_id,
        coalesce(h.club_name, a.club_name) as club_name,
        coalesce(h.home_games_played, 0) as home_games,
        coalesce(a.away_games_played, 0) as away_games,
        coalesce(h.home_wins, 0) as home_wins,
        coalesce(a.away_wins, 0) as away_wins,
        coalesce(h.home_draws, 0) as home_draws,
        coalesce(a.away_draws, 0) as away_draws,
        coalesce(h.home_losses, 0) as home_losses,
        coalesce(a.away_losses, 0) as away_losses,
        coalesce(h.home_goals_for, 0) as home_goals_for,
        coalesce(a.away_goals_for, 0) as away_goals_for,
        coalesce(h.home_goals_against, 0) as home_goals_against,
        coalesce(a.away_goals_against, 0) as away_goals_against
    from home_games h
    full outer join away_games a
        on h.season = a.season
        and h.competition_name = a.competition_name
        and h.round = a.round
        and h.club_id = a.club_id
),

competition_type as (
    select
        season,
        name as competition_name,
        case 
            when count(distinct round) > 20 and max(round) like '%. Matchday' then 'league'
            when sum(case when round like 'Group%' then 1 else 0 end) > 0 then 'tournament'
            else 'cup'
        end as format
    from {{ source('warehouse', 'games') }}
    where season between '2021' and '2024'
    group by season, name
),

normalized_rounds as (
    select 
        c.*,
        case
            when ct.format = 'league' and c.round like '%. Matchday' 
                then cast(substring(c.round, 1, position('. ' in c.round) - 1) as integer)
            else null
        end as matchday_number,
        case
            when c.round like 'Group%' then c.round
            when ct.format = 'league' then 'League'
            else 'Knockout'
        end as stage
    from combined c
    join competition_type ct on c.season = ct.season and c.competition_name = ct.competition_name
),

grouped_stats as (
    select
        c.season,
        c.competition_name,
        ct.format,
        c.stage,
        c.club_id,
        c.club_name,
        sum(c.home_games + c.away_games) as games_played,
        sum(c.home_wins + c.away_wins) as wins,
        sum(c.home_draws + c.away_draws) as draws,
        sum(c.home_losses + c.away_losses) as losses,
        sum(c.home_goals_for + c.away_goals_for) as goals_for,
        sum(c.home_goals_against + c.away_goals_against) as goals_against,
        sum(c.home_goals_for + c.away_goals_for) - sum(c.home_goals_against + c.away_goals_against) as goal_difference,
        sum((c.home_wins + c.away_wins) * 3 + (c.home_draws + c.away_draws)) as points
    from normalized_rounds c
    join competition_type ct on c.season = ct.season and c.competition_name = ct.competition_name
    group by c.season, c.competition_name, ct.format, c.stage, c.club_id, c.club_name
)

select
    season,
    competition_name,
    format,
    stage,
    club_id,
    club_name,
    games_played,
    wins,
    draws,
    losses,
    goals_for,
    goals_against,
    goal_difference,
    points,
    -- Only calculate positions for league format or group stages in tournaments
    case 
        when format = 'league' or (format = 'tournament' and stage not like 'Knockout') 
        then row_number() over (
            partition by season, competition_name, stage 
            order by points desc, goal_difference desc, goals_for desc
        )
        else null
    end as position
from grouped_stats
order by season desc, competition_name, stage, 
    case when stage like 'Group%' then stage end,
    points desc, goal_difference desc