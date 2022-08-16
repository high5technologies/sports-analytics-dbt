{{ config(
    tags=["nba"]
) }}

SELECT  
    game_team_period_score_key
    ,game_id
    ,team_id
    ,h_a
    ,game_date
    ,cast(period as INT64) as period
    ,period_type
    ,cast(score as INT64) as score
    ,load_datetime
    --, row_number() over (partition by game_team_period_score_key order by load_datetime desc) as dedup
FROM {{ source('nba_raw','raw_nbacom_game_team_period_score') }}
QUALIFY row_number() over (partition by game_team_period_score_key order by load_datetime desc) = 1