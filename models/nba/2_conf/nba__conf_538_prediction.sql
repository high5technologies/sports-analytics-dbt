{{ config(
    tags=["nba"]
) }}

SELECT  
    fivethirtyeight_key
    ,cast(carm_elo1_post as NUMERIC) as home_carm_elo_post
    ,cast(carm_elo1_pre as NUMERIC) as home_carm_elo_pre
    ,cast(carm_elo2_post as NUMERIC) as away_carm_elo_post
    ,cast(carm_elo2_pre as NUMERIC) as away_carm_elo_pre
    ,cast(carm_elo_prob1 as NUMERIC) as home_carm_elo_prob_pre
    ,cast(carm_elo_prob2 as NUMERIC) as away_carm_elo_prob_pre
    ,date as game_date
    ,cast(elo1_post as NUMERIC) as home_elo_post
    ,cast(elo1_pre as NUMERIC) as home_elo_pre
    ,cast(elo2_post as NUMERIC) as away_elo_post
    ,cast(elo2_pre as NUMERIC) as away_elo_pre
    ,cast(elo_prob1 as NUMERIC) as home_elo_prob_pre
    ,cast(elo_prob2 as NUMERIC) as away_elo_prob_pre
    ,neutral
    ,playoff
    ,cast(raptor1_pre as NUMERIC) as home_raptor_pre
    ,cast(raptor2_pre as NUMERIC) as away_raptor_pre
    ,cast(raptor_prob1 as NUMERIC) as home_raptor_prob_pre
    ,cast(raptor_prob2 as NUMERIC) as away_raptor_prob_pre
    ,cast(score1 as INT64) as home_score
    ,cast(score2 as INT64) as away_score
    ,cast(season as INT64) as season
    ,team1 as home_team_abbr -- home
    ,team2 as away_team_abbr  -- away
    ,load_datetime
    --, row_number() over (partition by fivethirtyeight_key order by load_datetime desc) as dedup
FROM {{ source('nba_raw','raw_538_predictions') }}
QUALIFY row_number() over (partition by fivethirtyeight_key order by load_datetime desc) = 1
