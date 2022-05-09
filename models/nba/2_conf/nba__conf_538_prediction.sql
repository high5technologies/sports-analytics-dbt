{{ config(
    tags=["nba"]
) }}

SELECT * 
FROM 
    (SELECT  
        fivethirtyeight_key
        ,cast(carm_elo1_post as NUMERIC) as carm_elo1_post
        ,cast(carm_elo1_pre as NUMERIC) as carm_elo1_pre
        ,cast(carm_elo2_post as NUMERIC) as carm_elo2_post
        ,cast(carm_elo2_pre as NUMERIC) as carm_elo2_pre
        ,cast(carm_elo_prob1 as NUMERIC) as carm_elo_prob1
        ,cast(carm_elo_prob2 as NUMERIC) as carm_elo_prob2
        ,date as game_date
        ,cast(elo1_post as NUMERIC) as elo1_post
        ,cast(elo1_pre as NUMERIC) as elo1_pre
        ,cast(elo2_post as NUMERIC) as elo2_post
        ,cast(elo2_pre as NUMERIC) as elo2_pre
        ,cast(elo_prob1 as NUMERIC) as elo_prob1
        ,cast(elo_prob2 as NUMERIC) as elo_prob2
        ,neutral
        ,playoff
        ,cast(raptor1_pre as NUMERIC) as raptor1_pre
        ,cast(raptor2_pre as NUMERIC) as raptor2_pre
        ,cast(raptor_prob1 as NUMERIC) as raptor_prob1
        ,cast(raptor_prob2 as NUMERIC) as raptor_prob2
        ,cast(score1 as INT64) as score1
        ,cast(score2 as INT64) as score2
        ,cast(season as INT64) as season
        ,team1
        ,team2
        ,load_datetime
        , row_number() over (partition by fivethirtyeight_key order by load_datetime desc) as dedup
    FROM {{ source('nba_raw','raw_538_predictions') }}
    ) a
WHERE dedup = 1



