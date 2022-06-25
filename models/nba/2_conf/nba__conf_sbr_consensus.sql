{{ config(
    tags=["nba"]
) }}

SELECT  
    game_key
    ,game_date
    --,case when EXTRACT(MONTH FROM game_date) > case when EXTRACT(MONTH FROM game_date) = 2020 then 11 else 7 end then EXTRACT(year FROM game_date) + 1 else EXTRACT(year FROM game_date) end as season
    ,h_a
    ,team_abbr
    ,team_city
    ,team_nickname
    ,o_u
    ,odds_mtid
    ,period_type
    ,odds_type
    ,epoch_timestamp
    ,cast(wagers_perc as NUMERIC) as consensus
    ,cast(wagers_count as INT64) as consensus_wagers
    ,load_datetime
    --, row_number() over (partition by game_key,h_a,o_u,odds_mtid order by load_datetime desc) as dedup
FROM nba.raw_sbr_consensus
QUALIFY row_number() over (partition by game_key,h_a,o_u,odds_mtid order by load_datetime desc) = 1
