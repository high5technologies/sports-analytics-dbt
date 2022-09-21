{{ config(
    tags=["nba"]
) }}

SELECT  
    fantasylabs_key
    ,game_date
    ,dfs_source
    ,cast(dfs_contest_id as INT64) as dfs_contest_id
    ,cast(fantasy_result_id as INT64) as fantasy_result_id
    ,cast(player_id as INT64) as player_id_fantasylabs
    ,upper(player_name) as player_name
    ,position
    ,team
    ,cast(salary as INT64) as salary
    ,cast(actual_points as numeric) as actual_points
    ,cast(ownership_average as numeric) as ownership_average
    ,cast(ownership_volatility as numeric) as ownership_volatility
    ,cast(gpp_grade as numeric) as gpp_grade
    ,load_datetime
FROM {{ source('nba_raw','raw_fantasylabs_ownership') }}
QUALIFY row_number() over (partition by fantasylabs_key order by load_datetime desc) = 1

