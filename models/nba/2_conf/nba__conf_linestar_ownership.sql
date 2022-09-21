{{ config(
    tags=["nba"]
) }}

SELECT  
    linestar_key
    ,game_date
    ,cast(pid as INT64) as pid
    ,dfs_source
    ,cast(dfs_contest_id as INT64) as dfs_contest_id
    ,linestar_type
    ,cast(player_id as INT64) as player_id_linestar
    ,player_name
    ,cast(owned as numeric) as owned
    ,player_pos
    ,team
    ,cast(player_salary as INT64) as player_salary
    ,load_datetime
FROM {{ source('nba_raw','raw_linestar_ownership') }}
QUALIFY row_number() over (partition by linestar_key order by load_datetime desc) = 1
