{{ config(
    tags=["nba"]
) }}


    SELECT  
        linestar_key
        ,game_date
        ,cast(pid as INT64) as pid
        ,case when dfs_source = 'DraftKings' then 'dk' when dfs_source = 'FanDuel' then 'fd' end as dfs_source
        ,cast(dfs_contest_id as INT64) as dfs_contest_id
        ,linestar_type
        ,cast(player_id as INT64) as player_id_linestar
        ,player_name
        ,cast(owned as numeric) as owned
        ,player_pos
        ,case when player_pos not in ('PG','SG','SF','PF','C') then null else player_pos end as player_pos_adj
        ,team
        ,cast(player_salary as INT64) as player_salary
        ,load_datetime
    FROM {{ source('nba_raw','raw_linestar_ownership') }}
    QUALIFY row_number() over (partition by linestar_key order by load_datetime desc) = 1
