{{ config(
    tags=["nba"]
) }}

SELECT  
    swish_salary_key
    ,game_date
    ,salary_source as dfs_source
    ,player_id as player_id_swish
    ,player_name
    ,team_abbr
    ,pos_main
    ,case when pos_main not in ('PG','SG','SF','PF','C') then null else pos_main end as pos_main_adj
    ,cast(projected_fantasy_pts as numeric) as projected_fantasy_pts
    ,cast(nullif(REGEXP_REPLACE(avg_pts,r'[^0-9.]', ''),'') as numeric) as avg_pts
    ,cast(nullif(REGEXP_REPLACE(fpts_diff,r'[^0-9.]', ''),'') as numeric) as fpts_diff

    ,PARSE_DATE('%F', prev_game_date) as prev_game_date
    ,case when cast(replace(salary,',','') as INT64) < 1000 then null else cast(replace(salary,',','') as INT64) end as salary
    ,cast(replace(salary_diff,',','') as INT64) as salary_diff
    ,cast(salary_diff_percentage as numeric) as salary_diff_percentage
    ,load_datetime
FROM {{ source('nba_raw','raw_swish_salary') }}
QUALIFY row_number() over (partition by swish_salary_key order by load_datetime desc) = 1
