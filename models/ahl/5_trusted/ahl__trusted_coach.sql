{{ config(
    tags=["ahl"]
    , labels = {'project': 'sports_analytics', 'league':'ahl'}
    , materialized='incremental'    
    , unique_key='unique_key'
    , merge_update_columns = ['coach_primary_role','update_datetime']
) }}

SELECT 
    GENERATE_UUID() as coach_sk
    , coach_name as unique_key
    , coach_name
    , coach_first_name
    , coach_last_name
    , role as coach_primary_role
    , CURRENT_DATETIME() as insert_datetime
FROM
    (SELECT coach_first_name, coach_last_name, role, coach_name
        , row_number() over (partition by coach_name order by role_count desc, last_coached desc) as dedup_id
    FROM
        (SELECT distinct coach_first_name, coach_last_name, role, coach_name
            , count(role) over (partition by coach_name, role) as role_count 
            , max(game_date) over (partition by coach_name, role) as last_coached
        FROM {{ ref('ahl__conf_hockeytech_coach') }}
        ) a
    ) c
WHERE dedup_id = 1 -- dedup to single "primary position" per player_id
