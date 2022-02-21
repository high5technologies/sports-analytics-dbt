{{ config(
    tags=["ahl"]
    , labels = {'project': 'sports_analytics', 'league':'ahl'}
    , materialized='incremental'    
    , unique_key='unique_key'
    , merge_update_columns = ['ref_primary_role','jersey_number','update_datetime']
) }}

SELECT 
    GENERATE_UUID() as ref_sk
    , ref.ref_name as unique_key
    , ref.ref_name
    , ref.ref_first_name
    , ref.ref_last_name
    , role.role as ref_primary_role
    , ref.jersey_number
    , CURRENT_DATETIME() as insert_datetime
    , CURRENT_DATETIME() as update_datetime
FROM 
    (SELECT ref_first_name, ref_last_name, jersey_number, ref_name
    FROM
        (SELECT ref_first_name, ref_last_name, jersey_number, ref_name
            , row_number() over (partition by ref_name order by last_refed desc) as dedup_id
        FROM
            (SELECT distinct ref_first_name, ref_last_name, jersey_number, ref_name
                , max(game_date) over (partition by ref_name) as last_refed
            FROM {{ ref('ahl__conf_hockeytech_ref') }}
            ) a
        ) b
    WHERE b.dedup_id = 1 -- dedup based on last played to get most recent name/birthdate/jersey number
    ) ref
    inner join 
        (SELECT ref_first_name, ref_last_name, role, ref_name
        FROM
            (SELECT ref_first_name, ref_last_name, role, ref_name
                , row_number() over (partition by ref_name order by role_count desc, last_refed desc) as dedup_id
            FROM
                (SELECT distinct ref_first_name, ref_last_name, role, ref_name
                    , count(role) over (partition by ref_name, role) as role_count 
                    , max(game_date) over (partition by ref_name, role) as last_refed
                FROM {{ ref('ahl__conf_hockeytech_ref') }}
                ) a
            ) b
        WHERE b.dedup_id = 1 -- dedup to single "primary position" per player_id
        ) role
        on ref.ref_name = role.ref_name
