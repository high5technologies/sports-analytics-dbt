{{ config(
    tags=["nba"]
    , labels = {'project': 'sports_analytics', 'league':'nba'}
    , materialized='incremental'    
    , unique_key='unique_key'
    , merge_update_columns = ['arena_name','arena_city','arena_state','arena_country','arena_timezone','arena_street_address','arena_postal_code'
                                ,'update_datetime']
) }}
    
SELECT 
    GENERATE_UUID() as arena_sk
    , arena_id_nbacom as unique_key
    , a.*
    , CURRENT_DATETIME() as insert_datetime
    , CURRENT_DATETIME() as update_datetime
FROM
    (SELECT distinct
        arena_id as arena_id_nbacom
        , arena_name
        , arena_city
        , arena_state
        , arena_country
        , arena_timezone
        , arena_street_address
        , arena_postal_code
    FROM {{ ref('nba__conf_nbacom_game') }}
    ) a
