{{ config(
    tags=["ahl"]
    , labels = {'project': 'sports_analytics', 'league':'ahl'}
    , partition_by = {
      'field': 'game_date',
      'data_type': 'date',
      'granularity': 'day'
    }
    , materialized='incremental'    
    , unique_key='unique_key'
    , merge_update_columns = ['role','referee_flag','linesman_flag','update_datetime']
) }}

SELECT 
    GENERATE_UUID() as game_ref_sk
    , g.game_sk || '|' || r.ref_sk as unique_key
    , g.game_sk
    , r.ref_sk 
    , g.game_date
    , rr.role
    , rr.referee_flag
    , rr.linesman_flag
    , CURRENT_DATETIME() as insert_datetime
FROM {{ ref('ahl__conf_hockeytech_ref') }} rr 
    inner join {{ ref('ahl__trusted_game') }} g 
        on rr.game_key = g.game_id 
    inner join {{ ref('ahl__trusted_ref') }} r
        on rr.ref_name = r.ref_name
    
    