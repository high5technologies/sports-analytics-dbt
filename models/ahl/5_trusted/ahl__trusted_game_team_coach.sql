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
    , merge_update_columns = ['role','head_coach_flag','assistant_coach_flag','update_datetime']
) }}

SELECT 
    GENERATE_UUID() as game_team_coach_sk
    , gt.game_team_sk || '|' || c.coach_sk as unique_key
    , gt.game_team_sk
    , c.coach_sk 
    , gt.game_date
    , rg.role
    , rg.head_coach_flag
    , rg.assistant_coach_flag
    , CURRENT_DATETIME() as insert_datetime
    , CURRENT_DATETIME() as update_datetime
FROM {{ ref('ahl__conf_hockeytech_coach') }} rg 
    inner join {{ ref('ahl__trusted_game') }} g 
        on rg.game_key = g.game_id 
    inner join {{ ref('ahl__trusted_game_team') }} gt 	
        on g.game_sk = gt.game_sk
        and rg.h_a = gt.h_a
    inner join {{ ref('ahl__trusted_coach') }} c 
        on rg.coach_name = c.coach_name

  