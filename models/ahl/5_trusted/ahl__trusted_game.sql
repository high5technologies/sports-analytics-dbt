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
    , merge_update_columns = ['season','season_type','game_start_time','game_end_time','game_duration','game_duration_minutes','attendance','started_flag'
                                ,'complete_flag','ot_flag','so_flag','venue','game_bit','update_datetime']
) }}

SELECT
    GENERATE_UUID() as game_sk
    --md5(concat(t.team_id,'|',t.season)) as team_sk
    , game_id as unique_key
    , game_id
    , game_date
    , season
    , season_type 
    , game_start_time
    , game_end_time
    , game_duration
    , game_duration_minutes
    , attendance
    , started_flag
    , complete_flag
    , ot_flag
    , so_flag
    , venue
    , game_bit
    , CURRENT_DATETIME() as insert_datetime
    , CURRENT_DATETIME() as update_datetime
FROM {{ ref('ahl__conf_hockeytech_game') }}
{% if is_incremental() %}
  WHERE game_date >= (SELECT max(game_date) from {{ this }})
{% endif %}
