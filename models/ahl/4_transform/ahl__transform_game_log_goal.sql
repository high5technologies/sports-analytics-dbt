{{ config(
    tags=["ahl"]
    , labels = {'project': 'sports_analytics', 'league':'ahl'}
    , partition_by = {
      'field': 'game_date',
      'data_type': 'date',
      'granularity': 'day'
    }
    , materialized='table'
) }}
 
SELECT *, row_number() over (partition by game_sk, pb_h_a order by event_order_number) as goal_number 
FROM {{ ref('ahl__transform_game_log_goals_penalties_all') }} 
WHERE event_type = 'GOAL'
