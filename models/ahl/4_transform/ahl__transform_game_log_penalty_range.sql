{{ config(
    tags=["ahl"]
    , labels = {'project': 'sports_analytics', 'league':'ahl'}
    , materialized='table'
) }}

SELECT a.*
    , coalesce(lead(game_start_event_time_seconds) OVER (partition by game_sk ORDER BY game_start_event_time_seconds) , 3600) as game_end_event_time_seconds
    , row_number() OVER (partition by game_sk ORDER BY game_start_event_time_seconds) as game_time_range_number
FROM
    (SELECT game_sk, game_key, 'PENALTY BEGIN' as event_type, game_event_time_seconds as game_start_event_time_seconds
    FROM {{ ref('ahl__transform_game_log_penalty_adj') }}
    UNION DISTINCT
    SELECT game_sk, game_key, 'PENALTY END' as event_type, game_pp_end as game_start_event_time_seconds
    FROM {{ ref('ahl__transform_game_log_penalty_adj') }}
    ) a
