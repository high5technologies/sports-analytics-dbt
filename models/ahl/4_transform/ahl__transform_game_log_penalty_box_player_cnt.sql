{{ config(
    tags=["ahl"]
    , labels = {'project': 'sports_analytics', 'league':'ahl'}
    , materialized='table'
) }}

SELECT r.game_sk, r.game_key, r.game_start_event_time_seconds, r.game_end_event_time_seconds
    , SUM(case when p.pb_h_a = 'A' then 1 else 0 end) as away_penalty_box_count
    , SUM(case when p.pb_h_a = 'H' then 1 else 0 end) as home_penalty_box_count
FROM {{ ref('ahl__transform_game_log_penalty_range') }} r 
    left join {{ ref('ahl__transform_game_log_penalty_adj') }} p 
        on r.game_key = p.game_key
        and (
            (p.game_event_time_seconds >= r.game_start_event_time_seconds and p.game_event_time_seconds < r.game_end_event_time_seconds)
            or (p.game_event_time_seconds < r.game_start_event_time_seconds and p.game_pp_end > r.game_start_event_time_seconds)
            )
GROUP BY  r.game_sk, r.game_key, r.game_start_event_time_seconds, r.game_end_event_time_seconds
-- ORDER BY r.game_sk, r.game_start_event_time_seconds

