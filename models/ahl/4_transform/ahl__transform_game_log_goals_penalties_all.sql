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

SELECT gl.game_log_key, gl.game_key, g.game_sk, gl.game_date, gt.team_sk, gl.period_number, gl.event_type, gl.penalty
    , case when gl.event_type = 'GOAL' and gt.h_a = 'A' then 'H' when gl.event_type = 'GOAL' and gt.h_a = 'H' then 'A' else gt.h_a end as pb_h_a -- flip goals to affect the penalty box of the opposite team
    , gl.event_order_number
    , gl.pis
    , gl.event_time_in_seconds
    , gl.game_event_time_seconds
    , gl.game_pp_end
FROM {{ ref('ahl__transform_game_log_base') }} gl 
    inner join {{ ref('ahl__trusted_game') }} g 
        on g.game_id = gl.game_key
    inner join {{ ref('ahl__trusted_game_team') }} gt
        on g.game_sk = gt.game_sk
        and gl.h_a = gt.h_a
WHERE 
    (	
        (gl.event_type = 'PENALTY' and gl.pim <= 5)  --  need to use PIM <= 5 to determine whether player was in box.  The "penalty_is_power_play" flag in API is false for offsetting penalties ... penalty_is_power_play = 'True'
        or (gl.event_type = 'GOAL' and shot_is_goal = true)
    )
		