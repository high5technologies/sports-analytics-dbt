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

WITH CTE_PENALTY_GOAL as (
    SELECT p.game_log_key, p.game_sk, p.team_sk, p.game_key, p.pb_h_a, p.game_date, p.period_number, p.event_type, p.penalty, p.game_event_time_seconds, p.game_pp_end
        , g.game_log_key as goal_game_log_key, g.game_event_time_seconds as goal_time_seconds
        , count(*) over (partition by p.game_log_key) as penalty_dup_cnt
        , row_number() over (partition by g.game_log_key order by p.game_event_time_seconds) as penalty_dup_num -- seems weird to partition by goal for penalty, but correct
        , count(*) over (partition by g.game_log_key) as goal_dup_cnt
        , row_number() over (partition by p.game_log_key order by g.game_event_time_seconds) as goal_dup_num
    FROM {{ ref('ahl__transform_game_log_penalty') }} p 
        left join {{ ref('ahl__transform_game_log_goal') }} g 
            on g.game_sk = p.game_sk 
            and g.pb_h_a = p.pb_h_a
            and g.game_event_time_seconds between p.game_event_time_seconds and p.game_pp_end
    --WHERE 
        --p.game_sk = 'a14ae381-a3c4-418d-b86a-1d8355250d25'
        --and p.pb_h_a = 'H' 
        --and  p.period_number = 2
        --p.game_sk = 'c959b5e9-2edb-48c2-aec9-c39c994da06c'
        --and p.pb_h_a = 'H'
        --and p.period_number = 2
)
SELECT game_log_key, game_sk, team_sk, game_key, pb_h_a, game_date, period_number, event_type, penalty
    , game_event_time_seconds
    , case 
        when goal_game_log_key is null then game_pp_end -- no goal scored, take original end time
        when penalty_dup_num = goal_dup_num then goal_time_seconds -- goal scored during pp, goal time is now end of pp
        else game_pp_end -- goal matched to different penalty, ignore goal and take original end time
    end as game_pp_end 
    , game_pp_end as orig_game_pp_end
    , goal_game_log_key, goal_time_seconds, penalty_dup_cnt, penalty_dup_num, goal_dup_cnt, goal_dup_num
FROM CTE_PENALTY_GOAL
WHERE 
    penalty_dup_cnt = 1 or (penalty_dup_cnt > 1 and penalty_dup_num = goal_dup_num)

