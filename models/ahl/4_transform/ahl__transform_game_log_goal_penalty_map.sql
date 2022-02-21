-- THIS IS NOT USED

    /*SELECT goal_game_log_key, pb_h_a, goal_number, game_event_time_seconds, penalty_game_log_key, penalty_number
    FROM 
        (*/
        SELECT g.game_log_key as goal_game_log_key, g.pb_h_a, g.goal_number, g.game_event_time_seconds
            , p.game_log_key as penalty_game_log_key, p.penalty_number
            , case when p.penalty_number is not null then 1 else 0 end as match_flag
            , row_number() over (partition by g.game_sk, g.pb_h_a order by penalty_number asc) as dedup_id
        FROM {{ ref('ahl__transform_game_log_goal') }} g
            left join {{ ref('ahl__transform_game_log_penalty') }} p
                on g.game_sk = p.game_sk
                and g.pb_h_a = p.pb_h_a
                and g.game_event_time_seconds between p.game_event_time_seconds and p.game_pp_end
        WHERE g.goal_number = 1 --var_goal_number
    /*        and not exists
                (SELECT 1 FROM TMP_PENALTIES_USED m WHERE p.game_log_key = m.penalty_game_log_key)
        ) a
    WHERE dedup_id = 1
    */