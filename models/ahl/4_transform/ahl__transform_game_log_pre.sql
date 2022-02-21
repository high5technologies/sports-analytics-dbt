{{ config(
    tags=["ahl"]
    , labels = {'project': 'sports_analytics', 'league':'ahl'}
    , materialized='table'
) }}

SELECT 
    gl.game_date
    , gl.event_order_number
    , gl.event_time_in_seconds
    , gl.game_event_time_seconds
    , gl.period_number
    , gl.game_log_key
    , gl.game_key
    , gl.event_type
    , gl.period, gl.time, gl.team_id
    , gl.player_id
    , gl.shot_type, gl.shot_quality, gl.shot_is_goal, gl.shooter_position, gl.x_location, gl.y_location, gl.goalie_id
    , gl.h_a 
    
    , gl.game_winning_goal_flag
    , gl.insurance_goal_flag
    , gl.empty_net_flag
    , gl.penalty_shot_flag
    , gl.primary_assist_player_id
    , gl.secondary_assist_player_id
    , gl.plus_player_id_1
    , gl.plus_player_id_2
    , gl.plus_player_id_3
    , gl.plus_player_id_4
    , gl.plus_player_id_5
    , gl.plus_player_id_6
    , gl.minus_player_id_1
    , gl.minus_player_id_2
    , gl.minus_player_id_3
    , gl.minus_player_id_4
    , gl.minus_player_id_5
    , gl.minus_player_id_6
    , gl.penalty, gl.penalty_is_power_play, gl.penalty_servered_by_player_id, gl.pim, gl.goalie_coming_in_id, gl.goalie_coming_out_id, gl.load_datetime		
    , gl.pis, gl.game_pp_end

    , case 
        when gl.power_play_flag = true then true
        when gl.power_play_flag = false then false
        -- when gl.event_type = 'PENALTY' then null
        when pbc.game_key is null then false
        when gl.h_a = 'H' and coalesce(pbc.home_penalty_box_count,0) < coalesce(pbc.away_penalty_box_count,0) then true
        when gl.h_a = 'A' and coalesce(pbc.away_penalty_box_count,0) < coalesce(pbc.home_penalty_box_count,0) then true
    end as power_play_flag
    , case 
        when gl.short_handed_flag = true then true
        when gl.short_handed_flag = false then false
        -- when gl.event_type = 'PENALTY' then null
        when pbc.game_key is null then false
        when gl.h_a = 'H' and coalesce(pbc.home_penalty_box_count,0) > coalesce(pbc.away_penalty_box_count,0) then true
        when gl.h_a = 'A' and coalesce(pbc.away_penalty_box_count,0) > coalesce(pbc.home_penalty_box_count,0) then true
    end as short_handed_flag
    , case 
        when pbc.game_key is null then 0
        when gl.h_a = 'A' then coalesce(pbc.home_penalty_box_count,0) - coalesce(pbc.away_penalty_box_count,0) 
        when gl.h_a = 'H' then coalesce(pbc.away_penalty_box_count,0) - coalesce(pbc.home_penalty_box_count,0) 
    end as ice_advantage
    , case
        when pbc.game_key is null then 'E'
        when coalesce(pbc.home_penalty_box_count,0) = 0 and coalesce(pbc.away_penalty_box_count,0) = 0 then 'E'
        when coalesce(pbc.home_penalty_box_count,0) = coalesce(pbc.away_penalty_box_count,0) then 
            concat('E',5- case when pbc.home_penalty_box_count > 2 then 2 else pbc.home_penalty_box_count end,'-', 5- case when pbc.away_penalty_box_count > 2 then 2 else pbc.away_penalty_box_count end)
        when gl.h_a = 'H' and coalesce(pbc.home_penalty_box_count,0) < coalesce(pbc.away_penalty_box_count,0) then 
            concat('PP',5- case when pbc.home_penalty_box_count > 2 then 2 else pbc.home_penalty_box_count end,'-', 5- case when pbc.away_penalty_box_count > 2 then 2 else pbc.away_penalty_box_count end)
        when gl.h_a = 'A' and coalesce(pbc.away_penalty_box_count,0) < coalesce(pbc.home_penalty_box_count,0) then 
            concat('PP',5- case when pbc.away_penalty_box_count > 2 then 2 else pbc.away_penalty_box_count end,'-', 5- case when pbc.home_penalty_box_count > 2 then 2 else pbc.home_penalty_box_count end)
        when gl.h_a = 'H' and coalesce(pbc.home_penalty_box_count,0) > coalesce(pbc.away_penalty_box_count,0) then 
            concat('SH',5- case when pbc.home_penalty_box_count > 2 then 2 else pbc.home_penalty_box_count end,'-', 5- case when pbc.away_penalty_box_count > 2 then 2 else pbc.away_penalty_box_count end)
        when gl.h_a = 'A' and coalesce(pbc.away_penalty_box_count,0) > coalesce(pbc.home_penalty_box_count,0) then 	
            concat('SH',5- case when pbc.away_penalty_box_count > 2 then 2 else pbc.away_penalty_box_count end,'-', 5- case when pbc.home_penalty_box_count > 2 then 2 else pbc.home_penalty_box_count end)
    end as ice_advantage_code
    -- , pbc.game_start_event_time_seconds, pbc.game_end_event_time_seconds, pbc.away_penalty_box_count, pbc.home_penalty_box_count
    -- , gl.game_event_time_seconds, gl.event_type, gl.h_a
FROM {{ ref('ahl__transform_game_log_base') }} gl
    left join {{ ref('ahl__transform_game_log_penalty_box_player_cnt') }} pbc
        on gl.game_key = pbc.game_key
        and gl.game_event_time_seconds > pbc.game_start_event_time_seconds 
        and gl.game_event_time_seconds <= pbc.game_end_event_time_seconds
WHERE gl.team_id != 0 and gl.player_id != 0 -- remove general team penalties (coach penalities)
