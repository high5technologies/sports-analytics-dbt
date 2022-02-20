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
    --, materialized='incremental'    
    --, unique_key='unique_key'
    --, merge_update_columns = ['position','shots_against','saves','goals_against','assists','goals','points','pim','mvp_flag','captain_status'
    --                          ,'starting_flag','time_on_ice','time_on_ice_seconds','update_datetime']
SELECT 
    gl.game_date
    , gl.event_order_number
    , gl.event_time_in_seconds
    , gl.game_event_time_seconds
    , gl.period_number
    , gl.game_log_key, gl.game_key
    , case when goal.game_log_key is not null then 'GOAL' else upper(gl.event_type) end as event_type
    , gl.period, gl.time, gl.team_id
    , gl.player_id as player_id
    , gl.shot_type, gl.shot_quality, gl.shot_is_goal, gl.shooter_position, gl.x_location, gl.y_location, gl.goalie_id
    , case when gl.team_id = rg.away_team_id then 'A' when gl.team_id = rg.home_team_id then 'H' end as h_a 
    , coalesce(goal.power_play_flag, gl.power_play_flag) as power_play_flag
    , coalesce(goal.short_handed_flag, gl.short_handed_flag) as short_handed_flag
    , coalesce(goal.game_winning_goal_flag, gl.game_winning_goal_flag) as game_winning_goal_flag
    , coalesce(goal.insurance_goal_flag, gl.insurance_goal_flag) as insurance_goal_flag
    , coalesce(goal.empty_net_flag, gl.empty_net_flag) as empty_net_flag
    , coalesce(goal.penalty_shot_flag, gl.penalty_shot_flag) as penalty_shot_flag
    , coalesce(goal.primary_assist_player_id, gl.primary_assist_player_id) as primary_assist_player_id
    , coalesce(goal.secondary_assist_player_id, gl.secondary_assist_player_id) as secondary_assist_player_id
    , coalesce(goal.plus_player_id_1, gl.plus_player_id_1) as plus_player_id_1
    , coalesce(goal.plus_player_id_2, gl.plus_player_id_2) as plus_player_id_2
    , coalesce(goal.plus_player_id_3, gl.plus_player_id_3) as plus_player_id_3
    , coalesce(goal.plus_player_id_4, gl.plus_player_id_4) as plus_player_id_4
    , coalesce(goal.plus_player_id_5, gl.plus_player_id_5) as plus_player_id_5
    , coalesce(goal.plus_player_id_6, gl.plus_player_id_6) as plus_player_id_6
    , coalesce(goal.minus_player_id_1, gl.minus_player_id_1) as minus_player_id_1
    , coalesce(goal.minus_player_id_2, gl.minus_player_id_2) as minus_player_id_2
    , coalesce(goal.minus_player_id_3, gl.minus_player_id_3) as minus_player_id_3
    , coalesce(goal.minus_player_id_4, gl.minus_player_id_4) as minus_player_id_4
    , coalesce(goal.minus_player_id_5, gl.minus_player_id_5) as minus_player_id_5
    , coalesce(goal.minus_player_id_6, gl.minus_player_id_6) as minus_player_id_6
    
    , gl.penalty, gl.penalty_is_power_play, gl.penalty_servered_by_player_id, gl.pim, gl.goalie_coming_in_id, gl.goalie_coming_out_id, gl.load_datetime		
    , gl.pis, gl.game_pp_end
FROM {{ ref('ahl__conf_hockeytech_gamelog') }} gl 
    inner join {{ ref('ahl__conf_hockeytech_game') }} rg
        on gl.game_key = rg.game_key
    inner join {{ ref('ahl__trusted_game') }} g 
        on gl.game_key = g.game_id 
    left join {{ ref('ahl__conf_hockeytech_gamelog') }} goal 
        on gl.game_key = goal.game_key
        and gl.period = goal.period
        and gl.time = goal.time
        and goal.event_type = 'GOAL'
        and gl.shot_is_goal = true
WHERE gl.event_type != 'GOAL' 
			