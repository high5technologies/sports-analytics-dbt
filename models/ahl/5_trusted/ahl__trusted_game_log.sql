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
    , merge_update_columns = ['event_group_id','event_order_number','shot','shot_type','shot_quality','shot_location_top','shot_location_left'
                             ,'shot_location_top_adj','shot_location_left_adj','goal','assist','assist_primary','assist_secondary'
                             ,'save','goal_against','pim','penalty','plus_minus','power_play_flag','short_handed_flag','game_winner_flag'
                             ,'insurance_goal_flag','empty_net_flag','ice_advantage','ice_advantage_code','penalty_shot_flag'
                             ,'seconds_since_team_last_same_event','player_game_event_count','player_season_event_count'
                             ,'team_pre_event_score','opp_pre_event_score','team_pre_event_score_diff','update_datetime']
) }}

WITH cte_base as (
    SELECT 
        --GENERATE_UUID() as game_log_sk
        game_log_sk
        , game_sk || '|' || team_sk || '|' || player_sk || '|' || event_type || '|' || game_event_time_seconds || '|' || event_dup_order_id as unique_key
        , game_sk, team_sk, player_sk, game_date, period, period_number, event_type, event_group_id, event_order_number
        , event_time, event_time_in_seconds, game_event_time_seconds, event_dup_order_id, shot, shot_type, shot_quality
        , shot_location_top, shot_location_left, shot_location_top_adj, shot_location_left_adj 
        , goal, assist, assist_primary, assist_secondary, save,  goal_against, pim, penalty, plus_minus
        , power_play_flag, short_handed_flag, game_winner_flag, insurance_goal_flag, empty_net_flag, penalty_shot_flag
        , ice_advantage, ice_advantage_code
        , game_event_time_seconds - coalesce(lag(game_event_time_seconds) over (partition by game_sk, team_sk, event_type order by game_event_time_seconds, event_dup_order_id),0) as seconds_since_team_last_same_event
        , sum(1) over (partition by game_sk, player_sk, event_type order by game_event_time_seconds, event_dup_order_id) as player_game_event_count
        , sum(1) over (partition by player_sk, event_type order by game_date, game_event_time_seconds, event_dup_order_id) as player_season_event_count
    FROM 
        (SELECT 
            l.game_log_sk
            , g.game_sk
            , gt.team_sk
            , p.player_sk
            , g.game_date
            , l.period, l.period_number,l.event_type,l.event_group_id -- p.player_sk,
            , row_number() over (partition by g.game_sk order by l.game_event_time_seconds, l.event_type, l.penalty) as event_order_number
            ,l.event_time,l.event_time_in_seconds,l.game_event_time_seconds
            , row_number() over (partition by g.game_sk, gt.team_sk, p.player_sk, l.period, l.event_time_in_seconds, l.event_type order by l.penalty) as event_dup_order_id
            ,l.shot,l.shot_type,l.shot_quality,l.shot_location_top,l.shot_location_left
            ,case when shot_location_top > (587 / 2) then (587-(l.shot_location_top-5)) else shot_location_top end as shot_location_top_adj
            ,case when shot_location_top > (587 / 2) then (299-(l.shot_location_left-3)) else shot_location_left end as shot_location_left_adj
            ,l.goal,l.assist,l.assist_primary,l.assist_secondary
            ,l.save,l.goal_against,l.pim,l.penalty,l.plus_minus,l.power_play_flag,l.short_handed_flag,l.game_winner_flag,l.insurance_goal_flag,l.empty_net_flag
            , l.ice_advantage, l.ice_advantage_code 
            ,l.penalty_shot_flag
            --, rs.team_pre_event_score, rs.opp_pre_event_score, rs.team_pre_event_score_diff
            
        FROM {{ ref('ahl__transform_game_log_data') }} l
            inner join {{ ref('ahl__trusted_game') }} g
                on l.game_id = g.game_id
            inner join {{ ref('ahl__trusted_game_team') }} gt
                on g.game_sk = gt.game_sk
                and gt.h_a = l.h_a
            inner join {{ ref('ahl__trusted_player') }} p
                on l.player_id = p.player_id
            --left join cte_running_score rs 
            --    on l.game_log_sk = rs.game_log_sk
        )     
)

, cte_running_score as (  
    SELECT game_log_sk
        , case when h_a = 'A' then away_game_team_rolling_goals else home_game_team_rolling_goals end as team_pre_event_score
        , case when h_a = 'H' then away_game_team_rolling_goals else home_game_team_rolling_goals end as opp_pre_event_score
        , case when h_a = 'A' then away_game_team_rolling_goals else home_game_team_rolling_goals end
        - case when h_a = 'H' then away_game_team_rolling_goals else home_game_team_rolling_goals end as team_pre_event_score_diff
    FROM
        (SELECT 
            gl.game_log_sk, gt.h_a
            ,sum(case when gt.h_a = 'A' then coalesce(gl.goal,0) end) over (partition by g.game_sk order by gl.game_event_time_seconds, case gl.event_type when 'GOAL' then 1 else 0 end, gl.event_dup_order_id) - case when event_type = 'GOAL' and gt.h_a = 'A' then 1 else 0 end as away_game_team_rolling_goals    
            ,sum(case when gt.h_a = 'H' then coalesce(gl.goal,0) end) over (partition by g.game_sk order by gl.game_event_time_seconds, case gl.event_type when 'GOAL' then 1 else 0 end, gl.event_dup_order_id) - case when event_type = 'GOAL' and gt.h_a = 'H' then 1 else 0 end as home_game_team_rolling_goals
        FROM cte_base gl
            inner join {{ ref('ahl__trusted_game') }} g
                on gl.game_sk = g.game_sk
            inner join {{ ref('ahl__trusted_game_team') }} gt
                on g.game_sk = gt.game_sk
                and gl.team_sk = gt.team_sk
        )
)

SELECT 
    b.*
    , rs.team_pre_event_score
    , rs.opp_pre_event_score
    , rs.team_pre_event_score_diff
    , CURRENT_DATETIME() as insert_datetime
    , CURRENT_DATETIME() as update_datetime
FROM cte_base b
    left join cte_running_score rs 
        on b.game_log_sk = rs.game_log_sk

/*
    SELECT a.*
        , case when h_a = 'A' then away_game_team_rolling_goals else home_game_team_rolling_goals end as team_pre_event_score
        , case when h_a = 'H' then away_game_team_rolling_goals else home_game_team_rolling_goals end as opp_pre_event_score
        , case when h_a = 'A' then away_game_team_rolling_goals else home_game_team_rolling_goals end
        - case when h_a = 'H' then away_game_team_rolling_goals else home_game_team_rolling_goals end as team_pre_event_score_diff
    FROM
        (SELECT 
            -- sum(coalesce(gl.goal,0)) over (partition by gl.game_sk, gl.team_sk order by game_event_time_seconds, case event_type when 'GOAL' then 1 else 0 end, gl.event_dup_order_id) as game_team_rolling_goals    
            gt.h_a
            ,sum(case when gt.h_a = 'A' then coalesce(gl.goal,0) end) over (partition by g.game_sk order by gl.game_event_time_seconds, case gl.event_type when 'GOAL' then 1 else 0 end, gl.event_dup_order_id) - case when event_type = 'GOAL' and gt.h_a = 'A' then 1 else 0 end as away_game_team_rolling_goals    
            ,sum(case when gt.h_a = 'H' then coalesce(gl.goal,0) end) over (partition by g.game_sk order by gl.game_event_time_seconds, case gl.event_type when 'GOAL' then 1 else 0 end, gl.event_dup_order_id) - case when event_type = 'GOAL' and gt.h_a = 'H' then 1 else 0 end as home_game_team_rolling_goals    
            ,gl.event_type, gl.game_event_time_seconds, gl.event_dup_order_id, gl.game_log_sk 
        FROM {{ ref('ahl__transform_game_log_data') }} gl
            inner join {{ ref('ahl__trusted_game') }} g
                on gl.game_id = g.game_id
            inner join {{ ref('ahl__trusted_game_team') }} gt
                on g.game_sk = gt.game_sk
                and gt.h_a = gl.h_a
        ) a
)
*/


        