{{ config(
    tags=["ahl"]
    , labels = {'project': 'sports_analytics', 'league':'ahl'}
    , materialized='table'
) }}

SELECT 
    GENERATE_UUID() as game_log_sk
    , rgl.game_key as game_id
    , rgl.period
    , rgl.period_number
    , rgl.`time` as event_time
    , rgl.event_order_number as game_log_index
    , rgl.h_a
    , rgl.event_time_in_seconds
    , rgl.game_event_time_seconds
    , rgl.event_type
    , rgl.game_log_key as event_group_id
    , rgl.player_id
    , cast(null as INT64) as shot
    , cast(null as numeric) as shot_location_top
    , cast(null as numeric) as shot_location_left
    , null as shot_type
    , null as shot_quality
    , cast(null as INT64) as goal
    , cast(null as INT64) as assist
    , cast(null as INT64) as assist_primary
    , cast(null as INT64) as assist_secondary
    , cast(null as INT64) as save
    , cast(null as INT64) as goal_against
    , cast(rgl.pim as INT64) as pim
    , rgl.penalty
    , cast(null as INT64) as plus_minus
    , rgl.ice_advantage				-- 
    , rgl.ice_advantage_code
    , rgl.power_play_flag
    , rgl.short_handed_flag
    , cast(null as BOOL) as game_winner_flag
    , cast(null as BOOL) as insurance_goal_flag
    , cast(null as BOOL) as empty_net_flag
    , cast(null as BOOL) as penalty_shot_flag
FROM {{ ref('ahl__transform_game_log_pre') }} rgl 
WHERE event_type = 'PENALTY'

-- GOALIE CHANGE
UNION ALL
SELECT 
    GENERATE_UUID() as game_log_sk
    , rgl.game_key as game_id
    , rgl.period
    , rgl.period_number
    , rgl.`time` as event_time
    , rgl.event_order_number as game_log_index
    , rgl.h_a
    , rgl.event_time_in_seconds
    , rgl.game_event_time_seconds
    , case when rgl.goalie_coming_in_id is not null then 'GOALIE ON' else 'GOALIE OFF' end event_type
    , rgl.game_log_key as event_group_id
    , case when rgl.goalie_coming_in_id is not null then rgl.goalie_coming_in_id else rgl.goalie_coming_out_id end as player_id
    , cast(null as INT64) as shot
    , cast(null as numeric) as shot_location_top
    , cast(null as numeric) as shot_location_left
    , null as shot_type
    , null as shot_quality
    , cast(null as INT64) as goal
    , cast(null as INT64) as assist
    , cast(null as INT64) as assist_primary
    , cast(null as INT64) as assist_secondary
    , cast(null as INT64) as save
    , cast(null as INT64) as goal_against
    , cast(null as INT64) as pim
    , null as penalty
    , cast(null as INT64) as plus_minus
    , rgl.ice_advantage				-- 
    , rgl.ice_advantage_code
    , rgl.power_play_flag
    , rgl.short_handed_flag
    , cast(null as BOOL) as game_winner_flag
    , cast(null as BOOL) as insurance_goal_flag
    , cast(null as BOOL) as empty_net_flag
    , cast(null as BOOL) as penalty_shot_flag
FROM {{ ref('ahl__transform_game_log_pre') }} rgl 
WHERE event_type = 'GOALIE_CHANGE'

-- MISSED SHOTS - Shooter
UNION ALL
SELECT 
    GENERATE_UUID() as game_log_sk
    , rgl.game_key as game_id
    , rgl.period
    , rgl.period_number
    , rgl.`time` as event_time
    , rgl.event_order_number as game_log_index
    , rgl.h_a
    , rgl.event_time_in_seconds
    , rgl.game_event_time_seconds
    , rgl.event_type
    , rgl.game_log_key as event_group_id
    , rgl.player_id
    , 1 as shot
    , rgl.x_location as shot_location_top
    , rgl.y_location as shot_location_left
    , rgl.shot_type as shot_type
    , rgl.shot_quality as shot_quality
    , 0 as goal
    , cast(null as INT64) as assist
    , cast(null as INT64) as assist_primary
    , cast(null as INT64) as assist_secondary
    , cast(null as INT64) as save
    , cast(null as INT64) as goal_against
    , cast(null as INT64) as pim
    , null as penalty
    , cast(null as INT64) as plus_minus
    , rgl.ice_advantage				-- 
    , rgl.ice_advantage_code
    , rgl.power_play_flag
    , rgl.short_handed_flag
    , cast(null as BOOL) as game_winner_flag
    , cast(null as BOOL) as insurance_goal_flag
    , cast(null as BOOL) as empty_net_flag
    , cast(null as BOOL) as penalty_shot_flag
FROM {{ ref('ahl__transform_game_log_pre') }} rgl 
WHERE event_type = 'SHOT'

-- MISSED SHOTS - Goalie
UNION ALL
SELECT 
    GENERATE_UUID() as game_log_sk
    , rgl.game_key as game_id
    , rgl.period
    , rgl.period_number
    , rgl.`time` as event_time
    , rgl.event_order_number as game_log_index
    , case when rgl.h_a = 'A' then 'H' else 'A' end as h_a -- flip h_a since this is the goalie h_a
    , rgl.event_time_in_seconds
    , rgl.game_event_time_seconds
    , 'SAVE' as event_type
    , rgl.game_log_key as event_group_id
    , rgl.goalie_id as player_id
    , cast(null as INT64) as shot
    , rgl.x_location as shot_location_top
    , rgl.y_location as shot_location_left
    , rgl.shot_type as shot_type
    , rgl.shot_quality as shot_quality
    , 0 as goal
    , cast(null as INT64) as assist
    , cast(null as INT64) as assist_primary
    , cast(null as INT64) as assist_secondary
    , 1 as save
    , cast(null as INT64) as goal_against
    , cast(null as INT64) as pim
    , null as penalty
    , cast(null as INT64) as plus_minus
    , rgl.ice_advantage				-- 
    , rgl.ice_advantage_code
    , rgl.power_play_flag
    , rgl.short_handed_flag
    , cast(null as BOOL) as game_winner_flag
    , cast(null as BOOL) as insurance_goal_flag
    , cast(null as BOOL) as empty_net_flag
    , cast(null as BOOL) as penalty_shot_flag
FROM {{ ref('ahl__transform_game_log_pre') }} rgl 
WHERE event_type = 'SHOT'

-- GOALS - Shooter
UNION ALL
SELECT 
    GENERATE_UUID() as game_log_sk
    , rgl.game_key as game_id
    , rgl.period
    , rgl.period_number
    , rgl.`time` as event_time
    , rgl.event_order_number as game_log_index
    , rgl.h_a
    , rgl.event_time_in_seconds
    , rgl.game_event_time_seconds
    , rgl.event_type
    , rgl.game_log_key as event_group_id
    , rgl.player_id
    , 1 as shot
    , rgl.x_location as shot_location_top
    , rgl.y_location as shot_location_left
    , rgl.shot_type as shot_type
    , rgl.shot_quality as shot_quality
    , 1 as goal
    , case when primary_assist_player_id > 0 then 1 else 0 end as assist
    , case when primary_assist_player_id > 0 then 1 else 0 end as assist_primary
    , case when secondary_assist_player_id > 0 then 1 else 0 end as assist_secondary
    , cast(null as INT64) as save
    , cast(null as INT64) as goal_against
    , cast(null as INT64) as pim
    , null as penalty
    , cast(null as INT64) as plus_minus
    , rgl.ice_advantage				-- 
    , rgl.ice_advantage_code
    , rgl.power_play_flag
    , rgl.short_handed_flag
    , rgl.game_winning_goal_flag as game_winner_flag
    , rgl.insurance_goal_flag
    , rgl.empty_net_flag
    , rgl.penalty_shot_flag
FROM {{ ref('ahl__transform_game_log_pre') }} rgl 
WHERE event_type = 'GOAL'

-- GOALS - Goalie
UNION ALL
SELECT 
    GENERATE_UUID() as game_log_sk
    , rgl.game_key as game_id
    , rgl.period
    , rgl.period_number
    , rgl.`time` as event_time
    , rgl.event_order_number as game_log_index
    , case when rgl.h_a = 'A' then 'H' else 'A' end as h_a -- flip h_a since this is the goalie h_a
    , rgl.event_time_in_seconds
    , rgl.game_event_time_seconds
    , 'GOAL AGAINST' as event_type
    , rgl.game_log_key as event_group_id
    , rgl.goalie_id as player_id
    , cast(null as INT64) as shot
    , rgl.x_location as shot_location_top
    , rgl.y_location as shot_location_left
    , rgl.shot_type as shot_type
    , rgl.shot_quality as shot_quality
    , cast(null as INT64) as goal
    , cast(null as INT64) as assist
    , cast(null as INT64) as assist_primary
    , cast(null as INT64) as assist_secondary
    , cast(null as INT64) as save
    , 1 as goal_against
    , cast(null as INT64) as pim
    , null as penalty
    , cast(null as INT64) as plus_minus
    , rgl.ice_advantage				-- 
    , rgl.ice_advantage_code
    , rgl.power_play_flag
    , rgl.short_handed_flag
    , rgl.game_winning_goal_flag as game_winner_flag
    , rgl.insurance_goal_flag
    , rgl.empty_net_flag
    , rgl.penalty_shot_flag
FROM {{ ref('ahl__transform_game_log_pre') }} rgl 
WHERE event_type = 'GOAL'

-- GOALS - Assist - Primary
UNION ALL
SELECT 
    GENERATE_UUID() as game_log_sk
    , rgl.game_key as game_id
    , rgl.period
    , rgl.period_number
    , rgl.`time` as event_time
    , rgl.event_order_number as game_log_index
    , rgl.h_a
    , rgl.event_time_in_seconds
    , rgl.game_event_time_seconds
    , 'ASSIST' as event_type
    , rgl.game_log_key as event_group_id
    , rgl.primary_assist_player_id as player_id
    , cast(null as INT64) as shot
    , rgl.x_location as shot_location_top
    , rgl.y_location as shot_location_left
    , rgl.shot_type as shot_type
    , rgl.shot_quality as shot_quality
    , cast(null as INT64) as goal
    , 1 as assist
    , 1 as assist_primary
    , cast(null as INT64) as assist_secondary
    , cast(null as INT64) as save
    , cast(null as INT64) as goal_against
    , cast(null as INT64) as pim
    , null as penalty
    , cast(null as INT64) as plus_minus
    , rgl.ice_advantage				-- 
    , rgl.ice_advantage_code
    , rgl.power_play_flag
    , rgl.short_handed_flag
    , rgl.game_winning_goal_flag as game_winner_flag
    , rgl.insurance_goal_flag
    , rgl.empty_net_flag
    , rgl.penalty_shot_flag
FROM {{ ref('ahl__transform_game_log_pre') }} rgl 
WHERE event_type = 'GOAL' and rgl.primary_assist_player_id is not null

-- GOALS - Assist - Secondary
UNION ALL
SELECT 
    GENERATE_UUID() as game_log_sk
    , rgl.game_key as game_id
    , rgl.period
    , rgl.period_number
    , rgl.`time` as event_time
    , rgl.event_order_number as game_log_index
    , rgl.h_a
    , rgl.event_time_in_seconds
    , rgl.game_event_time_seconds
    , 'ASSIST' as event_type
    , rgl.game_log_key as event_group_id
    , rgl.secondary_assist_player_id as player_id
    , cast(null as INT64) as shot
    , rgl.x_location as shot_location_top
    , rgl.y_location as shot_location_left
    , rgl.shot_type as shot_type
    , rgl.shot_quality as shot_quality
    , cast(null as INT64) as goal
    , 1 as assist
    , cast(null as INT64) as assist_primary
    , 1 as assist_secondary
    , cast(null as INT64) as save
    , cast(null as INT64) as goal_against
    , cast(null as INT64) as pim
    , null as penalty
    , cast(null as INT64) as plus_minus
    , rgl.ice_advantage				-- 
    , rgl.ice_advantage_code
    , rgl.power_play_flag
    , rgl.short_handed_flag
    , rgl.game_winning_goal_flag as game_winner_flag
    , rgl.insurance_goal_flag
    , rgl.empty_net_flag
    , rgl.penalty_shot_flag
FROM {{ ref('ahl__transform_game_log_pre') }} rgl 
WHERE event_type = 'GOAL' and rgl.secondary_assist_player_id is not null

-- Penalty Shot - Shooter
UNION ALL
SELECT 
    GENERATE_UUID() as game_log_sk
    , rgl.game_key as game_id
    , rgl.period
    , rgl.period_number
    , rgl.`time` as event_time
    , rgl.event_order_number as game_log_index
    , rgl.h_a 
    , rgl.event_time_in_seconds
    , rgl.game_event_time_seconds
    , rgl.event_type
    , rgl.game_log_key as event_group_id
    , rgl.player_id
    , 1 as shot
    , cast(null as numeric) as shot_location_top
    , cast(null as numeric) as shot_location_left
    , null as shot_type
    , null as shot_quality
    , cast(null as INT64) as goal
    , cast(null as INT64) as assist
    , cast(null as INT64) as assist_primary
    , cast(null as INT64) as assist_secondary
    , cast(null as INT64) as save
    , cast(null as INT64) as goal_against
    , cast(null as INT64) as pim
    , null as penalty
    , cast(null as INT64) as plus_minus
    , rgl.ice_advantage				-- 
    , rgl.ice_advantage_code
    , rgl.power_play_flag
    , rgl.short_handed_flag
    , rgl.game_winning_goal_flag as game_winner_flag
    , rgl.insurance_goal_flag
    , rgl.empty_net_flag
    , rgl.penalty_shot_flag
FROM {{ ref('ahl__transform_game_log_pre') }} rgl 
WHERE event_type = 'PENALTYSHOT'

-- Penalty Shot - Goalie
UNION ALL
SELECT 
    GENERATE_UUID() as game_log_sk
    , rgl.game_key as game_id
    , rgl.period
    , rgl.period_number
    , rgl.`time` as event_time
    , rgl.event_order_number as game_log_index
    , case when rgl.h_a = 'A' then 'H' else 'A' end as h_a -- flip h_a since this is the goalie h_a
    , rgl.event_time_in_seconds
    , rgl.game_event_time_seconds
    , rgl.event_type
    , rgl.game_log_key as event_group_id
    , rgl.goalie_id as player_id
    , cast(null as INT64) as shot
    , cast(null as numeric) as shot_location_top
    , cast(null as numeric) as shot_location_left
    , null as shot_type
    , null as shot_quality
    , cast(null as INT64) as goal
    , cast(null as INT64) as assist
    , cast(null as INT64) as assist_primary
    , cast(null as INT64) as assist_secondary
    , cast(null as INT64) as save
    , cast(null as INT64) as goal_against
    , cast(null as INT64) as pim
    , null as penalty
    , cast(null as INT64) as plus_minus
    , rgl.ice_advantage				-- 
    , rgl.ice_advantage_code
    , rgl.power_play_flag
    , rgl.short_handed_flag
    , rgl.game_winning_goal_flag as game_winner_flag
    , rgl.insurance_goal_flag
    , rgl.empty_net_flag
    , rgl.penalty_shot_flag
FROM {{ ref('ahl__transform_game_log_pre') }} rgl 
WHERE event_type = 'PENALTYSHOT'

-- PLUS/MINUS - MINUS 1
UNION ALL
SELECT 
    GENERATE_UUID() as game_log_sk
    , rgl.game_key as game_id
    , rgl.period
    , rgl.period_number
    , rgl.`time` as event_time
    , rgl.event_order_number as game_log_index
    , rgl.h_a 
    , rgl.event_time_in_seconds
    , rgl.game_event_time_seconds
    , 'PLUS/MINUS' as event_type
    , rgl.game_log_key as event_group_id
    , rgl.minus_player_id_1								-- -------------------
    , cast(null as INT64) as shot
    , cast(null as numeric) as shot_location_top
    , cast(null as numeric) as shot_location_left
    , null as shot_type
    , null as shot_quality
    , cast(null as INT64) as goal
    , cast(null as INT64) as assist
    , cast(null as INT64) as assist_primary
    , cast(null as INT64) as assist_secondary
    , cast(null as INT64) as save
    , cast(null as INT64) as goal_against
    , cast(null as INT64) as pim
    , null as penalty
    , -1 as plus_minus
    , rgl.ice_advantage				-- 
    , rgl.ice_advantage_code
    , rgl.power_play_flag
    , rgl.short_handed_flag
    , rgl.game_winning_goal_flag as game_winner_flag
    , rgl.insurance_goal_flag
    , rgl.empty_net_flag
    , rgl.penalty_shot_flag
FROM {{ ref('ahl__transform_game_log_pre') }} rgl
WHERE minus_player_id_1 is not null

-- PLUS/MINUS - MINUS 2
UNION ALL
SELECT 
    GENERATE_UUID() as game_log_sk
    , rgl.game_key as game_id
    , rgl.period
    , rgl.period_number
    , rgl.`time` as event_time
    , rgl.event_order_number as game_log_index
    , rgl.h_a 
    , rgl.event_time_in_seconds
    , rgl.game_event_time_seconds
    , 'PLUS/MINUS' as event_type
    , rgl.game_log_key as event_group_id
    , rgl.minus_player_id_2								-- -------------------
    , cast(null as INT64) as shot
    , cast(null as numeric) as shot_location_top
    , cast(null as numeric) as shot_location_left
    , null as shot_type
    , null as shot_quality
    , cast(null as INT64) as goal
    , cast(null as INT64) as assist
    , cast(null as INT64) as assist_primary
    , cast(null as INT64) as assist_secondary
    , cast(null as INT64) as save
    , cast(null as INT64) as goal_against
    , cast(null as INT64) as pim
    , null as penalty
    , -1 as plus_minus
    , rgl.ice_advantage				-- 
    , rgl.ice_advantage_code
    , rgl.power_play_flag
    , rgl.short_handed_flag
    , rgl.game_winning_goal_flag as game_winner_flag
    , rgl.insurance_goal_flag
    , rgl.empty_net_flag
    , rgl.penalty_shot_flag
FROM {{ ref('ahl__transform_game_log_pre') }} rgl
WHERE minus_player_id_2 is not null

-- PLUS/MINUS - MINUS 3
UNION ALL
SELECT 
    GENERATE_UUID() as game_log_sk
    , rgl.game_key as game_id
    , rgl.period
    , rgl.period_number
    , rgl.`time` as event_time
    , rgl.event_order_number as game_log_index
    , rgl.h_a 
    , rgl.event_time_in_seconds
    , rgl.game_event_time_seconds
    , 'PLUS/MINUS' as event_type
    , rgl.game_log_key as event_group_id
    , rgl.minus_player_id_3								-- -------------------
    , cast(null as INT64) as shot
    , cast(null as numeric) as shot_location_top
    , cast(null as numeric) as shot_location_left
    , null as shot_type
    , null as shot_quality
    , cast(null as INT64) as goal
    , cast(null as INT64) as assist
    , cast(null as INT64) as assist_primary
    , cast(null as INT64) as assist_secondary
    , cast(null as INT64) as save
    , cast(null as INT64) as goal_against
    , cast(null as INT64) as pim
    , null as penalty
    , -1 as plus_minus
    , rgl.ice_advantage				-- 
    , rgl.ice_advantage_code
    , rgl.power_play_flag
    , rgl.short_handed_flag
    , rgl.game_winning_goal_flag as game_winner_flag
    , rgl.insurance_goal_flag
    , rgl.empty_net_flag
    , rgl.penalty_shot_flag
FROM {{ ref('ahl__transform_game_log_pre') }} rgl
WHERE minus_player_id_3 is not null

-- PLUS/MINUS - MINUS 4
UNION ALL
SELECT 
    GENERATE_UUID() as game_log_sk
    , rgl.game_key as game_id
    , rgl.period
    , rgl.period_number
    , rgl.`time` as event_time
    , rgl.event_order_number as game_log_index
    , rgl.h_a 
    , rgl.event_time_in_seconds
    , rgl.game_event_time_seconds
    , 'PLUS/MINUS' as event_type
    , rgl.game_log_key as event_group_id
    , rgl.minus_player_id_4								-- -------------------
    , cast(null as INT64) as shot
    , cast(null as numeric) as shot_location_top
    , cast(null as numeric) as shot_location_left
    , null as shot_type
    , null as shot_quality
    , cast(null as INT64) as goal
    , cast(null as INT64) as assist
    , cast(null as INT64) as assist_primary
    , cast(null as INT64) as assist_secondary
    , cast(null as INT64) as save
    , cast(null as INT64) as goal_against
    , cast(null as INT64) as pim
    , null as penalty
    , -1 as plus_minus
    , rgl.ice_advantage				-- 
    , rgl.ice_advantage_code
    , rgl.power_play_flag
    , rgl.short_handed_flag
    , rgl.game_winning_goal_flag as game_winner_flag
    , rgl.insurance_goal_flag
    , rgl.empty_net_flag
    , rgl.penalty_shot_flag
FROM {{ ref('ahl__transform_game_log_pre') }} rgl
WHERE minus_player_id_4 is not null

-- PLUS/MINUS - MINUS 5
UNION ALL
SELECT 
    GENERATE_UUID() as game_log_sk
    , rgl.game_key as game_id
    , rgl.period
    , rgl.period_number
    , rgl.`time` as event_time
    , rgl.event_order_number as game_log_index
    , rgl.h_a 
    , rgl.event_time_in_seconds
    , rgl.game_event_time_seconds
    , 'PLUS/MINUS' as event_type
    , rgl.game_log_key as event_group_id
    , rgl.minus_player_id_5								-- -------------------
    , cast(null as INT64) as shot
    , cast(null as numeric) as shot_location_top
    , cast(null as numeric) as shot_location_left
    , null as shot_type
    , null as shot_quality
    , cast(null as INT64) as goal
    , cast(null as INT64) as assist
    , cast(null as INT64) as assist_primary
    , cast(null as INT64) as assist_secondary
    , cast(null as INT64) as save
    , cast(null as INT64) as goal_against
    , cast(null as INT64) as pim
    , null as penalty
    , -1 as plus_minus
    , rgl.ice_advantage				-- 
    , rgl.ice_advantage_code
    , rgl.power_play_flag
    , rgl.short_handed_flag
    , rgl.game_winning_goal_flag as game_winner_flag
    , rgl.insurance_goal_flag
    , rgl.empty_net_flag
    , rgl.penalty_shot_flag
FROM {{ ref('ahl__transform_game_log_pre') }} rgl
WHERE minus_player_id_5 is not null

-- PLUS/MINUS - MINUS 6
UNION ALL
SELECT 
    GENERATE_UUID() as game_log_sk
    , rgl.game_key as game_id
    , rgl.period
    , rgl.period_number
    , rgl.`time` as event_time
    , rgl.event_order_number as game_log_index
    , rgl.h_a 
    , rgl.event_time_in_seconds
    , rgl.game_event_time_seconds
    , 'PLUS/MINUS' as event_type
    , rgl.game_log_key as event_group_id
    , rgl.minus_player_id_6								-- -------------------
    , cast(null as INT64) as shot
    , cast(null as numeric) as shot_location_top
    , cast(null as numeric) as shot_location_left
    , null as shot_type
    , null as shot_quality
    , cast(null as INT64) as goal
    , cast(null as INT64) as assist
    , cast(null as INT64) as assist_primary
    , cast(null as INT64) as assist_secondary
    , cast(null as INT64) as save
    , cast(null as INT64) as goal_against
    , cast(null as INT64) as pim
    , null as penalty
    , -1 as plus_minus
    , rgl.ice_advantage				-- 
    , rgl.ice_advantage_code
    , rgl.power_play_flag
    , rgl.short_handed_flag
    , rgl.game_winning_goal_flag as game_winner_flag
    , rgl.insurance_goal_flag
    , rgl.empty_net_flag
    , rgl.penalty_shot_flag
FROM {{ ref('ahl__transform_game_log_pre') }} rgl
WHERE minus_player_id_6 is not null

-- PLUS/MINUS - PLUS 1
UNION ALL
SELECT 
    GENERATE_UUID() as game_log_sk
    , rgl.game_key as game_id
    , rgl.period
    , rgl.period_number
    , rgl.`time` as event_time
    , rgl.event_order_number as game_log_index
    , rgl.h_a 
    , rgl.event_time_in_seconds
    , rgl.game_event_time_seconds
    , 'PLUS/MINUS' as event_type
    , rgl.game_log_key as event_group_id
    , rgl.plus_player_id_1								-- -------------------
    , cast(null as INT64) as shot
    , cast(null as numeric) as shot_location_top
    , cast(null as numeric) as shot_location_left
    , null as shot_type
    , null as shot_quality
    , cast(null as INT64) as goal
    , cast(null as INT64) as assist
    , cast(null as INT64) as assist_primary
    , cast(null as INT64) as assist_secondary
    , cast(null as INT64) as save
    , cast(null as INT64) as goal_against
    , cast(null as INT64) as pim
    , null as penalty
    , 1 as plus_minus
    , rgl.ice_advantage				-- 
    , rgl.ice_advantage_code
    , rgl.power_play_flag
    , rgl.short_handed_flag
    , rgl.game_winning_goal_flag as game_winner_flag
    , rgl.insurance_goal_flag
    , rgl.empty_net_flag
    , rgl.penalty_shot_flag
FROM {{ ref('ahl__transform_game_log_pre') }} rgl
WHERE plus_player_id_1 is not null

-- PLUS/MINUS - PLUS 2
UNION ALL
SELECT  
    GENERATE_UUID() as game_log_sk
    , rgl.game_key as game_id
    , rgl.period
    , rgl.period_number
    , rgl.`time` as event_time
    , rgl.event_order_number as game_log_index
    , rgl.h_a 
    , rgl.event_time_in_seconds
    , rgl.game_event_time_seconds
    , 'PLUS/MINUS' as event_type
    , rgl.game_log_key as event_group_id
    , rgl.plus_player_id_2								-- -------------------
    , cast(null as INT64) as shot
    , cast(null as numeric) as shot_location_top
    , cast(null as numeric) as shot_location_left
    , null as shot_type
    , null as shot_quality
    , cast(null as INT64) as goal
    , cast(null as INT64) as assist
    , cast(null as INT64) as assist_primary
    , cast(null as INT64) as assist_secondary
    , cast(null as INT64) as save
    , cast(null as INT64) as goal_against
    , cast(null as INT64) as pim
    , null as penalty
    , 1 as plus_minus
    , rgl.ice_advantage				-- 
    , rgl.ice_advantage_code
    , rgl.power_play_flag
    , rgl.short_handed_flag
    , rgl.game_winning_goal_flag as game_winner_flag
    , rgl.insurance_goal_flag
    , rgl.empty_net_flag
    , rgl.penalty_shot_flag
FROM {{ ref('ahl__transform_game_log_pre') }} rgl
WHERE plus_player_id_2 is not null

-- PLUS/MINUS - PLUS 3
UNION ALL
SELECT 
    GENERATE_UUID() as game_log_sk
    , rgl.game_key as game_id
    , rgl.period
    , rgl.period_number
    , rgl.`time` as event_time
    , rgl.event_order_number as game_log_index
    , rgl.h_a 
    , rgl.event_time_in_seconds
    , rgl.game_event_time_seconds
    , 'PLUS/MINUS' as event_type
    , rgl.game_log_key as event_group_id
    , rgl.plus_player_id_3								-- -------------------
    , cast(null as INT64) as shot
    , cast(null as numeric) as shot_location_top
    , cast(null as numeric) as shot_location_left
    , null as shot_type
    , null as shot_quality
    , cast(null as INT64) as goal
    , cast(null as INT64) as assist
    , cast(null as INT64) as assist_primary
    , cast(null as INT64) as assist_secondary
    , cast(null as INT64) as save
    , cast(null as INT64) as goal_against
    , cast(null as INT64) as pim
    , null as penalty
    , 1 as plus_minus
    , rgl.ice_advantage				-- 
    , rgl.ice_advantage_code
    , rgl.power_play_flag
    , rgl.short_handed_flag
    , rgl.game_winning_goal_flag as game_winner_flag
    , rgl.insurance_goal_flag
    , rgl.empty_net_flag
    , rgl.penalty_shot_flag
FROM {{ ref('ahl__transform_game_log_pre') }} rgl
WHERE plus_player_id_3 is not null

-- PLUS/MINUS - PLUS 4
UNION ALL
SELECT 
    GENERATE_UUID() as game_log_sk
    , rgl.game_key as game_id
    , rgl.period
    , rgl.period_number
    , rgl.`time` as event_time
    , rgl.event_order_number as game_log_index
    , rgl.h_a 
    , rgl.event_time_in_seconds
    , rgl.game_event_time_seconds
    , 'PLUS/MINUS' as event_type
    , rgl.game_log_key as event_group_id
    , rgl.plus_player_id_4								-- -------------------
    , cast(null as INT64) as shot
    , cast(null as numeric) as shot_location_top
    , cast(null as numeric) as shot_location_left
    , null as shot_type
    , null as shot_quality
    , cast(null as INT64) as goal
    , cast(null as INT64) as assist
    , cast(null as INT64) as assist_primary
    , cast(null as INT64) as assist_secondary
    , cast(null as INT64) as save
    , cast(null as INT64) as goal_against
    , cast(null as INT64) as pim
    , null as penalty
    , 1 as plus_minus
    , rgl.ice_advantage				-- 
    , rgl.ice_advantage_code
    , rgl.power_play_flag
    , rgl.short_handed_flag
    , rgl.game_winning_goal_flag as game_winner_flag
    , rgl.insurance_goal_flag
    , rgl.empty_net_flag
    , rgl.penalty_shot_flag
FROM {{ ref('ahl__transform_game_log_pre') }} rgl
WHERE plus_player_id_4 is not null

-- PLUS/MINUS - PLUS 5
UNION ALL
SELECT 
    GENERATE_UUID() as game_log_sk
    , rgl.game_key as game_id
    , rgl.period
    , rgl.period_number
    , rgl.`time` as event_time
    , rgl.event_order_number as game_log_index
    , rgl.h_a 
    , rgl.event_time_in_seconds
    , rgl.game_event_time_seconds
    , 'PLUS/MINUS' as event_type
    , rgl.game_log_key as event_group_id
    , rgl.plus_player_id_5								-- -------------------
    , cast(null as INT64) as shot
    , cast(null as numeric) as shot_location_top
    , cast(null as numeric) as shot_location_left
    , null as shot_type
    , null as shot_quality
    , cast(null as INT64) as goal
    , cast(null as INT64) as assist
    , cast(null as INT64) as assist_primary
    , cast(null as INT64) as assist_secondary
    , cast(null as INT64) as save
    , cast(null as INT64) as goal_against
    , cast(null as INT64) as pim
    , null as penalty
    , 1 as plus_minus
    , rgl.ice_advantage				-- 
    , rgl.ice_advantage_code
    , rgl.power_play_flag
    , rgl.short_handed_flag
    , rgl.game_winning_goal_flag as game_winner_flag
    , rgl.insurance_goal_flag
    , rgl.empty_net_flag
    , rgl.penalty_shot_flag
FROM {{ ref('ahl__transform_game_log_pre') }} rgl
WHERE plus_player_id_5 is not null

-- PLUS/MINUS - PLUS 6
UNION ALL
SELECT 
    GENERATE_UUID() as game_log_sk
    , rgl.game_key as game_id
    , rgl.period
    , rgl.period_number
    , rgl.`time` as event_time
    , rgl.event_order_number as game_log_index
    , rgl.h_a 
    , rgl.event_time_in_seconds
    , rgl.game_event_time_seconds
    , 'PLUS/MINUS' as event_type
    , rgl.game_log_key as event_group_id
    , rgl.plus_player_id_6								-- -------------------
    , cast(null as INT64) as shot
    , cast(null as numeric) as shot_location_top
    , cast(null as numeric) as shot_location_left
    , null as shot_type
    , null as shot_quality
    , cast(null as INT64) as goal
    , cast(null as INT64) as assist
    , cast(null as INT64) as assist_primary
    , cast(null as INT64) as assist_secondary
    , cast(null as INT64) as save
    , cast(null as INT64) as goal_against
    , cast(null as INT64) as pim
    , null as penalty
    , 1 as plus_minus
    , rgl.ice_advantage				-- 
    , rgl.ice_advantage_code
    , rgl.power_play_flag
    , rgl.short_handed_flag
    , rgl.game_winning_goal_flag as game_winner_flag
    , rgl.insurance_goal_flag
    , rgl.empty_net_flag
    , rgl.penalty_shot_flag
FROM {{ ref('ahl__transform_game_log_pre') }} rgl
WHERE plus_player_id_6 is not null
