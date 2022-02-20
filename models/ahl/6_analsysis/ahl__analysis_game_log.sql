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
SELECT 
    GENERATE_UUID() as analysis_game_log_sk
    , g.season, g.game_date, gt.h_a, gt.w_l, t.team_abbr, t.division, t.conference
    , opp.team_abbr as opp, opp.division as opp_division, opp.conference as opp_conference, p.player_name, p.player_primary_position
    , gl.game_log_sk, gl.game_sk, gl.team_sk, gl.player_sk, gl.period, gl.period_number, gl.event_time, gl.event_time_in_seconds
    , gl.game_event_time_seconds, gl.event_dup_order_id, gl.event_type, gl.shot, gl.shot_location_top, gl.shot_location_left
    , gl.shot_type, gl.shot_quality, gl.shot_location_left_adj, gl.shot_location_top_adj
    , gl.goal, s.goalie_player_sk, s.goalie_name, s.save, s.goal_against
    , gl.power_play_flag, gl.short_handed_flag, gl.game_winner_flag, gl.insurance_goal_flag, gl.empty_net_flag, gl.penalty_shot_flag
    , gl.ice_advantage, gl.ice_advantage_code, gl.plus_minus, gl.pim, gl.penalty
    , a.primary_assist_player_sk, a.primary_assist_player_name, gl.assist, gl.assist_primary
    , a.secondary_assist_player_sk, a.secondary_assist_player_name, gl.assist_secondary
    , gl.event_order_number, gl.seconds_since_team_last_same_event, gl.player_game_event_count, s.goalie_game_event_count, gl.player_season_event_count
    , s.goalie_season_event_count, gl.team_pre_event_score, gl.opp_pre_event_score, gl.team_pre_event_score_diff
FROM {{ ref('ahl__trusted_game_log') }} gl
    inner join {{ ref('ahl__trusted_game') }} g 
        on gl.game_sk = g.game_sk
    inner join {{ ref('ahl__trusted_game_team') }} gt 
        on g.game_sk = gt.game_sk
        and gl.team_sk = gt.team_sk
    inner join {{ ref('ahl__trusted_team') }} t
        on gt.team_sk = t.team_sk
    inner join {{ ref('ahl__trusted_game_team') }} ogt
        on g.game_sk = ogt.game_sk
        and gl.team_sk != ogt.team_sk
    inner join {{ ref('ahl__trusted_team') }} opp
        on ogt.team_sk = opp.team_sk
    inner join {{ ref('ahl__trusted_player') }} p 
        on gl.player_sk = p.player_sk
    left join 
        (SELECT gl.game_log_sk
            , event_group_id
            , p.player_sk as goalie_player_sk
            , p.player_name as goalie_name
            , gl.save, gl.goal_against
            , player_game_event_count as goalie_game_event_count
            , player_season_event_count as goalie_season_event_count
            , case when gl.event_type = 'SAVE' then 'SHOT' when gl.event_type = 'GOAL AGAINST' then 'GOAL' end as event_type_adj 
        FROM {{ ref('ahl__trusted_game_log') }} gl
            inner join {{ ref('ahl__trusted_game') }} g 
                on gl.game_sk = g.game_sk
            inner join {{ ref('ahl__trusted_player') }} p
                on gl.player_sk = p.player_sk
        WHERE event_type in ('SAVE','GOAL AGAINST')
        ) s
        on gl.event_group_id = s.event_group_id
        and gl.event_type = s.event_type_adj
    left join 
        (SELECT 
            event_group_id, 1 as assist, 'GOAL' as event_type_adj
            , max(case when assist_primary = 1 then player_sk end) as primary_assist_player_sk
            , max(case when assist_primary = 1 then player_name end) as primary_assist_player_name
            , max(case when assist_secondary = 1 then player_sk end) as secondary_assist_player_sk
            , max(case when assist_secondary = 1 then player_name end) as secondary_assist_player_name
            , sum(coalesce(assist_primary,0)) as assist_primary, sum(coalesce(assist_secondary,0)) as assist_secondary
        FROM
            (SELECT event_group_id, gl.assist_primary,gl.assist_secondary, p.player_sk, p.player_name
            FROM {{ ref('ahl__trusted_game_log') }} gl
                inner join {{ ref('ahl__trusted_game') }} g 
                    on gl.game_sk = g.game_sk
                inner join {{ ref('ahl__trusted_player') }} p 
                    on gl.player_sk = p.player_sk
            WHERE gl.event_type = 'ASSIST'
            ) a
        GROUP BY event_group_id
        ) a 
        on gl.event_group_id = a.event_group_id
        and gl.event_type = a.event_type_adj
WHERE gl.event_type not in ('SAVE','GOAL AGAINST','ASSIST')