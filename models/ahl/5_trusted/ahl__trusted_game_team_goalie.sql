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
    , merge_update_columns = ['position','shots_against','saves','goals_against','assists','goals','points','pim','mvp_flag','captain_status'
                              ,'starting_flag','time_on_ice','time_on_ice_seconds','update_datetime']
) }}

SELECT
    GENERATE_UUID() as game_team_goalie_sk
    , gt.game_team_sk || '|' || p.player_sk as unique_key
    , gt.game_team_sk, p.player_sk
    , rgb.game_date
    , rgb.position, rgb.shots_against, rgb.saves, rgb.goals_against, rgb.assists, rgb.goals
    , rgb.points
    , rgb.pim
    , case when g.complete_flag = true then case when mvp.game_mvp_key is not null then true else true end end as mvp_flag
    , coalesce(rgb.captain_status,'') as captain_status
    , rgb.starting_flag
    , rgb.time_on_ice
    , rgb.time_on_ice_seconds
    , CURRENT_DATETIME() as insert_datetime
FROM {{ ref('ahl__conf_hockeytech_goaliebox') }} rgb
    inner join {{ ref('ahl__trusted_player') }} p
        on rgb.goalie_id = p.player_id
    inner join {{ ref('ahl__trusted_game') }} g 
        on rgb.game_key = g.game_id
    inner join {{ ref('ahl__trusted_game_team') }} gt
        on g.game_sk = gt.game_sk
        --and gt.h_a = 'A'
        and rgb.h_a = gt.h_a
    left join {{ ref('ahl__conf_hockeytech_mvp') }} mvp
        on rgb.game_key = mvp.game_key
        and rgb.goalie_id = mvp.player_id
    