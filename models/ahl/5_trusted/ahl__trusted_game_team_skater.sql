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
    , merge_update_columns = ['position','goals','assists','points','shots','pim','plus_minus','mvp_flag','captain_status','starting_flag','update_datetime']
) }}

SELECT
    GENERATE_UUID() as game_team_skater_sk
    , gt.game_team_sk || '|' || p.player_sk as unique_key
    , gt.game_team_sk, p.player_sk
    , rsb.game_date
    , rsb.position, rsb.goals, rsb.assists
    , rsb.points, rsb.shots, rsb.pim, rsb.plus_minus
    , case when g.complete_flag = true then case when mvp.game_mvp_key is not null then true else false end end as mvp_flag
    , coalesce(rsb.captain_status,'') as captain_status
    , rsb.starting_flag
    , CURRENT_DATETIME() as insert_datetime
    , CURRENT_DATETIME() as update_datetime
FROM {{ ref('ahl__conf_hockeytech_skaterbox') }} rsb
    inner join {{ ref('ahl__trusted_player') }} p
        on rsb.skater_id = p.player_id
    inner join {{ ref('ahl__trusted_game') }} g 
        on rsb.game_key = g.game_id
    inner join {{ ref('ahl__trusted_game_team') }} gt
        on g.game_sk = gt.game_sk
        --and gt.h_a = 'A'
        and rsb.h_a = gt.h_a
    left join {{ ref('ahl__conf_hockeytech_mvp') }} mvp
        on rsb.game_key = mvp.game_key
        and rsb.skater_id = mvp.player_id
    