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
    , merge_update_columns = ['h_a','w_l','win_bit','goals','shots','assists','hits','infractions'
                                ,'pim','ppgoals','ppopps','update_datetime']
) }}

SELECT 
    GENERATE_UUID() as game_team_sk
    , g.game_sk || '|' || t.team_sk as unique_key
    , g.game_sk
    , t.team_sk
    , rg.game_date
    , 'A' as h_a
    , case when g.complete_flag = true then case when rg.away_goals > rg.home_goals then 'W' else 'L' end end as w_l -- null if game not complete
    , case when g.complete_flag = true then case when rg.away_goals > rg.home_goals then true else false end end as win_bit 
    , rg.away_goals as goals
    , rg.away_shots as shots
    , rg.away_assists as assists
    , rg.away_hits as hits
    , rg.away_infractions as infractions
    , rg.away_pim as pim
    , rg.away_ppgoals as ppgoals
    , rg.away_ppopps as ppopps
    , CURRENT_DATETIME() as insert_datetime
FROM {{ ref('ahl__conf_hockeytech_game') }} rg
    inner join {{ ref('ahl__trusted_game') }} g 
        on rg.game_id = g.game_id
    inner join {{ ref('ahl__transform_team_lookup') }} lkp
        on rg.away_team_abbr = lkp.look_up
    inner join {{ ref('ahl__trusted_team') }} t
        on lkp.team_abbr = t.team_abbr
        and rg.season = t.season
{% if is_incremental() %}
    WHERE rg.game_date >= (SELECT max(game_date) from {{ this }})
{% endif %}
UNION ALL 
SELECT 
    GENERATE_UUID() as game_team_sk
    , g.game_sk || '|' || t.team_sk as unique_key
    , g.game_sk
    , t.team_sk
    , rg.game_date
    , 'H' as h_a
    , case when g.complete_flag = true then case when rg.away_goals < rg.home_goals then 'W' else 'L' end end as w_l -- null if game not complete
    , case when g.complete_flag = true then case when rg.away_goals < rg.home_goals then true else false end end as win_bit 
    , rg.home_goals as goals
    , rg.home_shots as shots
    , rg.home_assists as assists
    , rg.home_hits as hits
    , rg.home_infractions as infractions
    , rg.home_pim as pim
    , rg.home_ppgoals as ppgoals
    , rg.home_ppopps as ppopps
    , CURRENT_DATETIME() as insert_datetime
FROM {{ ref('ahl__conf_hockeytech_game') }} rg
    inner join {{ ref('ahl__trusted_game') }} g 
        on rg.game_id = g.game_id
    inner join {{ ref('ahl__transform_team_lookup') }} lkp
        on rg.home_team_abbr = lkp.look_up
    inner join {{ ref('ahl__trusted_team') }} t
        on lkp.team_abbr = t.team_abbr
        and rg.season = t.season
{% if is_incremental() %}
    WHERE rg.game_date >= (SELECT max(game_date) from {{ this }})
{% endif %}
    