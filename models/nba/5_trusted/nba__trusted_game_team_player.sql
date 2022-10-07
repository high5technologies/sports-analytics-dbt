{{ config(
    tags=["nba"]
    , labels = {'project': 'sports_analytics', 'league':'nba'}
    , materialized='incremental'    
    , unique_key='unique_key'
    , on_schema_change='sync_all_columns'
    , merge_update_columns = ['minutes_string','seconds_played','minutes_played','field_goals_made','field_goals_attempted'
                                ,'field_goals_percentage','three_pointers_made','three_pointers_attempted','three_pointers_percentage'
                                ,'two_pointers_made','two_pointers_attempted','two_pointers_percentage'
                                ,'free_throws_made','free_throws_attempted','free_throws_percentage'
                                ,'rebounds_defensive','rebounds_total','assists','steals','blocks','turnovers','fouls_personal'
                                ,'points','plus_minus_points','doubles_count','update_datetime']
    , partition_by = {
      'field': 'game_date',
      'data_type': 'date',
      'granularity': 'day'
    }
) }}

SELECT
    GENERATE_UUID() as game_team_player_sk
    , gp.minutes_string as unique_key
    , tgt.game_team_sk
    , tgt.game_date
    , tp.player_sk
    , gp.game_player_key as game_player_key_nbacom
    , gp.minutes_string
    , gp.seconds_played
    , gp.minutes_played
    , gp.field_goals_made
    , gp.field_goals_attempted
    , gp.field_goals_percentage
    , gp.three_pointers_made
    , gp.three_pointers_attempted
    , gp.three_pointers_percentage
    , gp.field_goals_made - gp.three_pointers_made as two_pointers_made
    , gp.field_goals_attempted - gp.three_pointers_attempted as two_pointers_attempted
    , case when gp.field_goals_attempted - gp.three_pointers_attempted = 0 then 0 else round((gp.field_goals_made - gp.three_pointers_made)/(gp.field_goals_attempted - gp.three_pointers_attempted),3) end as two_pointers_percentage
    , gp.free_throws_made
    , gp.free_throws_attempted
    , gp.free_throws_percentage
    , gp.rebounds_defensive
    , gp.rebounds_total
    , gp.assists
    , gp.steals
    , gp.blocks
    , gp.turnovers
    , gp.fouls_personal
    , gp.points
    , gp.plus_minus_points
    , case when gp.points >= 10 then 1 else 0 end 
       + case when gp.rebounds_total >= 10 then 1 else 0 end
       + case when gp.assists >= 10 then 1 else 0 end
       + case when gp.blocks >= 10 then 1 else 0 end
       + case when gp.steals >= 10 then 1 else 0 end as doubles_count
    , CURRENT_DATETIME() as insert_datetime
    , CURRENT_DATETIME() as update_datetime
FROM {{ ref('nba__conf_nbacom_game_player') }} gp
    inner join {{ ref('nba__conf_nbacom_game') }} g
        on gp.game_id = g.game_id
    inner join {{ ref('nba__trusted_game') }} tg 
        on g.game_key = tg.game_key_nbacom
    inner join {{ ref('nba__trusted_game_team') }} tgt
        on tg.game_sk = tgt.game_sk
        and gp.h_a = tgt.h_a
    inner join {{ ref('nba__trusted_player') }} tp
        on gp.player_id_nbacom = tp.player_id_nbacom
