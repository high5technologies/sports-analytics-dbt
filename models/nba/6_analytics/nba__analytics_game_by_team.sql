{{ config(
    tags=["nba"]
    , labels = {'project': 'sports_analytics', 'league':'nba'}
    , partition_by = {
      'field': 'game_date',
      'data_type': 'date',
      'granularity': 'day'
    }
    , materialized='incremental'    
    , unique_key='unique_key'
    , merge_update_columns = ['arena_name','arena_city','arena_state','arena_country','arena_timezone','game_yearmonth','game_yearmonth_formatted','game_week',
                              'season','season_type'
                              , 'team', 'team_abbr', 'division', 'conference'
                              , 'opp_team', 'opp_team_abbr', 'opp_division', 'opp_conference'
                              ,'game_status_text','game_timestamp_utc','game_datetime_central','game_start_time','attendance','complete_flag',
                              'ot_flag','ot_count','game_bit','sellout','series_game_number','series_text','if_necessary','national_broadcast_display',
                              'total_score_game','h_a','team_score','opp_score','w_l','win_bit','score_diff_game','team_seed',
                              'elo_f_d','raptor_f_d','elo_worth_game','elo_win_game','raptor_worth_game','raptor_win_game','update_datetime']
) }}

SELECT 
    g.game_date || '|' || tt.team_abbr || '|' || tt_opp.team_abbr as unique_key
    , g.game_sk
    , gt.game_team_sk
    , a.arena_sk
    , g.game_date
    , a.arena_name
    , a.arena_city
    , a.arena_state
    , a.arena_country
    , a.arena_timezone
    , g.game_yearmonth
    , g.game_yearmonth_formatted
    , g.game_week
    , g.season
    , g.season_type
    , tt.team
    , tt.team_abbr
    , tt.division
    , tt.conference
    , tt_opp.team as opp_team
    , tt_opp.team_abbr as opp_team_abbr
    , tt_opp.division as opp_division
    , tt_opp.conference as opp_conference
    , g.game_status_text
    , g.game_timestamp_utc
    , g.game_datetime_central
    , g.game_start_time
    , g.attendance
    , g.complete_flag
    , g.ot_flag
    , g.ot_count
    , g.game_bit
    , g.sellout
    , g.series_game_number
    , g.series_text
    , g.if_necessary
    , g.national_broadcast_display
    , g.total_score_game
    , gt.h_a
    , gt.team_score
    , gt.opp_score
    , gt.w_l
    , gt.win_bit
    , gt.score_diff_game
    , gt.team_seed
    , gt.elo_f_d
    , gt.raptor_f_d
    , gt.elo_worth_game
    , gt.elo_win_game
    , gt.raptor_worth_game
    , gt.raptor_win_game
    , CURRENT_DATETIME() as insert_datetime
    , CURRENT_DATETIME() as update_datetime
FROM {{ ref('nba__trusted_game') }} g
    inner join {{ ref('nba__trusted_game_team') }} gt
        on g.game_sk = gt.game_sk
    inner join {{ ref('nba__trusted_team') }} tt
        on gt.team_sk = tt.team_sk
    inner join {{ ref('nba__trusted_team') }} tt_opp
        on gt.opp_team_sk = tt_opp.team_sk
    inner join {{ ref('nba__trusted_arena') }} a 
        on g.arena_sk = a.arena_sk
{% if is_incremental() %}
    WHERE g.game_date >= (SELECT max(game_date) from {{ this }})
{% endif %}
