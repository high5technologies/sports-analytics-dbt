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
                              , 'opp_team', 'opp_team_abbr', 'opp_division', 'opp_conference', 'inter_division', 'inter_conference'
                              ,'game_status_text','game_timestamp_utc','game_datetime_central','game_start_time','attendance','complete_flag',
                              'ot_flag','ot_count','game_bit','sellout','series_game_number','series_text','if_necessary','national_broadcast_display',
                              'total_score_game','h_a','team_score','opp_score','w_l','wins','losses','score_diff_game','team_seed',
                              'elo_f_d','raptor_f_d','elo_worth_game','elo_win_game','raptor_worth_game','raptor_win_game',
                              'ml','ml_consensus','ml_consensus_wagers','ml_breakeven','ml_hold','ml_implied_prob','ml_breakeven_f_d','ml_datetime'
                            ,'ml_correct','ml_result','spread','spread_odds','spread_consensus','spread_consensus_wagers','spread_breakeven'
                            ,'spread_hold','spread_implied_prob','spread_datetime','spread_correct','spread_result','carmelo_ml_pick'
                            ,'raptor_ml_pick','carmelo_ml_pick_correct','elo_ml_pick_correct','raptor_ml_pick_correct','carmelo_ml_pick_result'
                            ,'elo_ml_pick_result','raptor_ml_pick_result','open_ml','open_spread','open_spread_odds','ml_open_diff'
                            ,'ml_hold_open_diff','ml_breakeven_open_diff','ml_implied_prob_open_diff','spread_open_diff','spread_hold_open_diff'
                            ,'spread_breakeven_open_diff','spread_implied_prob_open_diff','update_datetime']
) }}

SELECT 
    g.game_date || '|' || tt.team_abbr || '|' || tt_opp.team_abbr || '|' || gto.odds_period || '|' || gto.odds_maker || '|' || gto.opening_line_flag as unique_key
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
    , g.inter_division
    , g.inter_conference
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
    , gt.win_bit as wins
    , case when gt.win_bit = 1 then 0 when gt.win_bit = 0 then 1 end as losses
    , gt.score_diff_game
    , gt.team_seed
    , gt.elo_f_d
    , gt.raptor_f_d
    , gt.elo_worth_game
    , gt.elo_win_game
    , gt.raptor_worth_game
    , gt.raptor_win_game
 

    , gto.odds_period
    , gto.odds_maker
    , gto.opening_line_flag
    , gto.ml
    , gto.ml_consensus
    , gto.ml_consensus_wagers
    , gto.ml_breakeven
    , gto.ml_hold
    , gto.ml_implied_prob
    , gto.ml_breakeven_f_d
    -- , gto.ml_implied_f_d
    , gto.ml_datetime
    , gto.ml_correct
    , gto.ml_result

    , gto.spread
    , gto.spread_odds
    , gto.spread_consensus
    , gto.spread_consensus_wagers
    , gto.spread_breakeven
    , gto.spread_hold
    , gto.spread_implied_prob
    , gto.spread_datetime
    , gto.spread_correct
    , gto.spread_result

    , gto.carmelo_ml_pick
    , gto.elo_ml_pick
    , gto.raptor_ml_pick
    --, gto.espn_ml_pick
    , gto.carmelo_ml_pick_correct
    , gto.elo_ml_pick_correct
    , gto.raptor_ml_pick_correct
    --, gto.espn_ml_pick_correct
    , gto.carmelo_ml_pick_result
    , gto.elo_ml_pick_result
    , gto.raptor_ml_pick_result
    --, gto.espn_ml_pick_result

    , gto.open_ml
    , gto.open_spread
    , gto.open_spread_odds
    , gto.ml_open_diff
    , gto.ml_hold_open_diff
    , gto.ml_breakeven_open_diff
    , gto.ml_implied_prob_open_diff
    , gto.spread_open_diff
    , gto.spread_hold_open_diff
    , gto.spread_breakeven_open_diff
    , gto.spread_implied_prob_open_diff

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
    left join {{ ref('nba__trusted_game_team_odds') }} gto
        on gt.game_team_sk = gto.game_team_sk
    --inner join {{ ref('nba__trusted_game_team') }} ogt
    --    on g.game_sk = ogt.game_sk
    --    and gt.team_sk != ogt.team_sk
WHERE 
    gto.odds_period = 'FULL'
    and gto.odds_maker = 'SBR'
    and gto.opening_line_flag = false

{% if is_incremental() %}
    and g.game_date >= (SELECT max(game_date) from {{ this }})
{% endif %}