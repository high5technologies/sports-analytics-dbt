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
    , merge_update_columns = ['ml','ml_consensus','ml_consensus_wagers','ml_breakeven','ml_hold','ml_implied_prob','ml_breakeven_f_d','ml_datetime'
                                ,'ml_correct','ml_result','spread','spread_odds','spread_consensus','spread_consensus_wagers','spread_breakeven'
                                ,'spread_hold','spread_implied_prob','spread_datetime','spread_correct','spread_result','carmelo_ml_pick'
                                ,'raptor_ml_pick','carmelo_ml_pick_correct','elo_ml_pick_correct','raptor_ml_pick_correct','carmelo_ml_pick_result'
                                ,'elo_ml_pick_result','raptor_ml_pick_result','open_ml','open_spread','open_spread_odds','ml_open_diff'
                                ,'ml_hold_open_diff','ml_breakeven_open_diff','ml_implied_prob_open_diff','spread_open_diff','spread_hold_open_diff'
                                ,'spread_breakeven_open_diff','spread_implied_prob_open_diff','update_datetime']
) }}

-- AWAY
SELECT 
    GENERATE_UUID() as game_team_odds_sk
    , away_game_team_sk || '|' || odds_period || '|' || odds_maker || '|' || opening_line_flag as unique_key
    , away_game_team_sk as game_team_sk
    , game_date
    , odds_period
    , odds_maker
    , opening_line_flag
    , away_ml as ml
    , away_ml_consensus as ml_consensus
    , away_ml_consensus_wagers as ml_consensus_wagers
    , away_ml_breakeven as ml_breakeven
    , ml_hold
    , away_ml_implied_prob as ml_implied_prob
    , away_ml_breakeven_f_d as ml_breakeven_f_d
    -- , away_ml_implied_f_d as ml_implied_f_d
    , ml_datetime
    , away_ml_correct as ml_correct
    , away_ml_result as ml_result

    , away_spread as spread
    , away_spread_odds as spread_odds
    , away_spread_consensus as spread_consensus
    , away_spread_consensus_wagers as spread_consensus_wagers
    , away_spread_breakeven as spread_breakeven
    , spread_hold
    , away_spread_implied_prob as spread_implied_prob
    , spread_datetime
    , away_spread_correct as spread_correct
    , away_spread_result as spread_result

    , away_carmelo_ml_pick as carmelo_ml_pick
    , away_elo_ml_pick as elo_ml_pick
    , away_raptor_ml_pick as raptor_ml_pick
    --, away_espn_ml_pick as espn_ml_pick
    , away_carmelo_ml_pick_correct as carmelo_ml_pick_correct
    , away_elo_ml_pick_correct as elo_ml_pick_correct
    , away_raptor_ml_pick_correct as raptor_ml_pick_correct
    --, away_espn_ml_pick_correct as espn_ml_pick_correct
    , away_carmelo_ml_pick_result as carmelo_ml_pick_result
    , away_elo_ml_pick_result as elo_ml_pick_result
    , away_raptor_ml_pick_result as raptor_ml_pick_result
    --, away_espn_ml_pick_result as espn_ml_pick_result

    , open_away_ml as open_ml
    , open_away_spread as open_spread
    , open_away_spread_odds as open_spread_odds
    , away_ml_open_diff as ml_open_diff
    , ml_hold_open_diff as ml_hold_open_diff
    , away_ml_breakeven_open_diff as ml_breakeven_open_diff
    , away_ml_implied_prob_open_diff as ml_implied_prob_open_diff
    , away_spread_open_diff as spread_open_diff
    , spread_hold_open_diff as spread_hold_open_diff
    , away_spread_breakeven_open_diff as spread_breakeven_open_diff
    , away_spread_implied_prob_open_diff as spread_implied_prob_open_diff
    , CURRENT_DATETIME() as insert_datetime
    , CURRENT_DATETIME() as update_datetime
FROM {{ ref('nba__transform_stg_game_team_odds') }}
{% if is_incremental() %}
    WHERE game_date >= (SELECT max(game_date) from {{ this }})
{% endif %}

UNION ALL
-- HOME 
SELECT 
    GENERATE_UUID() as game_team_odds_sk
    , home_game_team_sk || '|' || odds_period || '|' || odds_maker || '|' || opening_line_flag as unique_key
    , home_game_team_sk as game_team_sk
    , game_date
    , odds_period
    , odds_maker
    , opening_line_flag
    , home_ml as ml
    , home_ml_consensus as ml_consensus
    , home_ml_consensus_wagers as ml_consensus_wagers
    , home_ml_breakeven as ml_breakeven
    , ml_hold
    , home_ml_implied_prob as ml_implied_prob
    , home_ml_breakeven_f_d as ml_breakeven_f_d
    -- , home_ml_implied_f_d as ml_implied_f_d
    , ml_datetime 
    , home_ml_correct as ml_correct
    , home_ml_result as ml_result

    , home_spread as spread
    , home_spread_odds as spread_odds
    , home_spread_consensus as spread_consensus
    , home_spread_consensus_wagers as spread_consensus_wagers
    , home_spread_breakeven as spread_breakeven
    , spread_hold
    , home_spread_implied_prob as spread_implied_prob
    , spread_datetime
    , home_spread_correct as spread_correct
    , home_spread_result as spread_result

    , home_carmelo_ml_pick as carmelo_ml_pick
    , home_elo_ml_pick as elo_ml_pick
    , home_raptor_ml_pick as raptor_ml_pick
    --, home_espn_ml_pick as espn_ml_pick
    , home_carmelo_ml_pick_correct as carmelo_ml_pick_correct
    , home_elo_ml_pick_correct as elo_ml_pick_correct
    , home_raptor_ml_pick_correct as raptor_ml_pick_correct
    --, home_espn_ml_pick_correct as espn_ml_pick_correct
    , home_carmelo_ml_pick_result as carmelo_ml_pick_result
    , home_elo_ml_pick_result as elo_ml_pick_result
    , home_raptor_ml_pick_result as raptor_ml_pick_result
    --, home_espn_ml_pick_result as espn_ml_pick_result

    , open_home_ml as open_ml
    , open_home_spread as open_spread
    , open_home_spread_odds as open_spread_odds
    , home_ml_open_diff as ml_open_diff
    , ml_hold_open_diff as ml_hold_open_diff
    , home_ml_breakeven_open_diff as ml_breakeven_open_diff
    , home_ml_implied_prob_open_diff as ml_implied_prob_open_diff
    , home_spread_open_diff as spread_open_diff
    , spread_hold_open_diff as spread_hold_open_diff
    , home_spread_breakeven_open_diff as spread_breakeven_open_diff
    , home_spread_implied_prob_open_diff as spread_implied_prob_open_diff
    , CURRENT_DATETIME() as insert_datetime
    , CURRENT_DATETIME() as update_datetime
FROM {{ ref('nba__transform_stg_game_team_odds') }}
{% if is_incremental() %}
    WHERE game_date >= (SELECT max(game_date) from {{ this }})
{% endif %}


