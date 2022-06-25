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
    , merge_update_columns = ['total','total_over_odds','total_under_odds','total_over_consensus','total_under_consensus','total_over_consensus_wagers'
                                ,'total_under_consensus_wagers','total_over_breakeven','total_under_breakeven','total_hold','total_over_implied_prob'
                                ,'total_under_implied_prob','total_datetime','open_total','open_total_over_odds','open_total_under_odds'
                                ,'total_open_diff','total_over_breakeven_open_diff','total_under_breakeven_open_diff','total_over_implied_prob_open_diff'
                                ,'total_under_implied_prob_open_diff''update_datetime']
) }}
    -----------------------------------------------------------
    -- Game Team Odds (ML/Spread)
    -----------------------------------------------------------


    

        MERGE nba.model_game_team_odds t
        USING 
            (
            -- AWAY
            SELECT 
                GENERATE_UUID() as game_team_odds_sk
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
                , away_espn_ml_pick as espn_ml_pick
                , away_carmelo_ml_pick_correct as carmelo_ml_pick_correct
                , away_elo_ml_pick_correct as elo_ml_pick_correct
                , away_raptor_ml_pick_correct as raptor_ml_pick_correct
                , away_espn_ml_pick_correct as espn_ml_pick_correct
                , away_carmelo_ml_pick_result as carmelo_ml_pick_result
                , away_elo_ml_pick_result as elo_ml_pick_result
                , away_raptor_ml_pick_result as raptor_ml_pick_result
                , away_espn_ml_pick_result as espn_ml_pick_result

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
            FROM TMP_NBA_GAME_TEAM_ODDS
            UNION ALL
            -- HOME 
            SELECT 
                GENERATE_UUID() as game_team_odds_sk
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
                , home_espn_ml_pick as espn_ml_pick
                , home_carmelo_ml_pick_correct as carmelo_ml_pick_correct
                , home_elo_ml_pick_correct as elo_ml_pick_correct
                , home_raptor_ml_pick_correct as raptor_ml_pick_correct
                , home_espn_ml_pick_correct as espn_ml_pick_correct
                , home_carmelo_ml_pick_result as carmelo_ml_pick_result
                , home_elo_ml_pick_result as elo_ml_pick_result
                , home_raptor_ml_pick_result as raptor_ml_pick_result
                , home_espn_ml_pick_result as espn_ml_pick_result

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
            FROM TMP_NBA_GAME_TEAM_ODDS
            ) s 
            on t.game_team_sk = s.game_team_sk
            and t.odds_period = s.odds_period
            and t.odds_maker = s.odds_maker
            and t.opening_line_flag = s.opening_line_flag
        WHEN MATCHED THEN
            UPDATE SET 
                t.ml = s.ml
                , t.ml_consensus = s.ml_consensus
                , t.ml_consensus_wagers = s.ml_consensus_wagers
                , t.ml_breakeven = s.ml_breakeven
                , t.ml_hold = s.ml_hold
                , t.ml_implied_prob = s.ml_implied_prob
                , t.ml_breakeven_f_d = s.ml_breakeven_f_d
                , t.ml_datetime = s.ml_datetime
                , t.ml_correct = s.ml_correct
                , t.ml_result = s.ml_result
                , t.spread = s.spread
                , t.spread_odds = s.spread_odds
                , t.spread_consensus = s.spread_consensus
                , t.spread_consensus_wagers = s.spread_consensus_wagers
                , t.spread_breakeven = s.spread_breakeven
                , t.spread_hold = s.spread_hold
                , t.spread_implied_prob = s.spread_implied_prob
                , t.spread_datetime = s.spread_datetime
                , t.spread_correct = s.spread_correct
                , t.spread_result = s.spread_result
                , t.carmelo_ml_pick = s.carmelo_ml_pick
                , t.elo_ml_pick = s.elo_ml_pick
                , t.raptor_ml_pick = s.raptor_ml_pick
                , t.espn_ml_pick = s.espn_ml_pick
                , t.carmelo_ml_pick_correct = s.carmelo_ml_pick_correct
                , t.elo_ml_pick_correct = s.elo_ml_pick_correct
                , t.raptor_ml_pick_correct = s.raptor_ml_pick_correct
                , t.espn_ml_pick_correct = s.espn_ml_pick_correct
                , t.carmelo_ml_pick_result = s.carmelo_ml_pick_result
                , t.elo_ml_pick_result = s.elo_ml_pick_result
                , t.raptor_ml_pick_result = s.raptor_ml_pick_result
                , t.espn_ml_pick_result = s.espn_ml_pick_result

                , t.open_ml = s.open_ml
                , t.open_spread = s.open_spread
                , t.open_spread_odds = s.open_spread_odds
                , t.ml_open_diff = s.ml_open_diff
                , t.ml_hold_open_diff = s.ml_hold_open_diff
                , t.ml_breakeven_open_diff = s.ml_breakeven_open_diff
                , t.ml_implied_prob_open_diff = s.ml_implied_prob_open_diff
                , t.spread_open_diff = s.spread_open_diff
                , t.spread_hold_open_diff = s.spread_hold_open_diff
                , t.spread_breakeven_open_diff = s.spread_breakeven_open_diff
                , t.spread_implied_prob_open_diff = s.spread_implied_prob_open_diff
                , t.update_datetime = CURRENT_DATETIME()
        WHEN NOT MATCHED THEN
            INSERT (game_team_odds_sk, game_team_sk, game_date, odds_period, odds_maker, opening_line_flag
                    , ml, ml_consensus, ml_consensus_wagers, ml_breakeven, ml_hold
                    , ml_implied_prob, ml_breakeven_f_d, ml_datetime, ml_correct, ml_result
                    , spread, spread_odds, spread_consensus, spread_consensus_wagers
                    , spread_breakeven, spread_hold, spread_implied_prob, spread_datetime, spread_correct, spread_result
                    , carmelo_ml_pick, elo_ml_pick, raptor_ml_pick, espn_ml_pick
                    , carmelo_ml_pick_correct, elo_ml_pick_correct, raptor_ml_pick_correct, espn_ml_pick_correct
                    , carmelo_ml_pick_result, elo_ml_pick_result, raptor_ml_pick_result, espn_ml_pick_result
                    , open_ml, open_spread, open_spread_odds
                    , ml_open_diff, ml_hold_open_diff, ml_breakeven_open_diff, ml_implied_prob_open_diff
                    , spread_open_diff, spread_hold_open_diff, spread_breakeven_open_diff, spread_implied_prob_open_diff
                    , insert_datetime)
            VALUES(game_team_odds_sk, game_team_sk, game_date, odds_period, odds_maker, opening_line_flag
                    , ml, ml_consensus, ml_consensus_wagers, ml_breakeven, ml_hold
                    , ml_implied_prob, ml_breakeven_f_d, ml_datetime, ml_correct, ml_result
                    , spread, spread_odds, spread_consensus, spread_consensus_wagers
                    , spread_breakeven, spread_hold, spread_implied_prob, spread_datetime, spread_correct, spread_result
                    , carmelo_ml_pick, elo_ml_pick, raptor_ml_pick, espn_ml_pick
                    , carmelo_ml_pick_correct, elo_ml_pick_correct, raptor_ml_pick_correct, espn_ml_pick_correct
                    , carmelo_ml_pick_result, elo_ml_pick_result, raptor_ml_pick_result, espn_ml_pick_result
                    , open_ml, open_spread, open_spread_odds
                    , ml_open_diff, ml_hold_open_diff, ml_breakeven_open_diff, ml_implied_prob_open_diff
                    , spread_open_diff, spread_hold_open_diff, spread_breakeven_open_diff, spread_implied_prob_open_diff
                    , insert_datetime)
        ;

    DROP TABLE TMP_NBA_GAME_TEAM_ODDS;
     


END;