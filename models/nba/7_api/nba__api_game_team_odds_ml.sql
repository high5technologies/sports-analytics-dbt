{{ config(
    tags=["nba"]
) }}

SELECT 
    unique_key
    --, game_sk
    --, game_team_sk
    --, arena_sk
    , game_date
    --, arena_name
    --, arena_city
    --, arena_state
    --, arena_country
    --, arena_timezone
    --, game_yearmonth
    , game_yearmonth_formatted as game_yearmonth
    , game_week
    , season
    , season_type
    --, team
    , team_abbr
    --, division
    --, conference
    --, opp_team
    , opp_team_abbr
    --, opp_division
    --, opp_conference
    , inter_division
    , inter_conference
    --, game_status_text
    --, game_timestamp_utc
    --, game_datetime_central
    --, game_start_time
    --, attendance
    --, complete_flag
    --, ot_flag
    --, ot_count
    --, game_bit
    --, sellout
    --, series_game_number
    --, series_text
    --, if_necessary
    --, national_broadcast_display
    --, total_score_game
    , h_a
    , team_score
    , opp_score
    , w_l
    , wins
    , losses
    , score_diff_game
    --, team_seed
    , elo_f_d
    , raptor_f_d
    , elo_worth_game
    , elo_win_game
    , raptor_worth_game
    , raptor_win_game
 

    --, odds_period
    --, odds_maker
    --, opening_line_flag
    , ml
    , ml_consensus
    , ml_consensus_wagers
    , ml_breakeven
    , ml_hold
    , ml_implied_prob
    , ml_breakeven_f_d
    -- , ml_implied_f_d
    --, ml_datetime
    , ml_correct
    , ml_result

    --, spread
    --, spread_odds
    --, spread_consensus
    --, spread_consensus_wagers
    --, spread_breakeven
    --, spread_hold
    --, spread_implied_prob
    --, spread_datetime
    --, spread_correct
    --, spread_result

    --, carmelo_ml_pick
    , elo_ml_pick
    , raptor_ml_pick
    --, espn_ml_pick
    --, carmelo_ml_pick_correct
    , elo_ml_pick_correct
    , raptor_ml_pick_correct
    --, espn_ml_pick_correct
    --, carmelo_ml_pick_result
    , elo_ml_pick_result
    , raptor_ml_pick_result
    --, espn_ml_pick_result

    --, open_ml
    --, open_spread
    --, open_spread_odds
    --, ml_open_diff
    --, ml_hold_open_diff
    --, ml_breakeven_open_diff
    --, ml_implied_prob_open_diff
    --, spread_open_diff
    --, spread_hold_open_diff
    --, spread_breakeven_open_diff
    --, spread_implied_prob_open_diff
FROM {{ ref('nba__analytics_game_team_odds')}}
