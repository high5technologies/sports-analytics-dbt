{{ config(
    tags=["nba"]
) }}

SELECT 
    unique_key
    --, a.arena_name
    --, a.arena_city
    --, a.arena_state
    --, a.arena_country
    --, a.arena_timezone
    --, game_yearmonth
    , game_yearmonth_formatted as game_yearmonth
    , game_week
    , season
    , season_type
    --, team
    , team_abbr
    --, division
    --, conference
    , team as opp_team
    , team_abbr as opp_team_abbr
    --, division as opp_division
    --, conference as opp_conference
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
    --, win_bit
    --, score_diff_game
    --, team_seed
    , elo_f_d
    , raptor_f_d
    , elo_worth_game
    , elo_win_game
    , raptor_worth_game
    , raptor_win_game
FROM {{ ref('nba__analytics_game_by_team')}}