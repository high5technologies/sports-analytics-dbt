{{ config(
    tags=["nba"]
) }}

SELECT 
    g.game_sk
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
FROM {{ ref('nba__trusted_game') }} g
    inner join {{ ref('nba__trusted_game_team') }} gt
        on g.game_sk = gt.game_sk
    inner join {{ ref('nba__trusted_arena') }} a 
        on g.arena_sk = a.arena_sk
