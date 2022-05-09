{{ config(
    tags=["nba"]
) }}

SELECT * 
FROM 
    (SELECT         
        game_key
        ,game_id
        ,url_id
        , cast(season as INT64) + 1 as season
        ,season_type
        ,1 as game_bit
        ,game_date
        ,game_code
        ,game_status
        ,game_status_text
        ,period
        ,case when game_status = "3" then TRUE else FALSE end as complete_flag
        ,case when game_status = "3" and cast(period as INT64) > 4 then TRUE else FALSE end as ot_flag
        ,case when game_status = "3" and cast(period as INT64) > 4 then cast(period as INT64) - 4 end as ot_count
        ,game_clock
        ,game_time_utc
        ,game_et
        , PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', game_time_utc) as game_timestamp_utc
        , DATETIME(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', game_time_utc), "America/Chicago") as game_datetime_central
        , time(DATETIME(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', game_time_utc), "America/Chicago")) as game_start_time
        ,away_team_id
        ,home_team_id
        ,duration as game_duration 
        ,(cast(split(duration,':')[offset(0)] as INT64) * 60) + cast(split(duration,':')[offset(1)] as INT64) as game_duration_minutes
        ,cast(attendance as INT64) as attendance
        ,case when sellout = "1" then TRUE else FALSE end as sellout
        ,cast(replace(nullif(series_game_number,''), 'Game ','') as INT64) as series_game_number
        ,series_text
        ,if_necessary
        ,arena_id
        ,arena_name
        ,arena_city
        ,arena_state
        ,arena_country
        ,arena_timezone
        ,arena_street_address
        ,arena_postal_code
        ,national_broadcaster_id
        ,national_broadcast_display
        ,home_team_name
        ,home_team_city
        ,home_team_tricode
        ,home_team_slug
        ,cast(home_team_wins as INT64) as home_team_wins
        ,cast(home_team_losses as INT64) as home_team_losses
        ,cast(home_score as INT64) as home_score
        ,home_in_bonus
        ,cast(home_seed as INT64) as home_seed
        ,home_statistics
        ,home_timeouts_remaining
        ,away_team_name
        ,away_team_city
        ,away_team_tricode
        ,away_team_slug
        ,cast(away_team_wins as INT64) as away_team_wins
        ,cast(away_team_losses as INT64) as away_team_losses
        ,cast(away_score as INT64) as away_score
        ,away_in_bonus
        ,cast(away_seed as INT64) as away_seed
        ,away_statistics
        ,away_timeouts_remaining
        ,home_pregame_stats_points
        ,home_pregame_stats_rebounds_total
        ,home_pregame_stats_assists
        ,home_pregame_stats_steals
        ,home_pregame_stats_blocks
        ,home_pregame_stats_turnovers
        ,home_pregame_stats_field_goals_percentage
        ,home_pregame_stats_three_pointers_percentage
        ,home_pregame_stats_free_throws_percentage
        ,home_pregame_stats_points_in_the_paint
        ,home_pregame_stats_points_second_chance
        ,home_pregame_stats_points_fast_break
        ,away_pregame_stats_points
        ,away_pregame_stats_rebounds_total
        ,away_pregame_stats_assists
        ,away_pregame_stats_steals
        ,away_pregame_stats_blocks
        ,away_pregame_stats_turnovers
        ,away_pregame_stats_field_goals_percentage
        ,away_pregame_stats_three_pointers_percentage
        ,away_pregame_stats_free_throws_percentage
        ,away_pregame_stats_points_in_the_paint
        ,away_pregame_stats_points_second_chance
        ,away_pregame_stats_points_fast_break
        ,home_postgame_stats_points
        ,home_postgame_stats_rebounds_total
        ,home_postgame_stats_assists
        ,home_postgame_stats_steals
        ,home_postgame_stats_blocks
        ,home_postgame_stats_turnovers
        ,home_postgame_stats_field_goals_percentage
        ,home_postgame_stats_three_pointers_percentage
        ,home_postgame_stats_free_throws_percentage
        ,home_postgame_stats_points_in_the_paint
        ,home_postgame_stats_points_second_chance
        ,home_postgame_stats_points_fast_break
        ,home_postgame_stats_biggest_lead
        ,home_postgame_stats_lead_changes
        ,home_postgame_stats_times_tied
        ,home_postgame_stats_biggest_scoring_run
        ,home_postgame_stats_turnovers_team
        ,home_postgame_stats_turnovers_total
        ,home_postgame_stats_rebounds_team
        ,home_postgame_stats_points_from_turnovers
        ,home_postgame_stats_bench_points
        ,away_postgame_stats_points
        ,away_postgame_stats_rebounds_total
        ,away_postgame_stats_assists
        ,away_postgame_stats_steals
        ,away_postgame_stats_blocks
        ,away_postgame_stats_turnovers
        ,away_postgame_stats_field_goals_percentage
        ,away_postgame_stats_three_pointers_percentage
        ,away_postgame_stats_free_throws_percentage
        ,away_postgame_stats_points_in_the_paint
        ,away_postgame_stats_points_second_chance
        ,away_postgame_stats_points_fast_break
        ,away_postgame_stats_biggest_lead
        ,away_postgame_stats_lead_changes
        ,away_postgame_stats_times_tied
        ,away_postgame_stats_biggest_scoring_run
        ,away_postgame_stats_turnovers_team
        ,away_postgame_stats_turnovers_total
        ,away_postgame_stats_rebounds_team
        ,away_postgame_stats_points_from_turnovers
        ,away_postgame_stats_bench_points
        ,home_starters_minutes
        ,home_starters_field_goals_made
        ,home_starters_field_goals_attempted
        ,home_starters_field_goals_percentage
        ,home_starters_three_pointers_made
        ,home_starters_three_pointers_attempted
        ,home_starters_three_pointers_percentage
        ,home_starters_free_throws_made
        ,home_starters_free_throws_attempted
        ,home_starters_free_throws_percentage
        ,home_starters_rebounds_offensive
        ,home_starters_rebounds_defensive
        ,home_starters_rebounds_total
        ,home_starters_assists
        ,home_starters_steals
        ,home_starters_blocks
        ,home_starters_turnovers
        ,home_starters_foulsPersonal
        ,home_starters_points
        ,away_starters_minutes
        ,away_starters_field_goals_made
        ,away_starters_field_goals_attempted
        ,away_starters_field_goals_percentage
        ,away_starters_three_pointers_made
        ,away_starters_three_pointers_attempted
        ,away_starters_three_pointers_percentage
        ,away_starters_free_throws_made
        ,away_starters_free_throws_attempted
        ,away_starters_free_throws_percentage
        ,away_starters_rebounds_offensive
        ,away_starters_rebounds_defensive
        ,away_starters_rebounds_total
        ,away_starters_assists
        ,away_starters_steals
        ,away_starters_blocks
        ,away_starters_turnovers
        ,away_starters_foulsPersonal
        ,away_starters_points
        ,home_bench_minutes
        ,home_bench_field_goals_made
        ,home_bench_field_goals_attempted
        ,home_bench_field_goals_percentage
        ,home_bench_three_pointers_made
        ,home_bench_three_pointers_attempted
        ,home_bench_three_pointers_percentage
        ,home_bench_free_throws_made
        ,home_bench_free_throws_attempted
        ,home_bench_free_throws_percentage
        ,home_bench_rebounds_offensive
        ,home_bench_rebounds_defensive
        ,home_bench_rebounds_total
        ,home_bench_assists
        ,home_bench_steals
        ,home_bench_blocks
        ,home_bench_turnovers
        ,home_bench_foulsPersonal
        ,home_bench_points
        ,away_bench_minutes
        ,away_bench_field_goals_made
        ,away_bench_field_goals_attempted
        ,away_bench_field_goals_percentage
        ,away_bench_three_pointers_made
        ,away_bench_three_pointers_attempted
        ,away_bench_three_pointers_percentage
        ,away_bench_free_throws_made
        ,away_bench_free_throws_attempted
        ,away_bench_free_throws_percentage
        ,away_bench_rebounds_offensive
        ,away_bench_rebounds_defensive
        ,away_bench_rebounds_total
        ,away_bench_assists
        ,away_bench_steals
        ,away_bench_blocks
        ,away_bench_turnovers
        ,away_bench_foulsPersonal
        ,away_bench_points
        ,load_datetime
        , row_number() over (partition by game_key order by load_datetime desc) as dedup
    FROM {{ source('nba_raw','raw_nbacom_game') }}
    ) a
WHERE dedup = 1

