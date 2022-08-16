{{ config(
    tags=["nba"]
) }}

SELECT * 
    , case when complete_flag 
        then 
            case 
                when home_score > away_score then 'H'
                when away_score > home_score then 'A'
                else 'T'
            end
        else 'P'
    end as winner_h_a
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
        ,cast(home_postgame_stats_points as INT64) as home_postgame_stats_points
        ,cast(home_postgame_stats_rebounds_total as INT64) as home_postgame_stats_rebounds_total
        ,cast(home_postgame_stats_assists as INT64) as home_postgame_stats_assists
        ,cast(home_postgame_stats_steals as INT64) as home_postgame_stats_steals
        ,cast(home_postgame_stats_blocks as INT64) as home_postgame_stats_blocks
        ,cast(home_postgame_stats_turnovers as INT64) as home_postgame_stats_turnovers
        ,cast(home_postgame_stats_field_goals_percentage as NUMERIC) as home_postgame_stats_field_goals_percentage
        ,cast(home_postgame_stats_three_pointers_percentage as NUMERIC) as home_postgame_stats_three_pointers_percentage
        ,cast(home_postgame_stats_free_throws_percentage as NUMERIC) as home_postgame_stats_free_throws_percentage
        ,cast(home_postgame_stats_points_in_the_paint as INT64) as home_postgame_stats_points_in_the_paint
        ,cast(home_postgame_stats_points_second_chance as INT64) as home_postgame_stats_points_second_chance
        ,cast(home_postgame_stats_points_fast_break as INT64) as home_postgame_stats_points_fast_break
        ,cast(home_postgame_stats_biggest_lead as INT64) as home_postgame_stats_biggest_lead
        ,cast(home_postgame_stats_lead_changes as INT64) as home_postgame_stats_lead_changes
        ,cast(home_postgame_stats_times_tied as INT64) as home_postgame_stats_times_tied
        ,cast(home_postgame_stats_biggest_scoring_run as INT64) as home_postgame_stats_biggest_scoring_run
        ,cast(home_postgame_stats_turnovers_team as INT64) as home_postgame_stats_turnovers_team
        ,cast(home_postgame_stats_turnovers_total as INT64) as home_postgame_stats_turnovers_total
        ,cast(home_postgame_stats_rebounds_team as INT64) as home_postgame_stats_rebounds_team
        ,cast(home_postgame_stats_points_from_turnovers as INT64) as home_postgame_stats_points_from_turnovers
        ,cast(home_postgame_stats_bench_points as INT64) as home_postgame_stats_bench_points
        ,cast(away_postgame_stats_points as INT64) as away_postgame_stats_points
        ,cast(away_postgame_stats_rebounds_total as INT64) as away_postgame_stats_rebounds_total
        ,cast(away_postgame_stats_assists as INT64) as away_postgame_stats_assists
        ,cast(away_postgame_stats_steals as INT64) as away_postgame_stats_steals
        ,cast(away_postgame_stats_blocks as INT64) as away_postgame_stats_blocks
        ,cast(away_postgame_stats_turnovers as INT64) as away_postgame_stats_turnovers
        ,cast(away_postgame_stats_field_goals_percentage as NUMERIC) as away_postgame_stats_field_goals_percentage
        ,cast(away_postgame_stats_three_pointers_percentage as NUMERIC) as away_postgame_stats_three_pointers_percentage
        ,cast(away_postgame_stats_free_throws_percentage as NUMERIC) as away_postgame_stats_free_throws_percentage
        ,cast(away_postgame_stats_points_in_the_paint as INT64) as away_postgame_stats_points_in_the_paint
        ,cast(away_postgame_stats_points_second_chance as INT64) as away_postgame_stats_points_second_chance
        ,cast(away_postgame_stats_points_fast_break as INT64) as away_postgame_stats_points_fast_break
        ,cast(away_postgame_stats_biggest_lead as INT64) as away_postgame_stats_biggest_lead
        ,cast(away_postgame_stats_lead_changes as INT64) as away_postgame_stats_lead_changes
        ,cast(away_postgame_stats_times_tied as INT64) as away_postgame_stats_times_tied
        ,cast(away_postgame_stats_biggest_scoring_run as INT64) as away_postgame_stats_biggest_scoring_run
        ,cast(away_postgame_stats_turnovers_team as INT64) as away_postgame_stats_turnovers_team
        ,cast(away_postgame_stats_turnovers_total as INT64) as away_postgame_stats_turnovers_total
        ,cast(away_postgame_stats_rebounds_team as INT64) as away_postgame_stats_rebounds_team
        ,cast(away_postgame_stats_points_from_turnovers as INT64) as away_postgame_stats_points_from_turnovers
        ,cast(away_postgame_stats_bench_points as INT64) as away_postgame_stats_bench_points
        ,home_starters_minutes as home_starters_minutes_string
        , cast(round(cast(split(home_starters_minutes,':')[ORDINAL(1)] as INT64) + (cast(split(home_starters_minutes,':')[ORDINAL(2)] as INT64) / 60),3) as NUMERIC) as home_starters_minutes
        , cast((cast(split(home_starters_minutes,':')[ORDINAL(1)] as INT64) * 60) + cast(split(home_starters_minutes,':')[ORDINAL(2)] as INT64) as INT64) as home_starters_seconds
        ,cast(home_starters_field_goals_made as INT64) as home_starters_field_goals_made
        ,cast(home_starters_field_goals_attempted as INT64) as home_starters_field_goals_attempted
        ,cast(home_starters_field_goals_percentage as NUMERIC) as home_starters_field_goals_percentage
        ,cast(home_starters_three_pointers_made as INT64) as home_starters_three_pointers_made
        ,cast(home_starters_three_pointers_attempted as INT64) as home_starters_three_pointers_attempted
        ,cast(home_starters_three_pointers_percentage as NUMERIC) as home_starters_three_pointers_percentage
        ,cast(home_starters_free_throws_made as INT64) as home_starters_free_throws_made
        ,cast(home_starters_free_throws_attempted as INT64) as home_starters_free_throws_attempted
        ,cast(home_starters_free_throws_percentage as NUMERIC) as home_starters_free_throws_percentage
        ,cast(home_starters_rebounds_offensive as INT64) as home_starters_rebounds_offensive
        ,cast(home_starters_rebounds_defensive as INT64) as home_starters_rebounds_defensive
        ,cast(home_starters_rebounds_total as INT64) as home_starters_rebounds_total
        ,cast(home_starters_assists as INT64) as home_starters_assists
        ,cast(home_starters_steals as INT64) as home_starters_steals
        ,cast(home_starters_blocks as INT64) as home_starters_blocks
        ,cast(home_starters_turnovers as INT64) as home_starters_turnovers
        ,cast(home_starters_foulsPersonal as INT64) as home_starters_foulsPersonal
        ,cast(home_starters_points as INT64) as home_starters_points
        , away_starters_minutes as away_starters_minutes_string
        , cast(round(cast(split(away_starters_minutes,':')[ORDINAL(1)] as INT64) + (cast(split(away_starters_minutes,':')[ORDINAL(2)] as INT64) / 60),3) as NUMERIC) as away_starters_minutes
        , cast((cast(split(away_starters_minutes,':')[ORDINAL(1)] as INT64) * 60) + cast(split(away_starters_minutes,':')[ORDINAL(2)] as INT64) as INT64) as away_starters_seconds
        ,cast(away_starters_field_goals_made as INT64) as away_starters_field_goals_made
        ,cast(away_starters_field_goals_attempted as INT64) as away_starters_field_goals_attempted
        ,cast(away_starters_field_goals_percentage as NUMERIC) as away_starters_field_goals_percentage
        ,cast(away_starters_three_pointers_made as INT64) as away_starters_three_pointers_made
        ,cast(away_starters_three_pointers_attempted as INT64) as away_starters_three_pointers_attempted
        ,cast(away_starters_three_pointers_percentage as NUMERIC) as away_starters_three_pointers_percentage
        ,cast(away_starters_free_throws_made as INT64) as away_starters_free_throws_made
        ,cast(away_starters_free_throws_attempted as INT64) as away_starters_free_throws_attempted
        ,cast(away_starters_free_throws_percentage as NUMERIC) as away_starters_free_throws_percentage
        ,cast(away_starters_rebounds_offensive as INT64) as away_starters_rebounds_offensive
        ,cast(away_starters_rebounds_defensive as INT64) as away_starters_rebounds_defensive
        ,cast(away_starters_rebounds_total as INT64) as away_starters_rebounds_total
        ,cast(away_starters_assists as INT64) as away_starters_assists
        ,cast(away_starters_steals as INT64) as away_starters_steals
        ,cast(away_starters_blocks as INT64) as away_starters_blocks
        ,cast(away_starters_turnovers as INT64) as away_starters_turnovers
        ,cast(away_starters_foulsPersonal as INT64) as away_starters_foulsPersonal
        ,cast(away_starters_points as INT64) as away_starters_points
        ,home_bench_minutes as home_bench_minutes_string
        , cast(round(cast(split(home_bench_minutes,':')[ORDINAL(1)] as INT64) + (cast(split(home_bench_minutes,':')[ORDINAL(2)] as INT64) / 60),3) as NUMERIC) as home_bench_minutes
        , cast((cast(split(home_bench_minutes,':')[ORDINAL(1)] as INT64) * 60) + cast(split(home_bench_minutes,':')[ORDINAL(2)] as INT64) as INT64) as home_bench_seconds
         
        ,cast(home_bench_field_goals_made as INT64) as home_bench_field_goals_made
        ,cast(home_bench_field_goals_attempted as INT64) as home_bench_field_goals_attempted
        ,cast(home_bench_field_goals_percentage as NUMERIC) as home_bench_field_goals_percentage
        ,cast(home_bench_three_pointers_made as INT64) as home_bench_three_pointers_made
        ,cast(home_bench_three_pointers_attempted as INT64) as home_bench_three_pointers_attempted
        ,cast(home_bench_three_pointers_percentage as NUMERIC) as home_bench_three_pointers_percentage
        ,cast(home_bench_free_throws_made as INT64) as home_bench_free_throws_made
        ,cast(home_bench_free_throws_attempted as INT64) as home_bench_free_throws_attempted
        ,cast(home_bench_free_throws_percentage as NUMERIC) as home_bench_free_throws_percentage
        ,cast(home_bench_rebounds_offensive as INT64) as home_bench_rebounds_offensive
        ,cast(home_bench_rebounds_defensive as INT64) as home_bench_rebounds_defensive
        ,cast(home_bench_rebounds_total as INT64) as home_bench_rebounds_total
        ,cast(home_bench_assists as INT64) as home_bench_assists
        ,cast(home_bench_steals as INT64) as home_bench_steals
        ,cast(home_bench_blocks as INT64) as home_bench_blocks
        ,cast(home_bench_turnovers as INT64) as home_bench_turnovers
        ,cast(home_bench_foulsPersonal as INT64) as home_bench_foulsPersonal
        ,cast(home_bench_points as INT64) as home_bench_points
        ,away_bench_minutes as away_bench_minutes_string
        , cast(round(cast(split(away_bench_minutes,':')[ORDINAL(1)] as INT64) + (cast(split(away_bench_minutes,':')[ORDINAL(2)] as INT64) / 60),3) as NUMERIC) as away_bench_minutes
        , cast((cast(split(away_bench_minutes,':')[ORDINAL(1)] as INT64) * 60) + cast(split(away_bench_minutes,':')[ORDINAL(2)] as INT64) as INT64) as away_bench_seconds
         
        ,cast(away_bench_field_goals_made as INT64) as away_bench_field_goals_made
        ,cast(away_bench_field_goals_attempted as INT64) as away_bench_field_goals_attempted
        ,cast(away_bench_field_goals_percentage as NUMERIC) as away_bench_field_goals_percentage
        ,cast(away_bench_three_pointers_made as INT64) as away_bench_three_pointers_made
        ,cast(away_bench_three_pointers_attempted as INT64) as away_bench_three_pointers_attempted
        ,cast(away_bench_three_pointers_percentage as NUMERIC) as away_bench_three_pointers_percentage
        ,cast(away_bench_free_throws_made as INT64) as away_bench_free_throws_made
        ,cast(away_bench_free_throws_attempted as INT64) as away_bench_free_throws_attempted
        ,cast(away_bench_free_throws_percentage as NUMERIC) as away_bench_free_throws_percentage
        ,cast(away_bench_rebounds_offensive as INT64) as away_bench_rebounds_offensive
        ,cast(away_bench_rebounds_defensive as INT64) as away_bench_rebounds_defensive
        ,cast(away_bench_rebounds_total as INT64) as away_bench_rebounds_total
        ,cast(away_bench_assists as INT64) as away_bench_assists
        ,cast(away_bench_steals as INT64) as away_bench_steals
        ,cast(away_bench_blocks as INT64) as away_bench_blocks
        ,cast(away_bench_turnovers as INT64) as away_bench_turnovers
        ,cast(away_bench_foulsPersonal as INT64) as away_bench_foulsPersonal
        ,cast(away_bench_points as INT64) as away_bench_points
        ,load_datetime
        --, row_number() over (partition by game_key order by load_datetime desc) as dedup
    FROM {{ source('nba_raw','raw_nbacom_game') }}
    QUALIFY row_number() over (partition by game_key order by load_datetime desc) = 1
    ) a


