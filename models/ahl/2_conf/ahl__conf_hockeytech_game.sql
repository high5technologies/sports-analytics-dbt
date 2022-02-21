{{ config(
    tags=["ahl"]
) }}

SELECT * 
FROM 
    (SELECT 
        cast(game_key as INT64) as game_key
        , cast(game_key as INT64) as game_id
        , game_date
        , season
        , season_index
        , season_type
        --, case when EXTRACT(MONTH FROM game_date) > 7 then EXTRACT(YEAR FROM game_date) + 1 else EXTRACT(YEAR FROM game_date) end as season
        --, 'R' as season_type -- HARD CODED FOR NOW .. ADD season_type to PYTHON scraper based on date range
        , PARSE_TIME("%I:%M %p", upper(nullif(start_time,''))) as game_start_time
        , PARSE_TIME("%I:%M %p", upper(nullif(end_time,''))) as game_end_time
        , duration as game_duration
        , (cast(split(nullif(duration,''),':')[ORDINAL(1)] as INT64) * 60) + cast(split(nullif(duration,''),':')[ORDINAL(2)] as INT64) as game_duration_minutes
        , cast(replace(attendance,',','') as INT64) as attendance
        , case when started = '1' then TRUE else FALSE end as started_flag
        , case when final = '1' then TRUE else FALSE end as complete_flag
        , case when status in ('Final OT','Final SO') then TRUE else FALSE end as ot_flag
        , status
        , has_shootout as so_flag
        , venue
        , 1 as game_bit
        , cast(away_team_id as INT64) as away_team_id
		, away_team_abbrev as away_team_abbr, away_team_city, away_team_name
        , cast(home_team_id as INT64) as home_team_id
		, home_team_abbrev as home_team_abbr, home_team_city, home_team_name
        , cast(away_goals as INT64) as away_goals
		, cast(away_shots as INT64) as away_shots
		, cast(away_assists as INT64) as away_assists
		, cast(away_hits as INT64) as away_hits
		, cast(away_infractions as INT64) as away_infractions
		, cast(away_pim as INT64) as away_pim
		, cast(away_ppgoals as INT64) as away_ppgoals
		, cast(away_ppopps as INT64) as away_ppopps
        , cast(home_goals as INT64) as home_goals
		, cast(home_shots as INT64) as home_shots
		, cast(home_assists as INT64) as home_assists
		, cast(home_hits as INT64) as home_hits
		, cast(home_infractions as INT64) as home_infractions
		, cast(home_pim as INT64) as home_pim
		, cast(home_ppgoals as INT64) as home_ppgoals
		, cast(home_ppopps as INT64) as home_ppopps
        , row_number() over (partition by game_key order by load_datetime desc) as dedup
    FROM {{ source('ahl_raw','raw_hockeytech_game') }}
    ) a
WHERE dedup = 1

