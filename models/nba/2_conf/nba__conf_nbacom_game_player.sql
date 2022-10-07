{{ config(
    tags=["nba"]
) }}

SELECT  
    game_player_key
    ,game_id
    ,team_id
    ,h_a
    ,person_id as player_id_nbacom
    ,game_date
    ,upper(first_name || ' ' || family_name) as player_name
    ,upper(first_name) as player_first_name
    ,upper(family_name) as player_last_name
    ,name_i as player_name_i
    ,player_slug
    ,position
    ,case when position = '' then FALSE when position != '' then TRUE end as starter_flag
    ,comment
    ,jersey_num
    ,minutes as minutes_string
    ,case when minutes = '' then 0 else (cast(split(minutes,':')[ORDINAL(1)] as INT64) * 60) + cast(split(minutes,':')[ORDINAL(2)] as INT64) end as seconds_played
    ,case when minutes = '' then 0 else (cast(split(minutes,':')[ORDINAL(1)] as INT64)) + (cast(split(minutes,':')[ORDINAL(2)] as INT64) / 60) end as minutes_played	
    ,cast(field_goals_made as INT64) as field_goals_made
    ,cast(field_goals_attempted as INT64) as field_goals_attempted
    ,cast(field_goals_percentage as NUMERIC) as field_goals_percentage
    ,cast(three_pointers_made as INT64) as three_pointers_made
    ,cast(three_pointers_attempted as INT64) as three_pointers_attempted
    ,cast(three_pointers_percentage as NUMERIC) as three_pointers_percentage
    ,cast(free_throws_made as INT64) as free_throws_made
    ,cast(free_throws_attempted as INT64) as free_throws_attempted
    ,cast(free_throws_percentage as NUMERIC) as free_throws_percentage
    ,cast(rebounds_defensive as INT64) as rebounds_defensive
    ,cast(rebounds_total as INT64) as rebounds_total
    ,cast(assists as INT64) as assists
    ,cast(steals as INT64) as steals
    ,cast(blocks as INT64) as blocks
    ,cast(turnovers as INT64) as turnovers
    ,cast(fouls_personal as INT64) as fouls_personal
    ,cast(points as INT64) as points
    ,cast(plus_minus_points as INT64) as plus_minus_points
    ,load_datetime
    --, row_number() over (partition by game_player_key order by load_datetime desc) as dedup
FROM {{ source('nba_raw','raw_nbacom_game_player') }}
QUALIFY row_number() over (partition by game_player_key order by load_datetime desc) = 1
