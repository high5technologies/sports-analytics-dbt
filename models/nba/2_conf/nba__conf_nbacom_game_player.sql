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
    ,minutes
    ,field_goals_made
    ,field_goals_attempted
    ,field_goals_percentage
    ,three_pointers_made
    ,three_pointers_attempted
    ,three_pointers_percentage
    ,free_throws_made
    ,free_throws_attempted
    ,free_throws_percentage
    ,rebounds_offensive
    ,rebounds_defensive
    ,rebounds_total
    ,assists
    ,steals
    ,blocks
    ,turnovers
    ,fouls_personal
    ,points
    ,plus_minus_points
    ,load_datetime
    --, row_number() over (partition by game_player_key order by load_datetime desc) as dedup
FROM {{ source('nba_raw','raw_nbacom_game_player') }}
QUALIFY row_number() over (partition by game_player_key order by load_datetime desc) = 1
