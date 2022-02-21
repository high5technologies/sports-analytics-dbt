{{ config(
    tags=["ahl"]
) }}

SELECT * 
FROM 
    (SELECT 
         goalie_box_key
         , cast(game_key as INT64) as game_key
         , game_date
        , cast(goalie_id as INT64) as goalie_id
        , first_name as player_first_name
        , last_name as player_last_name
        , concat(first_name, ' ', last_name) as player_name
        , cast(birth_date as date) as birth_date
        , cast(jersey_number as INT64) as jersey_number
        , position
        , nullif(status,'') as captain_status
        , h_a
        , case starting when '1' then true when '0' then false end as starting_flag
        , cast(goals_against as INT64) as goals_against
        , cast(saves as INT64) as saves
        , cast(shots_against as INT64) as shots_against
        , time_on_ice 
        , (cast(split(time_on_ice,':')[ORDINAL(1)] as INT64) * 60) + cast(split(time_on_ice,':')[ORDINAL(2)] as INT64) as  time_on_ice_seconds
        , cast(points as INT64) as points
        , cast(plus_minus as INT64) as plus_minus
        , cast(pim as INT64) as pim
        , cast(goals as INT64) as goals
        , cast(assists as INT64) as assists
        , row_number() over (partition by goalie_box_key order by load_datetime desc) as dedup
    FROM {{ source('ahl_raw','raw_hockeytech_goaliebox') }}
    ) a
WHERE dedup = 1