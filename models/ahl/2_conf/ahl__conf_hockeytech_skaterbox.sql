{{ config(
    tags=["ahl"]
) }}

SELECT * 
FROM 
    (SELECT skater_box_key
        , cast(game_key as INT64) as game_key
        , game_date
        , cast(skater_id as INT64) as skater_id
        , first_name as player_first_name
        , last_name as player_last_name
        , concat(first_name, ' ', last_name) as player_name
        , cast(birth_date as date) as birth_date
        , cast(jersey_number as INT64) as jersey_number
        , position
        , nullif(status,'') as captain_status
        , h_a
        , case starting when '1' then true when '0' then false end as starting_flag
        , cast(goals as INT64) as goals
        , cast(assists as INT64) as assists
        , cast(shots as INT64) as shots
        , cast(points as INT64) as points
        , cast(hits as INT64) as hits
        , cast(pim as INT64) as pim
        , cast(plus_minus as INT64) as plus_minus
        , row_number() over (partition by skater_box_key order by load_datetime desc) as dedup
        , load_datetime
    FROM {{ source('ahl_raw','raw_hockeytech_skaterbox') }}
    ) a
WHERE dedup = 1

    