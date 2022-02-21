{{ config(
    tags=["ahl"]
) }}

SELECT * 
FROM 
    (SELECT 
        game_ref_key
        , cast(game_key as INT64) as game_key
        , game_date
        , cast(jersey_number as INT64) as jersey_number
        , first_name as ref_first_name
        , last_name as ref_last_name
        , concat(first_name, ' ', last_name) as ref_name
        , upper(role) as role
        , case when upper(role) in ('REFEREE') then true else false end as referee_flag
		, case when upper(role) not in ('REFEREE') then true else false end as linesman_flag
        , load_datetime
        , row_number() over (partition by game_ref_key order by load_datetime desc) as dedup
    FROM {{ source('ahl_raw','raw_hockeytech_ref') }}
    ) a
WHERE dedup = 1


		