{{ config(
    tags=["ahl"]
) }}

SELECT * 
FROM 
    (SELECT  
        game_coach_key
        , cast(game_key as INT64) as game_key
        , game_date, h_a
        , first_name as coach_first_name
        , last_name as coach_last_name
        , concat(first_name, ' ', last_name) as coach_name
        , team as team_abbr
        , upper(role) as role
        , case when upper(role) in ('HEAD COACH','CO-COACH') then true else false end as head_coach_flag
		, case when upper(role) not in ('HEAD COACH','CO-COACH') then true else false end as assistant_coach_flag
        , load_datetime
        , row_number() over (partition by game_coach_key order by load_datetime desc) as dedup
    FROM {{ source('ahl_raw','raw_hockeytech_coach') }}
    ) a
WHERE dedup = 1