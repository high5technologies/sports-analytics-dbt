{{ config(
    tags=["nba"]
) }}

SELECT *
FROM 
    (SELECT  
        game_inactive_key
        ,game_id
        ,team_id
        ,h_a
        ,game_date
        ,person_id
        ,first_name
        ,family_name
        ,jersey_num
        ,load_datetime
        , row_number() over (partition by game_inactive_key order by load_datetime desc) as dedup
    FROM {{ source('nba_raw','raw_nbacom_game_inactive') }}
    ) a
WHERE dedup = 1

