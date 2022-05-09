{{ config(
    tags=["nba"]
) }}

SELECT *
FROM 
    (SELECT  
        game_official_key
        ,game_id
        ,person_id
        ,game_date
        ,name
        ,name_i
        ,first_name
        ,family_name
        ,jersey_num
        ,assignment
        ,load_datetime
        , row_number() over (partition by game_official_key order by load_datetime desc) as dedup
    FROM {{ source('nba_raw','raw_nbacom_game_official') }}
    ) a
WHERE dedup = 1
