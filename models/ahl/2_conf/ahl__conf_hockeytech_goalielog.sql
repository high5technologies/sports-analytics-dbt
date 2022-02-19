{{ config(
    tags=["ahl"]
) }}

SELECT * 
FROM 
    (SELECT *
        , row_number() over (partition by goalie_log_key order by load_datetime desc) as dedup
    FROM {{ source('ahl_raw','raw_hockeytech_goalielog') }}
    ) a
WHERE dedup = 1