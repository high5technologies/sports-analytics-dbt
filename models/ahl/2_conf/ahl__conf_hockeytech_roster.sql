{{ config(
    tags=["ahl"]
) }}

SELECT *
    ,case 
        when birth_place_location in ('AB','BC','NB','NL','NS','ON','PEI','QC','SK','MB','PE','YT','NT','NU') then 'Canada' 
        when length(birth_place_location) <= 2 then 'USA' 
        else birth_place_location 
    end as birth_place_country
FROM 
    (SELECT 
        roster_key
        , cast(season as int64) as season
        , cast(team_id as int64) as team_id
        , cast(player_id as int64) as player_id
        , name
        , cast(jersey_number as int64) as jersey_number
        , position
        , cast(birthdate as date) as birth_date
        , birthplace as birth_place
        , nullif(shoots,'') as shoots
        , nullif(catches,'') as catches
        , nullif(height,'') as height
        , case when instr(height,'-') > 0 then (cast(split(height,'-')[ORDINAL(1)] as INT64) * 12)
            + cast(split(height,'-')[ORDINAL(2)] as INT64) end as height_inches
        , SAFE_CAST(weight as INT64) as weight
        , case when instr(birthplace,',') > 0 then trim(split(replace(birthplace,'.',','),',')[ORDINAL(1)]) end as birth_place_city
        , case when instr(birthplace,',') > 0 then trim(split(replace(birthplace,'.',','),',')[ORDINAL(2)]) end as birth_place_location
        , row_number() over (partition by roster_key order by load_datetime desc) as dedup
        , load_datetime
    FROM {{ source('ahl_raw','raw_hockeytech_roster') }}
    ) a
WHERE dedup = 1
