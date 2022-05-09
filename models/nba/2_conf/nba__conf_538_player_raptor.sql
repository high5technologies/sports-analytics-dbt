{{ config(
    tags=["nba"]
) }}

SELECT * 
FROM
    (SELECT  
        fivethirtyeight_key
        ,player_name
        ,player_id
        ,season
        ,season_type
        ,team
        ,cast(poss as INT64) as poss
        ,cast(mp as INT64) as mp
        ,cast(raptor_box_offense as NUMERIC) as raptor_box_offense
        ,cast(raptor_box_defense as NUMERIC) as raptor_box_defense
        ,cast(raptor_box_total as NUMERIC) as raptor_box_total
        ,cast(raptor_onoff_offense as NUMERIC) as raptor_onoff_offense
        ,cast(raptor_onoff_defense as NUMERIC) as raptor_onoff_defense
        ,cast(raptor_onoff_total as NUMERIC) as raptor_onoff_total
        ,cast(raptor_offense as NUMERIC) as raptor_offense
        ,cast(raptor_defense as NUMERIC) as raptor_defense
        ,cast(raptor_total as NUMERIC) as raptor_total
        ,cast(war_total as NUMERIC) as war_total
        ,cast(war_reg_season as NUMERIC) as war_reg_season
        ,cast(war_playoffs as NUMERIC) as war_playoffs
        ,cast(predator_offense as NUMERIC) as predator_offense
        ,cast(predator_defense as NUMERIC) as predator_defense
        ,cast(predator_total as NUMERIC) as predator_total
        ,cast(pace_impact as NUMERIC) as pace_impact
        ,load_datetime
        , row_number() over (partition by fivethirtyeight_key order by load_datetime desc) as dedup
    FROM {{ source('nba_raw','raw_538_player_raptor') }}
    ) a
WHERE dedup = 1
