{{ config(
    tags=["ahl"]
) }}

SELECT * 
FROM 
    (SELECT 
        game_mvp_key
        , cast(game_key as INT64) as game_key
        , game_date
        , cast(player_id as INT64) as player_id
        , cast(team_id as INT64) as team_id
        , team_abbrev as team_abbr
        , position
        , case when starting = '1' then true else false end as starting_flag
        , load_datetime
        , row_number() over (partition by game_mvp_key order by load_datetime desc) as dedup
    FROM ahl.raw_hockeytech_mvp
    ) a
WHERE dedup = 1