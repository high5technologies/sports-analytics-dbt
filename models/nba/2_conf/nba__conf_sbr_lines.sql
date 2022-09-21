{{ config(
    tags=["nba"]
) }}

SELECT  
    game_key
    ,game_date
    ,h_a
    ,team_abbr
    ,team_city
    ,team_nickname
    ,opening_line_flag
    ,o_u
    ,odds_mtid
    ,period_type
    ,odds_type
    ,sports_book_key
    ,sports_book
    ,epoch_timestamp
    , cast(TIMESTAMP_MILLIS(cast(epoch_timestamp as INT64)) as datetime) as line_timestamp
    --, TIMESTAMP_MICROS(cast(epoch_timestamp as INT64)) as line_timestamp
    ,cast(line as numeric) as line
    ,cast(odds as numeric) as odds
    , round(case when cast(odds as numeric) < 0 then (-1 * cast(odds as numeric)) / (100 + (-1 * cast(odds as numeric))) when cast(odds as numeric) > 0 then 100 / (cast(odds as numeric)+100) end,4) as odds_breakeven
    ,load_datetime
    --, row_number() over (partition by game_key,h_a,opening_line_flag,o_u,odds_mtid,sports_book_key order by load_datetime desc) as dedup
FROM {{ source('nba_raw','raw_sbr_lines') }}
QUALIFY row_number() over (partition by game_key,h_a,opening_line_flag,o_u,odds_mtid,sports_book_key order by load_datetime desc) = 1
