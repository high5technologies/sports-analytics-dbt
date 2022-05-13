{{ config(
    tags=["nba"]
) }}

SELECT * 
FROM 
    (SELECT  
        game_key
        ,game_date
        , case when EXTRACT(MONTH FROM game_date) > case when EXTRACT(MONTH FROM game_date) = 2020 then 11 else 7 end then EXTRACT(year FROM game_date) + 1 else EXTRACT(year FROM game_date) end as season
        , PARSE_TIME("%I:%M%p", replace(replace(upper(game_start_time),'P','PM'),'A','AM')) as game_start_time
        -- , STR_TO_DATE(replace(replace(upper(game_start_time),'P','PM'),'A','AM'), '%h:%i%p') as game_start_time
        , cast(replace(attendance,',','') as INT64) as attendance
		, case when coalesce(cast(home_pts as INT64),0) > 0 and coalesce(cast(visitor_pts as INT64),0) > 0 then TRUE else FALSE end as complete_flag
		, case when overtimes is null then FALSE else TRUE end as ot_flag
		, coalesce(cast(nullif(replace(overtimes, 'OT', ''), '') as INT64),0) as ot_count
		, 1 as game_bit
        , case when coalesce(cast(home_pts as INT64),0) = 0 or coalesce(cast(visitor_pts as INT64),0) = 0 then null
               when cast(visitor_pts as INT64) > cast(home_pts as INT64) then 'A' 
               when cast(visitor_pts as INT64) < cast(home_pts as INT64) then 'H'
               when cast(visitor_pts as INT64) = cast(home_pts as INT64) then 'P'
        end as win_h_a
        ,home_team_name
        ,home_abbr
        ,visitor_team_name
        ,away_abbr
        ,box_score_url
        ,cast(home_pts as INT64) as home_pts
        ,cast(visitor_pts as INT64) as visitor_pts
        ,overtimes
        ,cast(h_g1_score as INT64) as h_g1_score
        ,cast(a_g1_score as INT64) as a_g1_score
        ,cast(h_g2_score as INT64) as h_g2_score
        ,cast(a_g2_score as INT64) as a_g2_score
        ,cast(h_g3_score as INT64) as h_g3_score
        ,cast(a_g3_score as INT64) as a_g3_score
        ,cast(h_g4_score as INT64) as h_g4_score
        ,cast(a_g4_score as INT64) as a_g4_score
        ,cast(h_g5_score as INT64) as h_g5_score
        ,cast(a_g5_score as INT64) as a_g5_score
        ,cast(h_g6_score as INT64) as h_g6_score
        ,cast(a_g6_score as INT64) as a_g6_score
        ,cast(h_g7_score as INT64) as h_g7_score
        ,cast(a_g7_score as INT64) as a_g7_score
        ,cast(h_ff_tov_pct as NUMERIC) as h_ff_tov_pct
        ,cast(a_ff_tov_pct as NUMERIC) as a_ff_tov_pct
        ,cast(h_ff_off_rtg as NUMERIC) as h_ff_off_rtg
        ,cast(a_ff_off_rtg as NUMERIC) as a_ff_off_rtg
        ,cast(h_ff_pace as NUMERIC) as h_ff_pace
        ,cast(a_ff_pace as NUMERIC) as a_ff_pace
        ,cast(h_ff_ft_rate as NUMERIC) as h_ff_ft_rate
        ,cast(a_ff_ft_rate as NUMERIC) as a_ff_ft_rate
        ,cast(h_ff_orb_pct as NUMERIC) as h_ff_orb_pct
        ,cast(a_ff_orb_pct as NUMERIC) as a_ff_orb_pct
        ,cast(h_ff_efg_pct as NUMERIC) as h_ff_efg_pct
        ,cast(a_ff_efg_pct as NUMERIC) as a_ff_efg_pct
        ,LOAD_DATETIME
        , row_number() over (partition by game_key order by load_datetime desc) as dedup
    FROM {{ source('nba_raw','raw_basketballreference_game') }}
    ) a
WHERE dedup = 1


