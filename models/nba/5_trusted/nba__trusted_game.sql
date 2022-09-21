{{ config(
    tags=["nba"]
    , labels = {'project': 'sports_analytics', 'league':'nba'}
    , partition_by = {
      'field': 'game_date',
      'data_type': 'date',
      'granularity': 'day'
    }
    , materialized='incremental'    
    , unique_key='unique_key'
    , merge_update_columns = ['game_week','season','season_type','arena_sk','game_status_text','game_timestamp_utc','game_datetime_central','game_start_time'
                                ,'game_duration','game_duration_minutes','attendance','complete_flag','ot_flag','ot_count','sellout','series_game_number'
                                ,'series_text','if_necessary','national_broadcaster_id','national_broadcast_display','total_score_game','total_score_game_4q'
                                ,'total_score_1h','total_score_1h_perc','total_score_2h','total_score_2h_perc','total_score_1q','total_score_1q_perc'
                                ,'total_score_2q','total_score_2q_perc','total_score_3q','total_score_3q_perc','total_score_4q','total_score_4q_perc','update_datetime']
) }}
    
SELECT  
    GENERATE_UUID() as game_sk
    , g.game_key as unique_key
    , a.arena_sk
    , g.game_key as game_key_nbacom
    , brg.game_key as game_key_br
    , g.game_date
    , CONCAT(CAST(EXTRACT(YEAR from g.game_date) as string), LPAD(CAST(EXTRACT(MONTH from g.game_date) as string),2,'0') ) as game_yearmonth
    , CONCAT(CAST(EXTRACT(YEAR from g.game_date) as string),'-', LPAD(CAST(EXTRACT(MONTH from g.game_date) as string),2,'0') ) as game_yearmonth_formatted
    --, EXTRACT(WEEK from g.game_date) as game_week
    , date_trunc(g.game_date, week) as game_week
    , g.season
    , g.season_type
    , g.game_status_text
    , g.game_timestamp_utc
    , g.game_datetime_central
    , g.game_start_time
    , g.game_duration
    , g.game_duration_minutes
    , g.attendance
    , g.complete_flag
    , g.ot_flag
    , g.ot_count
    , g.game_bit
    , g.sellout
    , g.series_game_number
    , g.series_text
    , g.if_necessary
    , g.national_broadcaster_id
    , g.national_broadcast_display
    , tps.total_score_game
    , tps.total_score_game_4q
    , tps.total_score_1q + tps.total_score_2q as total_score_1h
    , cast(round((tps.total_score_1q + tps.total_score_2q) / tps.total_score_game_4q,3) as NUMERIC) as total_score_1h_perc
    , tps.total_score_3q + tps.total_score_4q as total_score_2h
    , cast(round((tps.total_score_3q + tps.total_score_4q) / tps.total_score_game_4q,3) as NUMERIC) as total_score_2h_perc
    , tps.total_score_1q
    , cast(round(tps.total_score_1q / tps.total_score_game_4q,3) as NUMERIC) as total_score_1q_perc
    , tps.total_score_2q
    , cast(round(tps.total_score_2q / tps.total_score_game_4q,3) as NUMERIC) as total_score_2q_perc
    , tps.total_score_3q
    , cast(round(tps.total_score_3q / tps.total_score_game_4q,3) as NUMERIC) as total_score_3q_perc
    , tps.total_score_4q
    , cast(round(tps.total_score_4q / tps.total_score_game_4q,3) as NUMERIC) as total_score_4q_perc
    , case when ar.division = hr.division then true else false end as inter_division
    , case when ar.conference = hr.conference then true else false end as inter_conference
 
    , CURRENT_DATETIME() as insert_datetime
    , CURRENT_DATETIME() as update_datetime
FROM {{ ref('nba__conf_nbacom_game') }} g 
    inner join 
        (SELECT game_id
            , sum(score) as total_score_game
            , sum(case when period <= 4 then score end) as total_score_game_4q
            , sum(case when period = 1 then score end) as total_score_1q
            , sum(case when period = 2 then score end) as total_score_2q
            , sum(case when period = 3 then score end) as total_score_3q
            , sum(case when period = 4 then score end) as total_score_4q
        FROM {{ ref('nba__conf_nbacom_game_team_period_score') }}
        GROUP BY game_id
        ) tps
        on g.game_id = tps.game_id
    inner join {{ ref('nba__trusted_arena') }} a 
        on g.arena_id = a.arena_id_nbacom
    left join {{ ref('nba__transform_team_lookup') }} lkp_nbacom
        on g.home_team_tricode = lkp_nbacom.look_up
    left join {{ ref('nba__conf_basketballreference_game') }} brg 
        on g.game_date = brg.game_date 
    left join {{ ref('nba__transform_team_lookup') }} lkp_br
        on brg.home_abbr = lkp_br.look_up
        and lkp_nbacom.team_abbr = lkp_br.team_abbr

    left join {{ ref('nba__transform_team_lookup') }} hlkp
        on g.home_team_tricode = hlkp.look_up
    left join {{ source('nba_ref','ref_team') }} hr
        on hlkp.team_abbr = hr.team_abbr
    left join {{ ref('nba__transform_team_lookup') }} alkp
        on g.away_team_tricode = alkp.look_up
    left join {{ source('nba_ref','ref_team') }} ar
        on alkp.team_abbr = ar.team_abbr
{% if is_incremental() %}
  WHERE g.game_date >= (SELECT max(game_date) from {{ this }})
{% endif %}
