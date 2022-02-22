{{ config(
    tags=["ahl"]
) }}

SELECT * 
    , pim * 60 as pis
    , game_event_time_seconds + (coalesce(pim*60,0)) as game_pp_end

FROM 
    (SELECT 
        game_log_key
        , cast(game_key as INT64) as game_key
        , game_date
        , upper(event_type) as event_type
		, cast(split(game_log_key,'|')[ORDINAL(2)] as INT64) as event_order_number
        , period
		, period_number
		, time
		, case when period = 'SO' then 3901 else  (cast(split(time,':')[ORDINAL(1)] as INT64) * 60) + cast(split(time,':')[ORDINAL(2)] as INT64) end as event_time_in_seconds
		, case when period = 'SO' then 3901 else ((cast(split(time,':')[ORDINAL(1)] as INT64) * 60) + cast(split(time,':')[ORDINAL(2)] as INT64)) + ((period_number - 1)*60*20) end as game_event_time_seconds
        
		, cast(team_id as INT64) as team_id
        , cast(nullif(player_id,'') as INT64) as player_id
        , nullif(shot_type,'') as shot_type
        , nullif(shot_quality,'') as shot_quality
        , shot_is_goal
        , nullif(shooter_position,'') as shooter_position
        , cast(nullif(x_location,'') as NUMERIC) as x_location
        , cast(nullif(y_location,'') as NUMERIC) as y_location
        , cast(nullif(goalie_id,'') as INT64) as goalie_id
        , case when power_play = '1' then true when power_play = '0' then false end as power_play_flag
        , case when short_handed = '1' then true when short_handed = '0' then false end as short_handed_flag
        , case when game_winning_goal_flag = '1' then true when game_winning_goal_flag = '0' then false end as game_winning_goal_flag
        , case when insurance_goal_flag = '1' then true when insurance_goal_flag = '0' then false end as insurance_goal_flag
        , case when empty_net = '1' then true when empty_net = '0' then false end as empty_net_flag
        , cast(nullif(primary_assist_player_id,'') as INT64) as primary_assist_player_id
        , cast(nullif(secondary_assist_player_id,'') as INT64) as secondary_assist_player_id
        , nullif(penalty,'') as penalty
        , penalty_is_power_play
        , cast(nullif(penalty_servered_by_player_id,'') as INT64) as penalty_servered_by_player_id
        , case when penalty_shot_flag = '1' then true when penalty_shot_flag = '0' then false end as penalty_shot_flag
        , cast(nullif(pim,'') as NUMERIC) as pim
        , cast(nullif(plus_player_id_1,'') as INT64) as plus_player_id_1
        , cast(nullif(plus_player_id_2,'') as INT64) as plus_player_id_2
        , cast(nullif(plus_player_id_3,'') as INT64) as plus_player_id_3
        , cast(nullif(plus_player_id_4,'') as INT64) as plus_player_id_4
        , cast(nullif(plus_player_id_5,'') as INT64) as plus_player_id_5
        , cast(nullif(plus_player_id_6,'') as INT64) as plus_player_id_6
        , cast(nullif(minus_player_id_1,'') as INT64) as minus_player_id_1
        , cast(nullif(minus_player_id_2,'') as INT64) as minus_player_id_2
        , cast(nullif(minus_player_id_3,'') as INT64) as minus_player_id_3
        , cast(nullif(minus_player_id_4,'') as INT64) as minus_player_id_4
        , cast(nullif(minus_player_id_5,'') as INT64) as minus_player_id_5
        , cast(nullif(minus_player_id_6,'') as INT64) as minus_player_id_6
        , cast(nullif(goalie_coming_in_id,'') as INT64) as goalie_coming_in_id
        , cast(nullif(goalie_coming_out_id,'') as INT64) as goalie_coming_out_id
        , load_datetime
        , row_number() over (partition by game_log_key order by load_datetime desc) as dedup
    FROM 
        (SELECT * 
            , case when period = 'OT' then 4 when period = 'SO' then 99 when period like '%OT' then cast(left(period,1) as INT64) + 3 else cast(period as INT64) end as period_number
        FROM {{ source('ahl_raw','raw_hockeytech_gamelog') }}
        )
    ) a
WHERE dedup = 1

