{{ config(
    tags=["nba"]
) }}

SELECT *
    , reverse(split(reverse(assist_name_and_running_total),' ')[offset(0)]) as assist_running_total
    , reverse(split(reverse(assist_name_and_running_total),' ')[offset(1)]) as assist_last_name
    , row_number() over (partition by game_id, period, clock_text, action_type order by action_number asc) as game_clock_action_rank
    , row_number() over (partition by game_id order by period, clock_time) as event_order_number
FROM 
    (SELECT  
        game_event_key
        ,game_id
        ,game_date
        ,cast(action_number as INT64) as action_number
        ,clock
        ,replace(split(clock,'M')[offset(0)],'PT','') || ':' || split(replace(split(clock,'M')[offset(1)],'S',''),'.')[offset(0)] || '.' || split(replace(split(clock,'M')[offset(1)],'S',''),'.')[offset(1)] as clock_text
        ,cast(replace(split(clock,'M')[offset(0)],'PT','') as INT64) as clock_minute
        ,cast(split(replace(split(clock,'M')[offset(1)],'S',''),'.')[offset(0)] as INT64) as clock_second
        ,cast(split(replace(split(clock,'M')[offset(1)],'S',''),'.')[offset(1)] as INT64) as clock_sub_second
        ,PARSE_TIME('%M:%E2S', replace(split(clock,'M')[offset(0)],'PT','') || ':' || split(replace(split(clock,'M')[offset(1)],'S',''),'.')[offset(0)] || '.' || split(replace(split(clock,'M')[offset(1)],'S',''),'.')[offset(1)]) as clock_time
        ,cast(period as INT64) as period
        ,team_id
        ,team_tricode
        ,person_id
        ,player_name
        ,player_name_i
        ,cast(nullif(x_legacy,'') as INT64) as x_legacy
        ,cast(nullif(y_legacy,'') as INT64) as y_legacy
        ,cast(nullif(shot_distance,'') as INT64) as shot_distance
        ,shot_result
        ,cast(case when is_field_goal = '0' then FALSE when is_field_goal = '1' then TRUE end as BOOL) as is_field_goal
        ,cast(nullif(score_home,'') as INT64) as score_home
        , score_home as score_home_org
        ,cast(nullif(score_away,'') as INT64) as score_away
        ,cast(nullif(points_total,'') as INT64) as points_total
        ,nullif(location,'') as location
        ,description
        , case when COALESCE(action_type,'') != '' then upper(action_type)
            when description like '% STEAL (%' then 'STEAL'
            when description like '% BLOCK (%' then 'BLOCK'
            else 'UNKNOWN'
        end as action_type
        ,sub_type
        ,video_available

        --new
        , case when action_type = 'Free Throw' then 1 when is_field_goal = '1' then 
            case when description like '%3PT%' then 3 else 2 end
        end as points_possible
        , case when action_type = 'Free Throw' then
            case when cast(points_total as INT64) = 0 then 0 else 1 end
        when is_field_goal = '1' then 
            case when shot_result = 'Missed' then 0 when shot_result = 'Made' then
                case when description like '%3PT%' then 3 else 2 end
            end
        end as points_result
        , case when is_field_goal = '1' and description like '% AST)%' then true when is_field_goal = '1' then false end as is_assisted_field_goal
        , case when is_field_goal = '1' and description like '% AST)%' then 
            reverse(split(reverse(split(description,' AST)')[offset(0)]),'(')[offset(0)])
        end as assist_name_and_running_total -- reverse and split this on space above
        , case when action_type = 'Substitution' then split(replace(description, 'SUB: ',''),' FOR ')[offset(0)] end as sub_new_player_last_name
        , case when action_type = 'Substitution' then split(replace(description, 'SUB: ',''),' FOR ')[offset(1)] end as sub_old_player_last_name
        , case when action_type in ('Foul','Violation') and CONTAINS_SUBSTR(description, ')') then reverse(split(split(reverse(description),')')[offset(1)],'(')[offset(0)]) end as ref_name

        ,load_datetime
        , row_number() over (partition by game_event_key order by load_datetime desc) as dedup
        FROM {{ source('nba_raw','raw_nbacom_game_event') }}
    ) a
WHERE dedup = 1
