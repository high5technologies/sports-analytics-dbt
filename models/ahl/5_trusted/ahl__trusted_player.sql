{{ config(
    tags=["ahl"]
    , labels = {'project': 'sports_analytics', 'league':'ahl'}
    , materialized='incremental'    
    , unique_key='unique_key'
    , merge_update_columns = ['player_name','player_first_name','player_last_name','player_primary_position','jersey_number','birth_date','birth_place'
                                ,'birth_place_city','birth_place_location','birth_place_country','update_datetime']
) }}

SELECT 
    GENERATE_UUID() as player_sk
    , player.player_id as unique_key
    , player.player_id
    , player.player_name
    , player.player_first_name
    , player.player_last_name
    , pos.position as player_primary_position
    , player.jersey_number
    , player.birth_date
    , r.birth_place, r.birth_place_city, r.birth_place_location, r.birth_place_country
    , CURRENT_DATETIME() as insert_datetime
FROM 
    (SELECT player_id, player_first_name, player_last_name, birth_date, jersey_number,player_name
    FROM
        (SELECT player_id, player_first_name, player_last_name, birth_date, jersey_number,player_name
        , row_number() over (partition by player_id order by last_played desc) as dedup_id
        FROM
            (SELECT distinct skater_id as player_id, player_first_name, player_last_name, birth_date, jersey_number,player_name
                , max(game_date) over (partition by skater_id) as last_played
            FROM {{ ref('ahl__conf_hockeytech_skaterbox') }} sb 
            UNION ALL 
            SELECT distinct goalie_id as player_id, player_first_name, player_last_name, birth_date, jersey_number,player_name
                , max(game_date) over (partition by goalie_id) as last_played
            FROM {{ ref('ahl__conf_hockeytech_goaliebox') }}
            ) a
        ) b
    WHERE b.dedup_id = 1 -- dedup based on last played to get most recent name/birthdate/jersey number
    ) player
    inner join 
        (SELECT player_id, position 
        FROM
            (SELECT player_id, position
                , row_number() over (partition by player_id order by position_count desc, last_played desc) as dedup_id
            FROM
                (SELECT distinct skater_id as player_id, position
                    , count(position) over (partition by skater_id, position) as position_count 
                    , max(game_date) over (partition by skater_id, position) as last_played
                FROM {{ ref('ahl__conf_hockeytech_skaterbox') }}
                UNION ALL 
                SELECT distinct goalie_id as player_id, position
                    , count(position) over (partition by goalie_id, position) as position_count 
                    , max(game_date) over (partition by goalie_id, position) as last_played
                FROM {{ ref('ahl__conf_hockeytech_goaliebox') }} 
                ) a
            ) b
        WHERE b.dedup_id = 1 -- dedup to single "primary position" per player_id
        ) pos
        on player.player_id = pos.player_id
    left join 
        (SELECT player_id, birth_place,birth_place_city,birth_place_location, birth_place_country
            ,row_number() over (partition by player_id order by season desc, load_datetime desc) as dedup_player_id
        FROM {{ ref('ahl__conf_hockeytech_roster') }}
        ) r
        on player.player_id = r.player_id
        and r.dedup_player_id = 1

 