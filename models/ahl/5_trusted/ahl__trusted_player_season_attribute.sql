{{ config(
    tags=["ahl"]
    , labels = {'project': 'sports_analytics', 'league':'ahl'}
    , partition_by={
      'field': 'season',
      'data_type': 'int64',
      'range': {
        'start': 1950,
        'end': 2100,
        'interval': 1
      }
    }
    , materialized='incremental'    
    , unique_key='unique_key'
    , merge_update_columns = ['height','height_inches','weight','shoots','catches','jersey_number','position','update_datetime']
) }}

SELECT 
    GENERATE_UUID() as player_season_attribute_sk
    , player_sk || '|' || season as unique_key
    ,player_sk, season, height, height_inches, weight, shoots, catches, jersey_number, position
    , CURRENT_DATETIME() as insert_datetime
    , CURRENT_DATETIME() as update_datetime
FROM
    (SELECT 
        p.player_sk
        , r.season
        , r.height
        , r.height_inches
        , r.weight
        , r.shoots
        , r.catches
        , r.jersey_number
        , r.position
        , row_number() over (partition by r.season, r.player_id order by r.load_datetime desc) as dedup
    FROM {{ ref('ahl__conf_hockeytech_roster') }} r
        inner join {{ ref('ahl__trusted_player') }} p 
            on r.player_id = p.player_id 
    {% if is_incremental() %}
        WHERE r.season >= (SELECT max(season) from {{ this }})
    {% endif %}
    ) a
WHERE dedup = 1  -- this dedup is for players on multiple rosters within a given year

    