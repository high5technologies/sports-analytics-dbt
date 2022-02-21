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
    , merge_update_columns = ['team','team_abbr','team_city','team_nickname','division','conference','franchise_code','update_datetime']
) }}

SELECT
  GENERATE_UUID() as team_sk
  --md5(concat(t.team_id,'|',t.season)) as team_sk
  , t.team_id || '|' || t.season as unique_key
  , t.team_id
  , t.season
  , concat(t.team_city, ' ', t.team_name) as team
  , t.team_abbr
  , t.team_city
  , t.team_name as team_nickname
  , r.division
  , r.conference
  , r.franchise_code
  , CURRENT_DATETIME() as insert_datetime
  , CURRENT_DATETIME() as update_datetime
FROM 
    (SELECT DISTINCT season, away_team_id as team_id, away_team_abbr as team_abbr, away_team_city as team_city, away_team_name as team_name
    FROM {{ ref('ahl__conf_hockeytech_game') }}
    {% if is_incremental() %}
        WHERE season >= (SELECT max(season) from {{ this }})
    {% endif %}
    UNION DISTINCT
    SELECT DISTINCT  season, home_team_id as team_id, home_team_abbr as team_abbr, home_team_city as team_city, home_team_name as team_name
    FROM {{ ref('ahl__conf_hockeytech_game') }}
    {% if is_incremental() %}
        WHERE season >= (SELECT max(season) from {{ this }})
    {% endif %}
    ) t
    left join {{ ref('ahl__transform_team_lookup') }} a 
        on t.team_abbr = a.look_up
    left join {{ source('ahl_ref','ref_team') }} r
        on a.team_abbr = r.team_abbr
