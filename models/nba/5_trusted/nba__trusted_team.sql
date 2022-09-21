{{ config(
    tags=["nba"]
    , labels = {'project': 'sports_analytics', 'league':'nba'}
    , materialized='incremental'    
    , unique_key='unique_key'
    , merge_update_columns = ['team','team_city','team_nickname','team_slug','division','franchise_code','update_datetime']
) }}

SELECT 
    GENERATE_UUID() as team_sk
    , t.team_abbr || '|' || t.season as unique_key
    , t.team_id_nbacom
    , t.season
    , concat(t.team_city, ' ', t.team_nickname) as team
    , r.team_abbr
    , t.team_city
    , t.team_nickname
    , t.team_slug
    , r.division
    , r.conference
    , r.franchise_code
    , CURRENT_DATETIME() as insert_datetime
    , CURRENT_DATETIME() as update_datetime
FROM 
    (SELECT distinct season, home_team_id as team_id_nbacom, home_team_city as team_city, home_team_name as team_nickname, home_team_tricode as team_abbr, home_team_slug as team_slug 
    FROM {{ ref('nba__conf_nbacom_game') }}
    --WHERE season = var_season
    UNION DISTINCT 
    SELECT distinct season, away_team_id as team_id_nbacom, away_team_city as team_city, away_team_name as team_nickname, away_team_tricode as team_abbr, away_team_slug as team_slug 
    FROM {{ ref('nba__conf_nbacom_game') }} 
    --WHERE season = var_season
    ) t
    left join {{ ref('nba__transform_team_lookup') }} lkp
        on t.team_abbr = lkp.look_up
    left join {{ source('nba_ref','ref_team') }} r
        on lkp.team_abbr = r.team_abbr
