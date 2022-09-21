{{ config(
    tags=["nba"]
) }}

SELECT season, season_type, team, count(*) as games_play
FROM 
    (SELECT season, season_type, home_team_tricode as team, game_date
    FROM {{ ref('nba__conf_nbacom_game') }}
    UNION ALL
    SELECT season, season_type, away_team_tricode as team, game_date
    FROM {{ ref('nba__conf_nbacom_game') }}
    ) d
WHERE season_type in ('Regular Season','Playoffs')
GROUP BY season, season_type, team
ORDER BY season, season_type desc, team