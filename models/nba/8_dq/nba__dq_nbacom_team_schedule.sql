{{ config(
    tags=["nba"]
) }}

SELECT season, season_type, team, game_date
FROM 
    (SELECT season, season_type, home_team_tricode as team, game_date
    FROM {{ ref('nba__conf_nbacom_game') }}
    UNION ALL
    SELECT season, season_type, away_team_tricode as team, game_date
    FROM {{ ref('nba__conf_nbacom_game') }}
    ) d
WHERE team = 'SAC'
    and game_date between '2018-10-01' and '2019-07-01'
ORDER BY game_date