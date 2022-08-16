{{ config(
    tags=["nba"]
) }}

SELECT a.game_key, a.game_date, a.team_abbr as away_team_abbr, h.team_abbr as home_team_abbr
FROM
    (SELECT distinct l.game_key, l.game_date, lkp.team_abbr
    FROM {{ ref('nba__conf_sbr_lines') }} l
        left join {{ ref('nba__transform_team_lookup') }} lkp
            on l.team_abbr = lkp.look_up
    WHERE h_a = 'A'
    ) a
    inner join 
        (SELECT distinct game_key, lkp.team_abbr
        FROM {{ ref('nba__conf_sbr_lines') }} l
            left join {{ ref('nba__transform_team_lookup') }} lkp
                on l.team_abbr = lkp.look_up
        WHERE h_a = 'H'
        ) h
        on a.game_key = h.game_key
