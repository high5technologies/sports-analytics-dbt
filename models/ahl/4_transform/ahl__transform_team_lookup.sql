{{ config(
    tags=["ahl"]
) }}

SELECT team_abbr, alias as look_up FROM {{ source('ahl_ref','ref_team_alias') }}
UNION DISTINCT 
SELECT team_abbr, team_abbr as look_up FROM {{ source('ahl_ref','ref_team') }}