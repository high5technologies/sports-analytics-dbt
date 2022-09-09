{{ config(
    tags=["nba"]
) }}

with swish_game_player as (
    SELECT distinct
        ss.player_id_swish
        , ss.game_date
        , ss.player_name
        , lkp.team_abbr
    FROM {{ ref('nba__conf_swish_salary') }} ss 
        inner join {{ ref('nba__transform_team_lookup') }} lkp
            on upper(ss.team_abbr) = lkp.look_up
)

, nbacom_game_players as (
    SELECT distinct 
        gp.player_id_nbacom
        , lkp.team_abbr
        , gp.game_date
        , upper(replace(player_first_name,'.','')) as first_name
        , upper(player_last_name) as last_name 
    FROM {{ ref('nba__conf_nbacom_game_player') }} gp
        inner join {{ ref('nba__conf_nbacom_game') }} g 
            on gp.game_id = g.game_id 
        inner join {{ ref('nba__transform_team_lookup') }} lkp 
            on lkp.look_up = case when gp.h_a = 'A' then g.away_team_tricode else g.home_team_tricode end
        
)

, fuzzy_match_score as (
    SELECT 
        coalesce(s.game_date, n.game_date) as game_date
        , coalesce(s.team_abbr, n.team_abbr) as team_abbr
        , s.player_id_swish
        , n.player_id_nbacom
        , s.player_name as swish_player_name
        , n.first_name as nbacom_first_name
        , n.last_name as nbacom_last_name
        , n.first_name || ' ' || n.last_name as nbacom_player_name
        , common.fuzzy_jaro_wrinkler_distance(s.player_name,n.first_name || ' ' || n.last_name) as name_score
        --, common.fuzzy_jaro_wrinkler_distance(n.last_name,r.last_name) as last_name_score
    FROM swish_game_player s 
        join nbacom_game_players n 
            on s.team_abbr = n.team_abbr -- blocking section
            and s.game_date = n.game_date
)

, fuzzy_match_score_ranks as (
    SELECT *
        , ROW_NUMBER() OVER (partition by player_id_swish, team_abbr, game_date ORDER BY name_score desc) as swish_score_rank
        , ROW_NUMBER() OVER (partition by player_id_nbacom, team_abbr, game_date ORDER BY name_score desc) as nbacom_score_rank
    FROM fuzzy_match_score
)

SELECT * 
FROM fuzzy_match_score_ranks
WHERE (swish_score_rank = 1 or nbacom_score_rank = 1)
    and game_date = '2019-03-01'
ORDER BY game_date, player_id_swish
/*
WITH nbacom_game_players as (
    SELECT distinct 
        gp.player_id_nbacom
        , lkp.team_abbr
        , upper(replace(player_first_name,'.','')) as first_name
        , upper(player_last_name) as last_name 
    FROM {{ ref('nba__conf_nbacom_game_player') }} gp
        inner join {{ ref('nba__conf_nbacom_game') }} g 
            on gp.game_id = g.game_id
        inner join {{ ref('nba__transform_team_lookup') }} lkp 
            on lkp.look_up = case when gp.h_a = 'A' then away_team_tricode else home_team_tricode end
    WHERE gp.game_date between var_start_game_date and var_end_game_date
)

, cte_rotoguru_players as (
    SELECT distinct 
        player_id as player_id_rotoguru
        , lkp.team_abbr
        , upper(replace(player_first_name,'.','')) as first_name
        , upper(player_last_name) as last_name
    FROM nba.vw_raw_rotoguru_dfssalary r
        inner join nba.vw_trusted_team_lookup lkp 
            on lkp.look_up = r.team
    WHERE r.game_date between var_start_game_date and var_end_game_date
)

, cte_match_score as (
    SELECT 
        n.team_abbr 
        , n.player_id_nbacom
        , r.player_id_rotoguru
        , `var_gcp_project_name`.common.fuzzy_jaro_wrinkler_distance(n.first_name,r.first_name) as first_name_score
        , `var_gcp_project_name`.common.fuzzy_jaro_wrinkler_distance(n.last_name,r.last_name) as last_name_score
        , n.first_name as first_name_nbacom
        , n.last_name as last_name_nbacom
        , r.first_name as first_name_rg
        , r.last_name as last_name_rg
    FROM cte_nbacom_players n 
        inner join cte_rotoguru_players r
            on n.team_abbr = r.team_abbr -- blocking section
)

, cte_combined_score as (
    SELECT *
        , (first_name_score + last_name_score) / 2 as combined_score
        , ROW_NUMBER() OVER (partition by player_id_nbacom, team_abbr ORDER BY (first_name_score + last_name_score) / 2 desc) as score_rank
    FROM cte_match_score
)

, cte_match_results as (
    SELECT player_id_nbacom, player_id_rotoguru
    FROM cte_combined_score
    WHERE 
        (score_rank = 1 and combined_score >= .9)
        or (score_rank = 1 and last_name_nbacom = last_name_rg)
    QUALIFY row_number() over (partition by player_id_nbacom order by team_abbr) = 1
)
*/