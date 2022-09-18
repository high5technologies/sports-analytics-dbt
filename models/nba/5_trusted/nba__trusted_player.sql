{{ config(
    tags=["nba"]
    , labels = {'project': 'sports_analytics', 'league':'nba'}
    , materialized='incremental'    
    , unique_key='unique_key'
    , merge_update_columns = ['player_id_swish','player_id_fantasylabs','player_id_linestar','player_name','player_first_name','player_last_name'
                              ,'player_name_i','player_slug','player_primary_position','jersey_num','update_datetime']
) }}

-------------------------------------------------------------
-- Player base data (nba.com)
-------------------------------------------------------------
WITH cte_player_base as (
    SELECT 
        player_id_nbacom
        ,player_name
        ,player_first_name
        ,player_last_name
        ,player_name_i
        ,player_slug
        ,jersey_num
        ,position
        ,case when position = '' then 0 else count(*) end as cnt -- ignore counts where no position to get an actual position in the next query
    FROM {{ ref('nba__conf_nbacom_game_player') }}
    --WHERE position != ''
        --and game_date between var_start_game_date and var_end_game_date
    GROUP BY player_id_nbacom
        ,player_name
        ,player_first_name
        ,player_last_name
        ,player_name_i
        ,player_slug
        ,jersey_num
        ,position
) 

, base as (
    SELECT 
        GENERATE_UUID() as player_sk
        ,p.player_id_nbacom as unique_key
        ,p.player_id_nbacom
        --,mr.player_id_rotoguru
        ,p.player_name
        ,p.player_first_name
        ,p.player_last_name
        ,p.player_name_i
        ,p.player_slug
        ,nullif(p.jersey_num,'') as jersey_num
        ,p.position as player_primary_position
        ,row_number() over (partition by p.player_id_nbacom order by p.cnt, p.position) as dedup
        ,CURRENT_DATETIME() as insert_datetime
        ,CURRENT_DATETIME() as update_datetime
    FROM cte_player_base p 
        --left join cte_match_results mr 
        --    on p.player_id_nbacom = mr.player_id_nbacom
    WHERE 1=1
    QUALIFY dedup = 1
)

SELECT 
    b.player_sk
    ,b.unique_key
    ,b.player_id_nbacom
    ,f.player_id_swish
    ,f.player_id_fantasylabs
    ,f.player_id_linestar
    ,b.player_name
    ,b.player_first_name
    ,b.player_last_name
    ,b.player_name_i
    ,b.player_slug
    ,b.jersey_num
    ,b.player_primary_position
    ,b.insert_datetime
    ,b.update_datetime
FROM base b 
    left join {{ ref('nba__transform_player_fuzzy_match') }} f 
        on b.player_id_nbacom = f.player_id_nbacom


/*
WITH cte_nbacom_players as (
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