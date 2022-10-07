{{ config(
    tags=["nba"]
    , labels = {'project': 'sports_analytics', 'league':'nba'}
    , materialized='incremental'    
    , unique_key='unique_key'
    , on_schema_change='sync_all_columns'
    , merge_update_columns = ['swish_salary_key','pos','projected_fantasy_pts','avg_pts','salary','actual_dfs_points','percent_owned','update_datetime']
) }}

with linestar_pre as (
    SELECT game_date, player_id_linestar, dfs_source,player_pos_adj, player_pos, player_salary
        , count(player_salary) over (partition by game_date, player_id_linestar, dfs_source,player_pos_adj, player_pos, player_salary) as player_salary_count
        , avg(owned) over (partition by game_date, player_id_linestar, dfs_source,player_pos_adj, player_pos) as owned
    FROM {{ ref('nba__conf_linestar_ownership') }}
    --GROUP BY game_date, player_id_linestar, dfs_source,player_pos_adj, player_pos, player_salary
        --linestar_key,linestar_type
    --QUALIFY row_number() over (partition by game_date, player_id_linestar, dfs_source,player_pos_adj, player_pos order by player_salary_count desc) = 1
)

, linestar as (
    SELECT *
    FROM linestar_pre
    QUALIFY row_number() over (partition by game_date, player_id_linestar, dfs_source,player_pos_adj, player_pos order by player_salary_count desc) = 1
)

, fantasylabs_pre as (
    SELECT game_date, player_id_fantasylabs, dfs_source, position_adj, position, salary, actual_points
        , count(salary) over (partition by game_date, player_id_fantasylabs, dfs_source, position_adj, position, salary, actual_points) as salary_count
        , avg(ownership_average) over (partition by game_date, player_id_fantasylabs, dfs_source, position_adj, position, actual_points) as ownership_average
        --, avg(ownership_volatility) over (partition by game_date, player_id_fantasylabs, dfs_source, position_adj, position, actual_points) as ownership_volatility
    FROM {{ ref('nba__conf_fantasylabs_ownership') }}
)

, fantasylabs as (
    SELECT * 
    FROM fantasylabs_pre
    QUALIFY row_number() over (partition by game_date, player_id_fantasylabs, dfs_source,position_adj, position, actual_points order by salary_count desc) = 1
)

, data as (
    SELECT 
        tgtp.game_team_player_sk
        , coalesce(ss.dfs_source, ls.dfs_source, fl.dfs_source) as dfs_source
        , tgtp.game_date
        , ss.swish_salary_key
        --, fl.fantasylabs_key
        --, coalesce(lsa.linestar_key, lsp.linestar_key) as linestar_key

        --, ss.pos_main as pos_swish
        --, fl.position as pos_fantasylabs
        --, coalesce(lsa.player_pos, lsp.player_pos) as pos_linestar
        , coalesce(ss.pos_main_adj,fl.position_adj,ls.player_pos_adj,ss.pos_main,fl.position,ls.player_pos) as pos

        , ss.projected_fantasy_pts
        , ss.avg_pts
        --, ss.salary as salary_ss
        --, fl.salary as salary_fl
        --, lsa.player_salary as salary_actual
        --, lsp.player_salary as salary_projected
        , coalesce(fl.salary,ls.player_salary,ss.salary) as salary

        --, fl.actual_points
        , case coalesce(ss.dfs_source, ls.dfs_source, fl.dfs_source) 
            when 'fd' then  
                (tgtp.three_pointers_made * 3)
                + (tgtp.two_pointers_made * 2)
                + (tgtp.free_throws_made * 1)
                + (tgtp.rebounds_total * 1.2)
                + (tgtp.assists * 1.5)
                + (tgtp.blocks * 3)
                + (tgtp.steals * 3)
                + (tgtp.turnovers * -1)
            when 'dk' then
                (tgtp.points * 1)
                + (tgtp.three_pointers_made * 0.5)
                + (tgtp.rebounds_total * 1.25)
                + (tgtp.assists * 1.5)
                + (tgtp.steals * 2)
                + (tgtp.blocks * 2)
                + (tgtp.turnovers * -0.5)
                + (case when doubles_count = 2 then 1.5 when doubles_count >= 3 then 3 else 0  end)
        end as actual_dfs_points




        , round((coalesce(fl.ownership_average,0) + coalesce(ls.owned,0)) / case when fl.ownership_average is not null and ls.owned is not null then 2.0 else 1.0 end,1) as percent_owned
        --, fl.ownership_average
        --, ls.owned
        --, fl.ownership_volatility
        --, fl.gpp_grade
        
        --, ss.fpts_diff
        --, ss.salary_diff
        --, ss.salary_diff_percentage
    FROM {{ ref('nba__trusted_game_team_player') }} tgtp
        inner join {{ ref('nba__trusted_player') }} tp 
            on tgtp.player_sk = tp.player_sk
        left join {{ ref('nba__conf_swish_salary') }} ss 
            on tp.player_id_swish = ss.player_id_swish
            and tgtp.game_date = ss.game_date
        left join fantasylabs fl 
            on tp.player_id_fantasylabs = fl.player_id_fantasylabs
            and tgtp.game_date = fl.game_date
            and ss.dfs_source = fl.dfs_source
        left join linestar ls 
            on tp.player_id_linestar = ls.player_id_linestar
            and tgtp.game_date = ls.game_date
            and ss.dfs_source = ls.dfs_source
)

SELECT 
    GENERATE_UUID() as game_team_player_dfs_sk
    ,game_team_player_sk || '|' || dfs_source as unique_key
    , *
    , CURRENT_DATETIME() as insert_datetime
    , CURRENT_DATETIME() as update_datetime
FROM data 
WHERE game_date = '2021-10-26'
    and dfs_source = 'dk'
ORDER BY salary desc  



/*left join {{ ref('nba__conf_fantasylabs_ownership') }} fl
        on tp.player_id_fantasylabs = fl.player_id_fantasylabs
        and tgtp.game_date = fl.game_date
        and ss.dfs_source = fl.dfs_source
    */
    /*left join {{ ref('nba__conf_linestar_ownership') }} lsa
        on tp.player_id_linestar = lsa.player_id_linestar
        and tgtp.game_date = lsa.game_date
        and ss.dfs_source = lsa.dfs_source
        and lsa.linestar_type = 'actual'
    left join {{ ref('nba__conf_linestar_ownership') }} lsp
        on tp.player_id_linestar = lsp.player_id_linestar
        and tgtp.game_date = lsp.game_date
        and ss.dfs_source = lsp.dfs_source
        and lsp.linestar_type = 'projected'*/