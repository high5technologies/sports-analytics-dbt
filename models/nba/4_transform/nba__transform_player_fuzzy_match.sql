{{ config(
    tags=["nba"]
    ,materialized='table'
) }}

with nbacom_game_players as (
    SELECT distinct 
        gp.player_id_nbacom
        , lkp.team_abbr
        , gp.game_date
        , REGEXP_REPLACE(player_name,r'[^a-zA-Z ]','') as nbacom_player_name
        --, upper(REGEXP_REPLACE(player_first_name,r'[^a-zA-Z]','')) as first_name
        --, upper(REGEXP_REPLACE(player_last_name,r'[^a-zA-Z]','')) as last_name 
    FROM {{ ref('nba__conf_nbacom_game_player') }} gp
        inner join {{ ref('nba__conf_nbacom_game') }} g 
            on gp.game_id = g.game_id 
        inner join {{ ref('nba__transform_team_lookup') }} lkp 
            on lkp.look_up = case when gp.h_a = 'A' then g.away_team_tricode else g.home_team_tricode end
        
)

, swish_game_player as (
    SELECT distinct
        ss.player_id_swish
        , ss.game_date
        , REGEXP_REPLACE(ss.player_name,r'[^a-zA-Z ]','') as player_name
        , lkp.team_abbr
    FROM {{ ref('nba__conf_swish_salary') }} ss 
        inner join {{ ref('nba__transform_team_lookup') }} lkp
            on upper(ss.team_abbr) = lkp.look_up
)

, swish_fuzzy_match_score as (
    SELECT 
        coalesce(n.game_date, s.game_date) as game_date
        , coalesce(n.team_abbr, s.team_abbr) as team_abbr
        , s.player_id_swish
        , n.player_id_nbacom
        --, n.first_name as nbacom_first_name
        --, n.last_name as nbacom_last_name
        , n.nbacom_player_name
        , s.player_name as swish_player_name
        , common.fuzzy_jaro_wrinkler_distance(s.player_name,n.nbacom_player_name) as swish_name_score
    FROM nbacom_game_players n 
        join swish_game_player s 
            on s.team_abbr = n.team_abbr -- blocking section
            and s.game_date = n.game_date
)

, swish_fuzzy_match as (
    SELECT *
        , ROW_NUMBER() OVER (partition by player_id_swish, game_date ORDER BY swish_name_score desc) as swish_score_rank
        , ROW_NUMBER() OVER (partition by player_id_nbacom, game_date ORDER BY swish_name_score desc) as nbacom_score_rank
    FROM swish_fuzzy_match_score
    WHERE swish_name_score >= .75
    QUALIFY nbacom_score_rank = 1 and swish_score_rank = 1
        --ROW_NUMBER() OVER (partition by player_id_nbacom, team_abbr, game_date ORDER BY swish_name_score desc) = 1
)

--- 

, fantasylabs_game_player as (
    SELECT distinct
        ss.player_id_fantasylabs
        , ss.game_date
        , REGEXP_REPLACE(ss.player_name,r'[^a-zA-Z ]','') as player_name
        , ss.team
        , lkp.team_abbr
    FROM {{ ref('nba__conf_fantasylabs_ownership') }} ss 
        left join {{ ref('nba__transform_team_lookup') }} lkp
            on upper(ss.team) = lkp.look_up
)

/*SELECT * 
FROM fantasylabs_game_player
WHERE team != '' and team_abbr is null
*/

, fantasylabs_fuzzy_match_score as (
    SELECT 
        coalesce(n.game_date, s.game_date) as game_date
        , coalesce(n.team_abbr,s.team_abbr) as team_abbr
        , s.player_id_fantasylabs
        , n.player_id_nbacom
        --, n.first_name as nbacom_first_name
        --, n.last_name as nbacom_last_name
        , n.nbacom_player_name
        , s.player_name as fantasylabs_player_name
        , common.fuzzy_jaro_wrinkler_distance(s.player_name,n.nbacom_player_name) as fantasylabs_name_score
    FROM nbacom_game_players n 
        join fantasylabs_game_player s 
            --on case when s.team_abbr is null then n.team_abbr else s.team_abbr end = n.team_abbr -- blocking section  teams are wrong in the fantasylabs data - guessing its assigning current team only
            on s.game_date = n.game_date
)


, fantasylabs_fuzzy_match as (
    SELECT *
        , ROW_NUMBER() OVER (partition by player_id_fantasylabs, game_date ORDER BY fantasylabs_name_score desc) as fantasylabs_score_rank
        , ROW_NUMBER() OVER (partition by player_id_nbacom, game_date ORDER BY fantasylabs_name_score desc) as nbacom_score_rank
    FROM fantasylabs_fuzzy_match_score
    WHERE fantasylabs_name_score >= .75
    QUALIFY nbacom_score_rank = 1 and fantasylabs_score_rank = 1
        --ROW_NUMBER() OVER (partition by player_id_nbacom, team_abbr, game_date ORDER BY fantasylabs_name_score desc) = 1
)

---

, linestar_game_player as (
    SELECT distinct
        ss.player_id_linestar
        , ss.game_date
        , REGEXP_REPLACE(ss.player_name,r'[^a-zA-Z]','') as player_name
        --, ss.team
        , lkp.team_abbr
    FROM {{ ref('nba__conf_linestar_ownership') }} ss 
        left join {{ ref('nba__transform_team_lookup') }} lkp
            on upper(ss.team) = lkp.look_up
)


, linestar_fuzzy_match_score as (
    SELECT 
        coalesce(n.game_date, s.game_date) as game_date
        , coalesce(n.team_abbr, s.team_abbr) as team_abbr
        , s.player_id_linestar
        , n.player_id_nbacom
        --, n.first_name as nbacom_first_name
        --, n.last_name as nbacom_last_name
        , n.nbacom_player_name
        , s.player_name as linestar_player_name
        , common.fuzzy_jaro_wrinkler_distance(s.player_name,n.nbacom_player_name) as linestar_name_score
    FROM nbacom_game_players n 
        join linestar_game_player s 
            on s.team_abbr = n.team_abbr -- blocking section
            and s.game_date = n.game_date
)


, linestar_fuzzy_match as (
    SELECT *
        , ROW_NUMBER() OVER (partition by player_id_linestar, game_date ORDER BY linestar_name_score desc) as linestar_score_rank
        , ROW_NUMBER() OVER (partition by player_id_nbacom, game_date ORDER BY linestar_name_score desc) as nbacom_score_rank
    FROM linestar_fuzzy_match_score
    WHERE linestar_name_score >= .75
    QUALIFY nbacom_score_rank = 1 and linestar_score_rank = 1
        --ROW_NUMBER() OVER (partition by player_id_nbacom, team_abbr, game_date ORDER BY linestar_name_score desc) = 1
)

, unique_nbacom_players as (
    SELECT 
        n.player_id_nbacom
        , max(n.nbacom_player_name) as nbacom_player_name
    FROM nbacom_game_players n
    GROUP BY n.player_id_nbacom
)

, unqiue_swish as (
    SELECT player_id_nbacom, player_id_swish, swish_player_name, swish_name_score
        , count(*) as cnt
        , row_number() over(partition by player_id_nbacom order by count(*) desc) as row_id
    FROM swish_fuzzy_match
    --WHERE player_id_nbacom = '1628404'
    GROUP BY player_id_nbacom, player_id_swish, swish_player_name, swish_name_score
    QUALIFY row_id = 1 
)
    
, unqiue_fantasylabs as (
    SELECT player_id_nbacom, player_id_fantasylabs, fantasylabs_player_name, fantasylabs_name_score
        , count(*) as cnt
        , row_number() over(partition by player_id_nbacom order by count(*) desc) as row_id
    FROM fantasylabs_fuzzy_match
    --WHERE player_id_nbacom = '1628404'
    GROUP BY player_id_nbacom, player_id_fantasylabs, fantasylabs_player_name, fantasylabs_name_score
    QUALIFY row_id = 1 
)

, unqiue_linestar as (
    SELECT player_id_nbacom, player_id_linestar, linestar_player_name, linestar_name_score
        , count(*) as cnt
        , row_number() over(partition by player_id_nbacom order by count(*) desc) as row_id
    FROM linestar_fuzzy_match
    --WHERE player_id_nbacom = '1628404'
    GROUP BY player_id_nbacom, player_id_linestar, linestar_player_name, linestar_name_score
    QUALIFY row_id = 1 
)

SELECT 
    n.player_id_nbacom
    , s.player_id_swish
    , f.player_id_fantasylabs
    , l.player_id_linestar

    , n.nbacom_player_name 
     
    , s.swish_player_name
    , s.swish_name_score
    
    , f.fantasylabs_player_name
    , f.fantasylabs_name_score

    , l.linestar_player_name
    , l.linestar_name_score
FROM unique_nbacom_players n
    left join unqiue_swish s
        on n.player_id_nbacom = s.player_id_nbacom
    left join unqiue_fantasylabs f
        on n.player_id_nbacom = f.player_id_nbacom
    left join unqiue_linestar l
        on n.player_id_nbacom = l.player_id_nbacom


/*
SELECT * 
FROM unique_nbacom_players
WHERE player_id_nbacom in ('1629244','1630214')
*/
/*
SELECT player_id_nbacom, count(*) as cnt
FROM unique_nbacom_players
GROUP BY player_id_nbacom
HAVING count(*) > 1
*/
--SELECT * FROM fantasylabs_fuzzy_match WHERE game_date = '2021-01-01' and player_id_nbacom = '1628391'
/*
SELECT 
    n.team_abbr
    , n.game_date
    , n.player_id_nbacom
    , s.player_id_swish
    , f.player_id_fantasylabs
    , l.player_id_linestar

    , n.nbacom_player_name 
     
    , s.swish_player_name
    , s.swish_name_score
    
    , f.fantasylabs_player_name
    , f.fantasylabs_name_score

    , l.linestar_player_name
    , l.linestar_name_score
FROM nbacom_game_players n
    left join swish_fuzzy_match s
        on n.team_abbr = s.team_abbr
        and n.game_date = s.game_date
        and n.player_id_nbacom = s.player_id_nbacom
    left join fantasylabs_fuzzy_match f
        on n.team_abbr = f.team_abbr
        and n.game_date = f.game_date
        and n.player_id_nbacom = f.player_id_nbacom
    left join linestar_fuzzy_match l
        on n.team_abbr = l.team_abbr
        and n.game_date = l.game_date
        and n.player_id_nbacom = l.player_id_nbacom
--WHERE 
    --n.game_date = '2019-11-12' and n.player_id_nbacom in ('1629671','201144')
    --n.game_date = '2021-01-01'
    --and n.nbacom_player_name in ('TRE JONES','TRAE YOUNG')
*/






