{{ config(
    tags=["nba"]
    ,materialized='table'
) }}
/*
NOTE: this could use a change to materialization
*/
with cte_base_lines as (
    SELECT 
        aml.game_key, aml.game_date, aml.period_type as odds_period, aml.sports_book as odds_maker
        , aml.opening_line_flag
        , aml.line_timestamp as ml_datetime
        , cast(aml.odds as INT64) as away_ml, cast(hml.odds as INT64) as home_ml
        , aml.odds_breakeven as away_ml_breakeven
        , hml.odds_breakeven as home_ml_breakeven
        , case when aml.odds_breakeven > hml.odds_breakeven then 'F' 
            when aml.odds_breakeven < hml.odds_breakeven then 'D'
            when aml.odds_breakeven = hml.odds_breakeven then 'P'
        end as away_ml_breakeven_f_d
        , case when aml.odds_breakeven > hml.odds_breakeven then 'D' 
            when aml.odds_breakeven < hml.odds_breakeven then 'F'
            when aml.odds_breakeven = hml.odds_breakeven then 'P'
        end as home_ml_breakeven_f_d
        , round(1 - (aml.odds_breakeven + hml.odds_breakeven),4) as ml_hold
        , round(aml.odds_breakeven / (aml.odds_breakeven + hml.odds_breakeven),4) as away_ml_implied_prob
        , round(hml.odds_breakeven / (aml.odds_breakeven + hml.odds_breakeven),4) as home_ml_implied_prob
        , amlcon.consensus as away_ml_consensus, amlcon.consensus_wagers as away_ml_consensus_wagers
        , hmlcon.consensus as home_ml_consensus, hmlcon.consensus_wagers as home_ml_consensus_wagers

        , asp.line_timestamp as spread_datetime
        , asp.line as away_spread, cast(asp.odds as INT64) as away_spread_odds
        , hsp.line as home_spread, cast(hsp.odds as INT64) as home_spread_odds
        , asp.odds_breakeven as away_spread_breakeven
        , hsp.odds_breakeven as home_spread_breakeven
        , round(1 - (asp.odds_breakeven + hsp.odds_breakeven),4) as spread_hold
        , round(asp.odds_breakeven / (asp.odds_breakeven + hsp.odds_breakeven),4) as away_spread_implied_prob
        , round(hsp.odds_breakeven / (asp.odds_breakeven + hsp.odds_breakeven),4) as home_spread_implied_prob
        , aspcon.consensus as away_spread_consensus, aspcon.consensus_wagers as away_spread_consensus_wagers
        , hspcon.consensus as home_spread_consensus, hspcon.consensus_wagers as home_spread_consensus_wagers

        , cast(oaml.odds as INT64) as open_away_ml, cast(ohml.odds as INT64) as open_home_ml
        , oasp.line as open_away_spread, cast(oasp.odds as INT64) as open_away_spread_odds
        , ohsp.line as open_home_spread, cast(ohsp.odds as INT64) as open_home_spread_odds

        -- diff calcs to open lines
        , (case when aml.odds > 0 then aml.odds - 100 else aml.odds + 100 end) - (case when oaml.odds > 0 then oaml.odds - 100 else oaml.odds + 100 end) as away_ml_open_diff
        , (case when hml.odds > 0 then hml.odds - 100 else hml.odds + 100 end) - (case when ohml.odds > 0 then ohml.odds - 100 else ohml.odds + 100 end) as home_ml_open_diff
        , round(1 - (aml.odds_breakeven + hml.odds_breakeven),4) - round(1 - (oaml.odds_breakeven + ohml.odds_breakeven),4) as ml_hold_open_diff
        , aml.odds_breakeven - oaml.odds_breakeven as away_ml_breakeven_open_diff
        , hml.odds_breakeven - ohml.odds_breakeven as home_ml_breakeven_open_diff
        , round(aml.odds_breakeven / (aml.odds_breakeven + hml.odds_breakeven),4) - round(oaml.odds_breakeven / (oaml.odds_breakeven + ohml.odds_breakeven),4) as away_ml_implied_prob_open_diff
        , round(hml.odds_breakeven / (aml.odds_breakeven + hml.odds_breakeven),4) - round(ohml.odds_breakeven / (oaml.odds_breakeven + ohml.odds_breakeven),4) as home_ml_implied_prob_open_diff
        , asp.line - oasp.line as away_spread_open_diff
        , hsp.line - ohsp.line as home_spread_open_diff
        , round(1 - (asp.odds_breakeven + hsp.odds_breakeven),4) - round(1 - (oasp.odds_breakeven + ohsp.odds_breakeven),4) as spread_hold_open_diff
        , asp.odds_breakeven - oasp.odds_breakeven as away_spread_breakeven_open_diff
        , hsp.odds_breakeven - ohsp.odds_breakeven as home_spread_breakeven_open_diff
        , round(asp.odds_breakeven / (asp.odds_breakeven + hsp.odds_breakeven),4) - round(oasp.odds_breakeven / (oasp.odds_breakeven + ohsp.odds_breakeven),4) as away_spread_implied_prob_open_diff
        , round(hsp.odds_breakeven / (asp.odds_breakeven + hsp.odds_breakeven),4) - round(ohsp.odds_breakeven / (oasp.odds_breakeven + ohsp.odds_breakeven),4) as home_spread_implied_prob_open_diff

    FROM {{ ref('nba__conf_sbr_lines') }} aml -- away ml
        inner join {{ ref('nba__conf_sbr_lines') }} hml -- home ml
            on aml.game_key = hml.game_key
            and aml.period_type = hml.period_type
            and aml.sports_book = hml.sports_book
            and aml.opening_line_flag = hml.opening_line_flag
            and hml.odds_type = 'ML'
            and hml.h_a = 'H'
        inner join {{ ref('nba__conf_sbr_lines') }} asp -- away spread
            on aml.game_key = asp.game_key
            and aml.period_type = asp.period_type
            and aml.sports_book = asp.sports_book
            and aml.opening_line_flag = asp.opening_line_flag
            and asp.odds_type = 'SPREAD'
            and asp.h_a = 'A'
        inner join {{ ref('nba__conf_sbr_lines') }} hsp -- home spread
            on aml.game_key = hsp.game_key
            and aml.period_type = hsp.period_type
            and aml.sports_book = hsp.sports_book
            and aml.opening_line_flag = hsp.opening_line_flag
            and hsp.odds_type = 'SPREAD'
            and hsp.h_a = 'H'

        inner join {{ ref('nba__conf_sbr_lines') }} oaml -- open away ml
            on aml.game_key = oaml.game_key
            and aml.period_type = oaml.period_type
            and aml.sports_book = oaml.sports_book
            and oaml.odds_type = 'ML'
            and oaml.h_a = 'A'
            and oaml.opening_line_flag = true
        inner join {{ ref('nba__conf_sbr_lines') }} ohml -- open home ml
            on aml.game_key = ohml.game_key
            and aml.period_type = ohml.period_type
            and aml.sports_book = ohml.sports_book
            and ohml.odds_type = 'ML'
            and ohml.h_a = 'H'
            and ohml.opening_line_flag = true
        inner join {{ ref('nba__conf_sbr_lines') }} oasp -- open away spread
            on aml.game_key = oasp.game_key
            and aml.period_type = oasp.period_type
            and aml.sports_book = oasp.sports_book
            and oasp.odds_type = 'SPREAD'
            and oasp.h_a = 'A'
            and oasp.opening_line_flag = true
        inner join {{ ref('nba__conf_sbr_lines') }} ohsp -- open home spread
            on aml.game_key = ohsp.game_key
            and aml.period_type = ohsp.period_type
            and aml.sports_book = ohsp.sports_book
            and ohsp.odds_type = 'SPREAD'
            and ohsp.h_a = 'H'
            and ohsp.opening_line_flag = true
        left join {{ ref('nba__conf_sbr_consensus') }} amlcon 
            on aml.game_key = amlcon.game_key 
            and aml.period_type = amlcon.period_type 
            and amlcon.h_a = 'A'
            and amlcon.odds_type = 'ML'
        left join {{ ref('nba__conf_sbr_consensus') }} hmlcon 
            on aml.game_key = hmlcon.game_key 
            and aml.period_type = hmlcon.period_type 
            and hmlcon.h_a = 'H'
            and hmlcon.odds_type = 'ML'
        left join {{ ref('nba__conf_sbr_consensus') }} aspcon 
            on aml.game_key = aspcon.game_key 
            and aml.period_type = aspcon.period_type 
            and aspcon.h_a = 'A'
            and aspcon.odds_type = 'SPREAD'
        left join {{ ref('nba__conf_sbr_consensus') }} hspcon 
            on aml.game_key = hspcon.game_key 
            and aml.period_type = hspcon.period_type 
            and hspcon.h_a = 'H'
            and hspcon.odds_type = 'SPREAD'
    WHERE 
        aml.odds_type = 'ML' 
        and aml.h_a = 'A'
        --and aml.game_date between var_start_game_date and var_end_game_date
        -- and aml.opening_line_flag = false
        -- and aml.game_key = '3871425' and aml.period_type = 'FULL'
)

, cte_calcs1 as (
    SELECT 
        g.game_sk
        , agt.game_team_sk as away_game_team_sk
        , hgt.game_team_sk as home_game_team_sk
        , agt.w_l as away_w_l
        , hgt.w_l as home_w_l
        , agt.elo_prob_pre as away_elo_prob_pre, hgt.elo_prob_pre as home_elo_prob_pre
        , agt.raptor_prob_pre as away_raptor_prob_pre, hgt.raptor_prob_pre as home_raptor_prob_pre
        , case when l.odds_period != 'FULL' or agt.carm_elo_prob_pre is null or hgt.carm_elo_prob_pre is null then null 
                when agt.carm_elo_prob_pre > l.away_ml_breakeven and hgt.carm_elo_prob_pre > l.home_ml_breakeven then 'B' 
                when agt.carm_elo_prob_pre < l.away_ml_breakeven and hgt.carm_elo_prob_pre < l.home_ml_breakeven then 'M' 
                when agt.carm_elo_prob_pre > l.away_ml_breakeven then 'Y' else 'N' end as away_carmelo_ml_pick
        , case when l.odds_period != 'FULL' or agt.carm_elo_prob_pre is null or hgt.carm_elo_prob_pre is null then null 
                when agt.carm_elo_prob_pre > l.away_ml_breakeven and hgt.carm_elo_prob_pre > l.home_ml_breakeven then 'B' 
                when agt.carm_elo_prob_pre < l.away_ml_breakeven and hgt.carm_elo_prob_pre < l.home_ml_breakeven then 'M' 
                when hgt.carm_elo_prob_pre > l.home_ml_breakeven then 'Y' else 'N' end as home_carmelo_ml_pick
        , case when l.odds_period != 'FULL' or agt.elo_prob_pre is null or hgt.elo_prob_pre is null then null 
                when agt.elo_prob_pre > l.away_ml_breakeven and hgt.elo_prob_pre > l.home_ml_breakeven then 'B' 
                when agt.elo_prob_pre < l.away_ml_breakeven and hgt.elo_prob_pre < l.home_ml_breakeven then 'M' 
                when agt.elo_prob_pre > l.away_ml_breakeven then 'Y' else 'N' end as away_elo_ml_pick
        , case when l.odds_period != 'FULL' or agt.elo_prob_pre is null or hgt.elo_prob_pre is null then null 
                when agt.elo_prob_pre > l.away_ml_breakeven and hgt.elo_prob_pre > l.home_ml_breakeven then 'B'
                when agt.elo_prob_pre < l.away_ml_breakeven and hgt.elo_prob_pre < l.home_ml_breakeven then 'M' 
                when hgt.elo_prob_pre > l.home_ml_breakeven then 'Y' else 'N' end as home_elo_ml_pick
        , case when l.odds_period != 'FULL' or agt.raptor_prob_pre is null or hgt.raptor_prob_pre is null then null 
                when agt.raptor_prob_pre > l.away_ml_breakeven and hgt.raptor_prob_pre > l.home_ml_breakeven then 'B' 
                when agt.raptor_prob_pre < l.away_ml_breakeven and hgt.raptor_prob_pre < l.home_ml_breakeven then 'M' 
                when agt.raptor_prob_pre > l.away_ml_breakeven then 'Y' else 'N' end as away_raptor_ml_pick
        , case when l.odds_period != 'FULL' or agt.raptor_prob_pre is null or hgt.raptor_prob_pre is null then null 
                when agt.raptor_prob_pre > l.away_ml_breakeven and hgt.raptor_prob_pre > l.home_ml_breakeven then 'B' 
                when agt.raptor_prob_pre < l.away_ml_breakeven and hgt.raptor_prob_pre < l.home_ml_breakeven then 'M' 
                when hgt.raptor_prob_pre > l.home_ml_breakeven then 'Y' else 'N' end as home_raptor_ml_pick
        
        /*, case when l.odds_period != 'FULL' or agt.pre_espn_prob is null or hgt.pre_espn_prob is null then null 
                when agt.pre_espn_prob > l.away_ml_breakeven and hgt.pre_espn_prob > l.home_ml_breakeven then 'B' 
                when agt.pre_espn_prob < l.away_ml_breakeven and hgt.pre_espn_prob < l.home_ml_breakeven then 'M' 
                when agt.pre_espn_prob > l.away_ml_breakeven then 'Y' else 'N' end as away_espn_ml_pick
        , case when l.odds_period != 'FULL' or agt.pre_espn_prob is null or hgt.pre_espn_prob is null then null 
                when agt.pre_espn_prob > l.away_ml_breakeven and hgt.pre_espn_prob > l.home_ml_breakeven then 'B' 
                when agt.pre_espn_prob < l.away_ml_breakeven and hgt.pre_espn_prob < l.home_ml_breakeven then 'M' 
                when hgt.pre_espn_prob > l.home_ml_breakeven then 'Y' else 'N' end as home_espn_ml_pick
        */
        , agt.score_diff_game as away_score_diff_game
        /*, agt.score_diff_1h as away_score_diff_1h
        , agt.score_diff_2h as away_score_diff_2h
        , agt.score_diff_1q as away_score_diff_1q
        , agt.score_diff_2q as away_score_diff_2q
        , agt.score_diff_3q as away_score_diff_3q
        , agt.score_diff_4q as away_score_diff_4q
        */
        , hgt.score_diff_game as home_score_diff_game
        /*, hgt.score_diff_1h as home_score_diff_1h
        , hgt.score_diff_2h as home_score_diff_2h
        , hgt.score_diff_1q as home_score_diff_1q
        , hgt.score_diff_2q as home_score_diff_2q
        , hgt.score_diff_3q as home_score_diff_3q
        , hgt.score_diff_4q as home_score_diff_4q
        */
        , l.*
        /*, case when gt.carm_elo_prob_pre > ml_breakeven then 'T' else 'O' end as carmelo_ml_pick
        , case when gt.elo_prob_pre > ml_breakeven then 'T' else 'O' end as elo_ml_pick
        , case when gt.raptor_prob_pre > ml_breakeven then 'T' else 'O' end as raptor_ml_pick
        , case when (gt.carm_elo_prob_pre > sbrml.ml_breakeven and gt.w_l = 'W') or (gt.carm_elo_prob_pre <= sbrml.ml_breakeven and gt.w_l = 'L') then 1 else 0 end as carmelo_ml_pick_correct
        , case when (gt.elo_prob_pre > sbrml.ml_breakeven and gt.w_l = 'W') or (gt.elo_prob_pre <= sbrml.ml_breakeven and gt.w_l = 'L') then 1 else 0 end as elo_ml_pick_correct
        , case when (gt.raptor_prob_pre > sbrml.ml_breakeven and gt.w_l = 'W') or (gt.raptor_prob_pre <= sbrml.ml_breakeven and gt.w_l = 'L') then 1 else 0 end as raptor_ml_pick_correct
        */
    FROM {{ ref('nba__transform_sbr_game_team') }} sbr 
        inner join {{ ref('nba__trusted_game') }} g 
            on sbr.game_date = g.game_date 
        inner join {{ ref('nba__trusted_game_team') }} hgt 
            on g.game_sk = hgt.game_sk
        inner join {{ ref('nba__trusted_team') }} ht 
            on hgt.team_sk = ht.team_sk
            and sbr.home_team_abbr = ht.team_abbr 
        inner join {{ ref('nba__trusted_game_team') }} agt 
            on g.game_sk = agt.game_sk 
            and agt.h_a = 'A'
        inner join cte_base_lines l
            on sbr.game_key = l.game_key
    
)

, cte_calcs2 as (
    SELECT 
        case when away_w_l is null or coalesce(away_carmelo_ml_pick,'') != 'Y' then null when away_w_l = 'W' then true else false end as away_carmelo_ml_pick_correct
        ,case when home_w_l is null or coalesce(home_carmelo_ml_pick,'') != 'Y' then null when home_w_l = 'W' then true else false end as home_carmelo_ml_pick_correct
        ,case when away_w_l is null or coalesce(away_elo_ml_pick,'') != 'Y' then null when away_w_l = 'W' then true else false end as away_elo_ml_pick_correct
        ,case when home_w_l is null or coalesce(home_elo_ml_pick,'') != 'Y' then null when home_w_l = 'W' then true else false end as home_elo_ml_pick_correct
        ,case when away_w_l is null or coalesce(away_raptor_ml_pick,'') != 'Y' then null when away_w_l = 'W' then true else false end as away_raptor_ml_pick_correct
        ,case when home_w_l is null or coalesce(home_raptor_ml_pick,'') != 'Y' then null when home_w_l = 'W' then true else false end as home_raptor_ml_pick_correct
        
        --,case when away_w_l is null or coalesce(away_espn_ml_pick,'') != 'Y' then null when away_w_l = 'W' then true else false end as away_espn_ml_pick_correct
        --,case when home_w_l is null or coalesce(home_espn_ml_pick,'') != 'Y' then null when home_w_l = 'W' then true else false end as home_espn_ml_pick_correct
        
        ,case when away_w_l is null or away_ml is null then null 
                when odds_period = 'FULL' then case when away_score_diff_game = 0 then 'P' when away_score_diff_game > 0 then 'Y' else 'N' end
                /*when odds_period = '1H' then case when away_score_diff_1h = 0 then 'P' when away_score_diff_1h > 0 then 'Y' else 'N' end
                when odds_period = '2H' then case when away_score_diff_2h = 0 then 'P' when away_score_diff_2h > 0 then 'Y' else 'N' end
                when odds_period = '1Q' then case when away_score_diff_1q = 0 then 'P' when away_score_diff_1q > 0 then 'Y' else 'N' end
                when odds_period = '2Q' then case when away_score_diff_2q = 0 then 'P' when away_score_diff_2q > 0 then 'Y' else 'N' end
                when odds_period = '3Q' then case when away_score_diff_3q = 0 then 'P' when away_score_diff_3q > 0 then 'Y' else 'N' end
                when odds_period = '4Q' then case when away_score_diff_4q = 0 then 'P' when away_score_diff_4q > 0 then 'Y' else 'N' end
                */
        end as away_ml_correct
        ,case when home_w_l is null or home_ml is null then null 
                when odds_period = 'FULL' then case when home_score_diff_game = 0 then 'P' when home_score_diff_game > 0 then 'Y' else 'N' end
                /*when odds_period = '1H' then case when home_score_diff_1h = 0 then 'P' when home_score_diff_1h > 0 then 'Y' else 'N' end
                when odds_period = '2H' then case when home_score_diff_2h = 0 then 'P' when home_score_diff_2h > 0 then 'Y' else 'N' end
                when odds_period = '1Q' then case when home_score_diff_1q = 0 then 'P' when home_score_diff_1q > 0 then 'Y' else 'N' end
                when odds_period = '2Q' then case when home_score_diff_2q = 0 then 'P' when home_score_diff_2q > 0 then 'Y' else 'N' end
                when odds_period = '3Q' then case when home_score_diff_3q = 0 then 'P' when home_score_diff_3q > 0 then 'Y' else 'N' end
                when odds_period = '4Q' then case when home_score_diff_4q = 0 then 'P' when home_score_diff_4q > 0 then 'Y' else 'N' end
                */
        end as home_ml_correct
        ,case when away_w_l is null or away_spread is null then null 
                when odds_period = 'FULL' then case when away_score_diff_game = (away_spread * -1) then 'P' when away_score_diff_game > (away_spread * -1) then 'Y' else 'N' end
                /*when odds_period = '1H' then case when away_score_diff_1h = (away_spread * -1) then 'P' when away_score_diff_1h > (away_spread * -1)  then 'Y' else 'N' end
                when odds_period = '2H' then case when away_score_diff_2h = (away_spread * -1) then 'P' when away_score_diff_2h > (away_spread * -1)  then 'Y' else 'N' end
                when odds_period = '1Q' then case when away_score_diff_1q = (away_spread * -1) then 'P' when away_score_diff_1q > (away_spread * -1)  then 'Y' else 'N' end
                when odds_period = '2Q' then case when away_score_diff_2q = (away_spread * -1) then 'P' when away_score_diff_2q > (away_spread * -1)  then 'Y' else 'N' end
                when odds_period = '3Q' then case when away_score_diff_3q = (away_spread * -1) then 'P' when away_score_diff_3q > (away_spread * -1)  then 'Y' else 'N' end
                when odds_period = '4Q' then case when away_score_diff_4q = (away_spread * -1) then 'P' when away_score_diff_4q > (away_spread * -1)  then 'Y' else 'N' end
                */
        end as away_spread_correct
        ,case when home_w_l is null or home_spread is null then null 
                when odds_period = 'FULL' then case when home_score_diff_game = (home_spread * -1) then 'P' when home_score_diff_game > (home_spread * -1) then 'Y' else 'N' end
                /*when odds_period = '1H' then case when home_score_diff_1h = (home_spread * -1) then 'P' when home_score_diff_1h > (home_spread * -1)  then 'Y' else 'N' end
                when odds_period = '2H' then case when home_score_diff_2h = (home_spread * -1) then 'P' when home_score_diff_2h > (home_spread * -1)  then 'Y' else 'N' end
                when odds_period = '1Q' then case when home_score_diff_1q = (home_spread * -1) then 'P' when home_score_diff_1q > (home_spread * -1)  then 'Y' else 'N' end
                when odds_period = '2Q' then case when home_score_diff_2q = (home_spread * -1) then 'P' when home_score_diff_2q > (home_spread * -1)  then 'Y' else 'N' end
                when odds_period = '3Q' then case when home_score_diff_3q = (home_spread * -1) then 'P' when home_score_diff_3q > (home_spread * -1)  then 'Y' else 'N' end
                when odds_period = '4Q' then case when home_score_diff_4q = (home_spread * -1) then 'P' when home_score_diff_4q > (home_spread * -1)  then 'Y' else 'N' end
                */
        end as home_spread_correct
        , d.*
    FROM cte_calcs1 d
)

SELECT 
    cast(case 
                when away_carmelo_ml_pick_correct = false then -1 
                when away_carmelo_ml_pick = 'Y' and away_ml < 0 then 
                    round(((100 * 100) / abs(away_ml))/100,2)
                when away_carmelo_ml_pick = 'Y' and away_ml > 0 then
                    round(away_ml / 100,2)
                else null
            end as numeric) as away_carmelo_ml_pick_result
    , cast(case 
                when home_carmelo_ml_pick_correct = false then -1 
                when home_carmelo_ml_pick = 'Y' and home_ml < 0 then 
                    round(((100 * 100) / abs(home_ml))/100,2)
                when home_carmelo_ml_pick = 'Y' and home_ml > 0 then
                    round(home_ml / 100,2)
                else null
            end as numeric) as home_carmelo_ml_pick_result
    , cast(case 
                when away_elo_ml_pick_correct = false then -1 
                when away_elo_ml_pick = 'Y' and away_ml < 0 then 
                    round(((100 * 100) / abs(away_ml))/100,2)
                when away_elo_ml_pick = 'Y' and away_ml > 0 then
                    round(away_ml / 100,2)
                else null
            end as numeric) as away_elo_ml_pick_result
    , cast(case 
                when home_elo_ml_pick_correct = false then -1 
                when home_elo_ml_pick = 'Y' and home_ml < 0 then 
                    round(((100 * 100) / abs(home_ml))/100,2)
                when home_elo_ml_pick = 'Y' and home_ml > 0 then
                    round(home_ml / 100,2)
                else null
            end as numeric) as home_elo_ml_pick_result
    , cast(case 
                when away_raptor_ml_pick_correct = false then -1 
                when away_raptor_ml_pick = 'Y' and away_ml < 0 then 
                    round(((100 * 100) / abs(away_ml))/100,2)
                when away_raptor_ml_pick = 'Y' and away_ml > 0 then
                    round(away_ml / 100,2)
                else null
            end as numeric) as away_raptor_ml_pick_result
    , cast(case 
                when home_raptor_ml_pick_correct = false then -1 
                when home_raptor_ml_pick = 'Y' and home_ml < 0 then 
                    round(((100 * 100) / abs(home_ml))/100,2)
                when home_raptor_ml_pick = 'Y' and home_ml > 0 then
                    round(home_ml / 100,2)
                else null
            end as numeric) as home_raptor_ml_pick_result   

    /*, cast(case 
                when away_espn_ml_pick_correct = false then -1 
                when away_espn_ml_pick = 'Y' and away_ml < 0 then 
                    round(((100 * 100) / abs(away_ml))/100,2)
                when away_espn_ml_pick = 'Y' and away_ml > 0 then
                    round(away_ml / 100,2)
                else null
            end as numeric) as away_espn_ml_pick_result
    , cast(case 
                when home_espn_ml_pick_correct = false then -1 
                when home_espn_ml_pick = 'Y' and home_ml < 0 then 
                    round(((100 * 100) / abs(home_ml))/100,2)
                when home_espn_ml_pick = 'Y' and home_ml > 0 then
                    round(home_ml / 100,2)
                else null
            end as numeric) as home_espn_ml_pick_result   
    */
    , cast(case 
                when away_ml_correct = 'P' then 0
                when away_ml_correct = 'N' then -1
                when away_ml_correct = 'Y' and away_ml < 0 then
                    round(((100 * 100) / abs(away_ml))/100,2)
                when away_ml_correct = 'Y' and away_ml > 0 then
                    round(away_ml / 100,2)
                else null
            end as numeric) as away_ml_result
    , cast(case 
                when home_ml_correct = 'P' then 0
                when home_ml_correct = 'N' then -1
                when home_ml_correct = 'Y' and home_ml < 0 then
                    round(((100 * 100) / abs(home_ml))/100,2)
                when home_ml_correct = 'Y' and home_ml > 0 then
                    round(home_ml / 100,2)
                else null
            end as numeric) as home_ml_result
    , cast(case 
                when away_spread_correct = 'P' then 0
                when away_spread_correct = 'N' then -1
                when away_spread_correct = 'Y' and away_spread_odds < 0 then
                    round(((100 * 100) / abs(away_spread_odds))/100,2)
                when away_spread_correct = 'Y' and away_spread_odds > 0 then
                    round(away_spread_odds / 100,2)
                else null
            end as numeric) as away_spread_result
    , cast(case 
                when home_spread_correct = 'P' then 0
                when home_spread_correct = 'N' then -1
                when home_spread_correct = 'Y' and home_spread_odds < 0 then
                    round(((100 * 100) / abs(home_spread_odds))/100,2)
                when home_spread_correct = 'Y' and home_spread_odds > 0 then
                    round(home_spread_odds / 100,2)
                else null
            end as numeric) as home_spread_result
    , * 
FROM cte_calcs2
