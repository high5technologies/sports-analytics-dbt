{{ config(
    tags=["nba"]
    , labels = {'project': 'sports_analytics', 'league':'nba'}
    , partition_by = {
      'field': 'game_date',
      'data_type': 'date',
      'granularity': 'day'
    }
    , materialized='incremental'    
    , unique_key='unique_key'
    , merge_update_columns = ['w_l','win_bit','game_score','q1_score','q2_score','q3_score','q4_score','ot1_score'
                                ,'ot2_score','ot3_score','score_diff_game','score_diff_1h','score_diff_2h','score_diff_1q'
                                ,'score_diff_2q','score_diff_3q','score_diff_4q','game_pace','game_efg_perc','game_tov_perc'
                                ,'game_orb_perc','game_ft_fga','game_off_rtg','game_def_rtg','pre_carmelo','pre_carmelo_prob'
                                ,'post_carmelo','pre_elo','pre_elo_prob','post_elo','pre_raptor','pre_raptor_prob','pre_espn_prob'
                                ,'elo_f_d','raptor_f_d','elo_worth_game','elo_win_game','elo_worth_1h','elo_win_1h'
                                ,'elo_worth_1q','elo_win_1q','raptor_worth_game','raptor_win_game','raptor_worth_1h','raptor_win_1h'
                                ,'raptor_worth_1q','raptor_win_1q','espn_worth_game','espn_win_game','espn_worth_1h','espn_win_1h'
                                ,'espn_worth_1q','espn_win_1q'
                                ,'update_datetime']
) }}



SELECT 
    tg.game_sk
    , tg.game_date 
    , case when g.complete_flag and g.home_score > g.away_score then g.home_team_wins - 1 else g.home_team_wins end as team_wins 
    , case when g.complete_flag and g.home_score < g.away_score then g.home_team_losses - 1 else g.home_team_losses end as team_losses 
    , g.home_score as team_score
    , g.home_seed as team_seed

    , g.home_postgame_stats_points
    , g.home_postgame_stats_rebounds_total
    , g.home_postgame_stats_assists
    , g.home_postgame_stats_steals
    , g.home_postgame_stats_blocks
    , g.home_postgame_stats_turnovers
    , g.home_postgame_stats_field_goals_percentage
    , g.home_postgame_stats_three_pointers_percentage
    , g.home_postgame_stats_free_throws_percentage
    , g.home_postgame_stats_points_in_the_paint
    , g.home_postgame_stats_points_second_chance
    , g.home_postgame_stats_points_fast_break
    , g.home_postgame_stats_biggest_lead
    , g.home_postgame_stats_lead_changes
    , g.home_postgame_stats_times_tied
    , g.home_postgame_stats_biggest_scoring_run
    , g.home_postgame_stats_turnovers_team
    , g.home_postgame_stats_turnovers_total
    , g.home_postgame_stats_rebounds_team
    , g.home_postgame_stats_points_from_turnovers
    , g.home_postgame_stats_bench_points

    , g.home_starters_minutes
    , g.home_starters_field_goals_made
    , g.home_starters_field_goals_attempted
    , g.home_starters_field_goals_percentage
    , g.home_starters_three_pointers_made
    , g.home_starters_three_pointers_attempted
    , g.home_starters_three_pointers_percentage
    , g.home_starters_free_throws_made
    , g.home_starters_free_throws_attempted
    , g.home_starters_free_throws_percentage
    , g.home_starters_rebounds_offensive
    , g.home_starters_rebounds_defensive
    , g.home_starters_rebounds_total
    , g.home_starters_assists
    , g.home_starters_steals
    , g.home_starters_blocks
    , g.home_starters_turnovers
    , g.home_starters_foulsPersonal
    , g.home_starters_points
    , g.home_bench_minutes
    , g.home_bench_field_goals_made
    , g.home_bench_field_goals_attempted
    , g.home_bench_field_goals_percentage
    , g.home_bench_three_pointers_made
    , g.home_bench_three_pointers_attempted
    , g.home_bench_three_pointers_percentage
    , g.home_bench_free_throws_made
    , g.home_bench_free_throws_attempted
    , g.home_bench_free_throws_percentage
    , g.home_bench_rebounds_offensive
    , g.home_bench_rebounds_defensive
    , g.home_bench_rebounds_total
    , g.home_bench_assists
    , g.home_bench_steals
    , g.home_bench_blocks
    , g.home_bench_turnovers
    , g.home_bench_foulsPersonal
    , g.home_bench_points
    --, load_datetime
FROM {{ ref('nba__conf_nbacom_game') }} g
    inner join {{ ref('nba__trusted_game') }} tg
        on g.game_key = tg.game_key_nbacom
    left join {{ ref('nba__transform_team_lookup') }} lkp_nbacom
        on g.home_team_tricode = lkp_nbacom.look_up

   

    













/*
SELECT 
    GENERATE_UUID() as game_team_sk
    , game_sk || '|' || team_sk || '|' || h_a as unique_key
    , *
    , case when round(pre_elo_prob,3) = .5 then 'P' when round(pre_elo_prob,3) > .5 then 'F' when round(pre_elo_prob,3) < .5 then 'D' end as elo_f_d
    , case when round(pre_raptor_prob,3) = .5 then 'P' when round(pre_raptor_prob,3) > .5 then 'F' when round(pre_raptor_prob,3) < .5 then 'D' end as raptor_f_d
    , case 
        when score_diff_game > 0 then 1 - round(pre_elo_prob,3)
        when score_diff_game < 0 then - round(pre_elo_prob,3)
    end as elo_worth_game
    , case 
        when score_diff_game > 0 then 1
        when score_diff_game < 0 then 0
    end as elo_win_game
    , case 
        when score_diff_1h > 0 then 1 - round(pre_elo_prob,3)
        when score_diff_1h < 0 then - round(pre_elo_prob,3)
    end as elo_worth_1h
    , case 
        when score_diff_1h > 0 then 1
        when score_diff_1h < 0 then 0
    end as elo_win_1h
    , case 
        when score_diff_1q > 0 then 1 - round(pre_elo_prob,3)
        when score_diff_1q < 0 then - round(pre_elo_prob,3)
    end as elo_worth_1q
    , case 
        when score_diff_1q > 0 then 1
        when score_diff_1q < 0 then 0
    end as elo_win_1q
    
    , case 
        when score_diff_game > 0 then 1 - round(pre_raptor_prob,3)
        when score_diff_game < 0 then - round(pre_raptor_prob,3)
    end as raptor_worth_game
    , case 
        when score_diff_game > 0 then 1
        when score_diff_game < 0 then 0
    end as raptor_win_game
    , case 
        when score_diff_1h > 0 then 1 - round(pre_raptor_prob,3)
        when score_diff_1h < 0 then - round(pre_raptor_prob,3)
    end as raptor_worth_1h
    , case 
        when score_diff_1h > 0 then 1
        when score_diff_1h < 0 then 0
    end as raptor_win_1h
    , case 
        when score_diff_1q > 0 then 1 - round(pre_raptor_prob,3)
        when score_diff_1q < 0 then - round(pre_raptor_prob,3)
    end as raptor_worth_1q
    , case 
        when score_diff_1q > 0 then 1
        when score_diff_1q < 0 then 0
    end as raptor_win_1q

    , case 
        when score_diff_game > 0 then 1 - round(pre_espn_prob,3)
        when score_diff_game < 0 then - round(pre_espn_prob,3)
    end as espn_worth_game
    , case 
        when score_diff_game > 0 then 1
        when score_diff_game < 0 then 0
    end as espn_win_game
    , case 
        when score_diff_1h > 0 then 1 - round(pre_espn_prob,3)
        when score_diff_1h < 0 then - round(pre_espn_prob,3)
    end as espn_worth_1h
    , case 
        when score_diff_1h > 0 then 1
        when score_diff_1h < 0 then 0
    end as espn_win_1h
    , case 
        when score_diff_1q > 0 then 1 - round(pre_espn_prob,3)
        when score_diff_1q < 0 then - round(pre_espn_prob,3)
    end as espn_worth_1q
    , case 
        when score_diff_1q > 0 then 1
        when score_diff_1q < 0 then 0
    end as espn_win_1q
    , CURRENT_DATETIME() as insert_datetime
    , CURRENT_DATETIME() as update_datetime
FROM
    (SELECT 
        g.game_sk
        , t.team_sk
        , 'A' as h_a
        , brg.game_date
        , case when brg.win_h_a is null then null when brg.win_h_a = 'P' then 'P' when brg.win_h_a = 'A' then 'W' else 'L' end as w_l
        , case when brg.win_h_a is null then null when brg.win_h_a = 'P' then null when brg.win_h_a = 'A' then 1 else 0 end as win_bit
        , brg.visitor_pts as game_score
        , brg.a_g1_score as q1_score
        , brg.a_g2_score as q2_score
        , brg.a_g3_score as q3_score
        , brg.a_g4_score as q4_score
        , brg.a_g5_score as ot1_score
        , brg.a_g6_score as ot2_score
        , brg.a_g7_score as ot3_score

        , brg.visitor_pts - brg.home_pts as score_diff_game
        , (a_g1_score + a_g2_score) - (h_g1_score + h_g2_score) as score_diff_1h
        , (a_g3_score + a_g4_score) - (h_g3_score + h_g4_score) as score_diff_2h
        , a_g1_score - h_g1_score as score_diff_1q
        , a_g2_score - h_g2_score as score_diff_2q
        , a_g3_score - h_g3_score as score_diff_3q
        , a_g4_score - h_g4_score as score_diff_4q
        
        , brg.a_ff_pace as game_pace
        , brg.a_ff_efg_pct as game_efg_perc
        , brg.a_ff_tov_pct as game_tov_perc
        , brg.a_ff_orb_pct as game_orb_perc
        , brg.a_ff_ft_rate as game_ft_fga
        , brg.a_ff_off_rtg as game_off_rtg
        , brg.h_ff_off_rtg as game_def_rtg
        -- , (sum(brg.visitor_pts) over (partition by brg.away_abbr order by brg.game_date)) - brg.visitor_pts as pre_points_for
        -- , (sum(brg.home_pts) over (partition by brg.away_abbr order by brg.game_date)) - brg.home_pts as pre_points_against
        , p.carm_elo2_pre as pre_carmelo
        , p.carm_elo_prob2 as pre_carmelo_prob
        , p.carm_elo2_post as post_carmelo
        , p.elo2_pre as pre_elo
        , p.elo_prob2 as pre_elo_prob
        , p.elo2_post as post_elo
        , p.raptor2_pre as pre_raptor
        , p.raptor_prob2 as pre_raptor_prob
        , e.espn_away_win_perc as pre_espn_prob 
    FROM nba.vw_raw_basketballreference_game brg
        inner join nba.model_game g
            on brg.game_key = g.game_key
        inner join nba.vw_model_team_lookup lkp
            on brg.away_abbr = lkp.look_up
        inner join nba.model_team t
            on lkp.team_abbr = t.team_abbr
            and brg.season = t.season
        left join 
            (SELECT t.team_abbr, fte.*
            FROM nba.vw_raw_538_predictions fte
                inner join nba.vw_model_team_lookup t
                    on fte.team2 = t.look_up
            WHERE --fte.game_date >= (SELECT min(game_date) FROM nba.vw_raw_basketballreference_game WHERE season = var_season)
                fte.game_date between var_start_game_date and var_end_game_date
            ) p
            on g.game_date = p.game_date
            and lkp.team_abbr = p.team_abbr
        left join 
            (SELECT t.team_abbr, e.*
            FROM nba.vw_raw_espn_predictions e
                inner join nba.vw_model_team_lookup t
                    on e.away_abbr = t.look_up
            WHERE --fte.game_date >= (SELECT min(game_date) FROM nba.vw_raw_basketballreference_game WHERE season = var_season)
                e.game_date between var_start_game_date and var_end_game_date
            ) e
            on g.game_date = e.game_date
            and lkp.team_abbr = e.team_abbr
    {% if is_incremental() %}
        WHERE g.game_date >= (SELECT max(game_date) from {{ this }})
    {% endif %}
    UNION ALL 
    SELECT 
        g.game_sk
        , t.team_sk
        , 'H' as h_a
        , brg.game_date
        , case when brg.win_h_a is null then null when brg.win_h_a = 'P' then 'P' when brg.win_h_a = 'H' then 'W' else 'L' end as w_l
        , case when brg.win_h_a is null then null when brg.win_h_a = 'P' then null when brg.win_h_a = 'H' then 1 else 0 end as win_bit
        , brg.home_pts as game_score
        , brg.h_g1_score as q1_score
        , brg.h_g2_score as q2_score
        , brg.h_g3_score as q3_score
        , brg.h_g4_score as q4_score
        , brg.h_g5_score as ot1_score
        , brg.h_g6_score as ot2_score
        , brg.h_g7_score as ot3_score

        , brg.home_pts - brg.visitor_pts as score_diff_game
        , (h_g1_score + h_g2_score) - (a_g1_score + a_g2_score) as score_diff_1h
        , (h_g3_score + h_g4_score) - (a_g3_score + a_g4_score) as score_diff_2h
        , h_g1_score - a_g1_score as score_diff_1q
        , h_g2_score - a_g2_score as score_diff_2q
        , h_g3_score - a_g3_score as score_diff_3q
        , h_g4_score - a_g4_score as score_diff_4q

        , brg.h_ff_pace as game_pace
        , brg.h_ff_efg_pct as game_efg_perc
        , brg.h_ff_tov_pct as game_tov_perc
        , brg.h_ff_orb_pct as game_orb_perc
        , brg.h_ff_ft_rate as game_ft_fga
        , brg.h_ff_off_rtg as game_off_rtg
        , brg.a_ff_off_rtg as game_def_rtg
        -- , (sum(brg.home_pts) over (partition by brg.home_abbr order by brg.game_date)) - brg.home_pts as pre_points_for
        -- , (sum(brg.visitor_pts) over (partition by brg.home_abbr order by brg.game_date)) - brg.visitor_pts as pre_points_against
        , p.carm_elo1_pre as pre_carmelo
        , p.carm_elo_prob1 as pre_carmelo_prob
        , p.carm_elo1_post as post_carmelo
        , p.elo1_pre as pre_elo
        , p.elo_prob1 as pre_elo_prob
        , p.elo1_post as post_elo
        , p.raptor1_pre as pre_raptor
        , p.raptor_prob1 as pre_raptor_prob
        , e.espn_home_win_perc as pre_espn_prob 
    FROM nba.vw_raw_basketballreference_game brg
        inner join nba.model_game g
            on brg.game_key = g.game_key
        inner join nba.vw_model_team_lookup lkp
            on brg.home_abbr = lkp.look_up
        inner join nba.model_team t
            on lkp.team_abbr = t.team_abbr
            and brg.season = t.season
        left join 
            (SELECT t.team_abbr, fte.*
            FROM nba.vw_raw_538_predictions fte
                inner join nba.vw_model_team_lookup t
                    on fte.team1 = t.look_up
            WHERE --fte.game_date >= (SELECT min(game_date) FROM nba.vw_raw_basketballreference_game)
                fte.game_date between var_start_game_date and var_end_game_date
            ) p
            on g.game_date = p.game_date
            and lkp.team_abbr = p.team_abbr
        left join 
            (SELECT t.team_abbr, e.*
            FROM nba.vw_raw_espn_predictions e
                inner join nba.vw_model_team_lookup t
                    on e.home_abbr = t.look_up
            WHERE --fte.game_date >= (SELECT min(game_date) FROM nba.vw_raw_basketballreference_game WHERE season = var_season)
                e.game_date between var_start_game_date and var_end_game_date
            ) e
            on g.game_date = e.game_date
            and lkp.team_abbr = e.team_abbr
    {% if is_incremental() %}
        WHERE g.game_date >= (SELECT max(game_date) from {{ this }})
    {% endif %}
    ) a
        
 */   
            
