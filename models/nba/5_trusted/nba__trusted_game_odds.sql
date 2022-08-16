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
    , merge_update_columns = ['total','total_over_odds','total_under_odds','total_over_consensus','total_under_consensus','total_over_consensus_wagers'
                                ,'total_under_consensus_wagers','total_over_breakeven','total_under_breakeven','total_hold','total_over_implied_prob'
                                ,'total_under_implied_prob','total_datetime','open_total','open_total_over_odds','open_total_under_odds'
                                ,'total_open_diff','total_over_breakeven_open_diff','total_under_breakeven_open_diff','total_over_implied_prob_open_diff'
                                ,'total_under_implied_prob_open_diff''update_datetime']
) }}

SELECT 
    GENERATE_UUID() as game_odds_sk
    , g.game_sk || '|' || ou.period_type || '|' || ou.sports_book || '|' || ou.opening_line_flag as unique_key
    , g.game_sk 
    , g.game_date
    , ou.period_type as odds_period
    , ou.sports_book as odds_maker
    , ou.opening_line_flag
    , sbr.game_key as sbr_key
    , ou.total
    , ou.total_over_odds, ou.total_under_odds
    , ocon.consensus as total_over_consensus
    , ucon.consensus as total_under_consensus
    , ocon.consensus_wagers as total_over_consensus_wagers
    , ucon.consensus_wagers as total_under_consensus_wagers

    , ou.total_over_breakeven, ou.total_under_breakeven
    , ou.total_hold
    , ou.total_over_implied_prob, ou.total_under_implied_prob
    , ou.total_datetime

    , ou.open_total
    , ou.open_total_over_odds, ou.open_total_under_odds
    --, ou.open_total_over_breakeven, ou.open_total_under_breakeven
    --, ou.open_total_hold
    --, ou.open_total_over_implied_prob, ou.open_total_under_implied_prob
    --, ou.open_total_datetime

    , ou.total - ou.open_total as total_open_diff
    , ou.total_over_breakeven - ou.open_total_over_breakeven as total_over_breakeven_open_diff
    , ou.total_under_breakeven - ou.open_total_under_breakeven as total_under_breakeven_open_diff
    , ou.total_over_implied_prob - ou.open_total_over_implied_prob as total_over_implied_prob_open_diff
    , ou.total_under_implied_prob - ou.open_total_under_implied_prob as total_under_implied_prob_open_diff
            , ou.total_hold - ou.open_total_hold as total_hold_open_diff
    , CURRENT_DATETIME() as insert_datetime
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
    inner join 
        (SELECT 
            co.game_key, co.game_date, co.period_type, co.sports_book_key, co.sports_book, co.opening_line_flag
            , co.line_timestamp as total_datetime, co.line as total
            , co.odds as total_over_odds, cu.odds as total_under_odds
            , co.odds_breakeven as total_over_breakeven
            , cu.odds_breakeven as total_under_breakeven
            , (co.odds_breakeven + cu.odds_breakeven) - 1 as total_hold
            , round(co.odds_breakeven / (co.odds_breakeven + cu.odds_breakeven), 4) as total_over_implied_prob
            , round(cu.odds_breakeven / (co.odds_breakeven + cu.odds_breakeven), 4) as total_under_implied_prob
            , oo.line_timestamp as open_total_datetime, oo.line as open_total
            , oo.odds as open_total_over_odds, ou.odds as open_total_under_odds
            , oo.odds_breakeven as open_total_over_breakeven
            , ou.odds_breakeven as open_total_under_breakeven
            , (oo.odds_breakeven + ou.odds_breakeven) - 1 as open_total_hold
            , round(oo.odds_breakeven / (oo.odds_breakeven + ou.odds_breakeven), 4) as open_total_over_implied_prob
            , round(ou.odds_breakeven / (oo.odds_breakeven + ou.odds_breakeven), 4) as open_total_under_implied_prob
        FROM {{ ref('nba__conf_sbr_lines') }} co -- current over
            inner join {{ ref('nba__conf_sbr_lines') }} cu -- current under
                on co.game_key = cu.game_key
                and co.period_type = cu.period_type
                and co.sports_book_key = cu.sports_book_key
                and co.opening_line_flag = cu.opening_line_flag
                and cu.o_u = 'U'
                and cu.odds_type = 'TOTALS'
            inner join {{ ref('nba__conf_sbr_lines') }} oo -- open over
                on co.game_key = oo.game_key
                and co.period_type = oo.period_type
                and co.sports_book_key = oo.sports_book_key
                and oo.opening_line_flag = true
                and oo.o_u = 'O'
                and oo.odds_type = 'TOTALS'
            inner join {{ ref('nba__conf_sbr_lines') }} ou -- open under
                on co.game_key = ou.game_key
                and co.period_type = ou.period_type
                and co.sports_book_key = ou.sports_book_key
                and ou.opening_line_flag = true
                and ou.o_u = 'U'
                and ou.odds_type = 'TOTALS'
        WHERE 
            -- co.opening_line_flag = false
            co.o_u = 'O'
            and co.odds_type = 'TOTALS'
        ) ou
        on sbr.game_key = ou.game_key
    left join {{ ref('nba__conf_sbr_consensus') }} ocon 
        on sbr.game_key = ocon.game_key 
        and ou.period_type = ocon.period_type 
        and ocon.o_u = 'O'
        and ocon.odds_type = 'TOTALS'
    left join {{ ref('nba__conf_sbr_consensus') }} ucon 
        on sbr.game_key = ucon.game_key 
        and ou.period_type = ucon.period_type 
        and ucon.o_u = 'U'
        and ucon.odds_type = 'TOTALS'
{% if is_incremental() %}
    WHERE g.game_date >= (SELECT max(game_date) from {{ this }})
{% endif %}

    