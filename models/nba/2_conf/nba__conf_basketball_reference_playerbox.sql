{{ config(
    tags=["nba"]
) }}

SELECT *
    , split(player_name,' ')[ORDINAL(1)] as player_first_name
    , split(player_name,' ')[ORDINAL(array_length(split(player_name,' ')))] as player_last_name
	, left(player_name, 1) as player_first_name_initial
    , left(split(player_name,' ')[ORDINAL(array_length(split(player_name,' ')))], 1) as player_last_name_initial

FROM 
    (SELECT  
        player_stat_key
        ,game_key
        ,player_key
        ,game_date
        ,stat_period
        ,h_or_a
        ,team_abbrev as team_abbr
        , replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(
			  replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(
                replace(replace(replace(upper(trim(player)),'\'',''),'.',''),'\\\'','')
			, '\\XC3\\XA9','E'),'\\XC3\\XB3','O'),'\\XC4\\X87','C'),'\\XC3\\XA8','E'),'\\XC3\\XB3','O'),'\\XC4\\X8D','C'),'\\XC5\\XAB','U'),'\\XC4\\X81','A'),'\\XC5\\XBD','Z'),'\\XC5\\XBE','Z'),'\\XC4\\XB0','I')
			   ,'\\XC5\\XA0','S'),'\\XC3\\XB6','O'),'\\XC3\\XA1','A'),'\\XC5\\XA1','S'),'\\XC3\\XBD','Y'),'\\XC3\\XAD','I'),'\\XC3\\X81','A'),'\\XC4\\X8C','C'),'\\XC3\\XB2','O'),'\\XC5\\X86','N'),'\\XC4\\XA3','G'),'\\XC3\\XAA','E') as player_name
        ,player_link
        ,starter_flag
        ,reason	
        , mp as mp_text 
		--, round(cast(SUBSTRING_INDEX(mp,':',1) as unsigned integer) + (cast(SUBSTRING_INDEX(mp,':',-1) as unsigned integer) / 60),3) as mp_min
		, cast(round(cast(split(mp,':')[ORDINAL(1)] as INT64) + (cast(split(mp,':')[ORDINAL(2)] as INT64) / 60),3) as NUMERIC) as mp_min
        ,cast(fg as INT64) as fg	
        ,cast(fga as INT64) as fga
        ,cast(fg_pct as NUMERIC) as fg_pct
        ,cast(fg3 as INT64) as fg3
        ,cast(fg3a as INT64) as fg3a
        ,cast(fg3_pct as NUMERIC) as fg3_pct
        ,cast(ft as INT64) as ft 
        ,cast(fta as INT64) as fta 
        ,cast(ft_pct as NUMERIC) as ft_pct  
        ,cast(orb as INT64) as orb 
        ,cast(drb as INT64) as drb 
        ,cast(trb as INT64) as trb 
        ,cast(ast as INT64) as ast 
        ,cast(stl as INT64) as stl 
        ,cast(blk as INT64) as blk 
        ,cast(tov as INT64) as tov  
        ,cast(pf as INT64) as pf
        ,cast(pts as INT64) as pts
        , cast(replace(plus_minus,'+','') as INT64) as plus_minus
        ,LOAD_DATETIME
        , row_number() over (partition by player_stat_key order by load_datetime desc) as dedup
    FROM {{ source('nba_raw','raw_basketballreference_playerbox') }}
    ) a
WHERE dedup = 1

