{%- macro sql_gamedate_to_season(league, game_date_column) -%}
    {# convert game_date in select to a season #}
    {%- if league.lower() is in (['nba'])  -%}
        {%- set season_sql = "case when extract(month from " ~ game_date_column ~ ") >= 10 then extract(year from " ~ game_date_column ~ ") + 1 else extract(year from " ~ game_date_column ~ ") end" -%}
    {%- else -%}
        {{ exceptions.raise_compiler_error("Invalid league - not coded for yet: " ~ league) }}
    {%- endif -%}
    {{ season_sql }}
{%- endmacro -%}

