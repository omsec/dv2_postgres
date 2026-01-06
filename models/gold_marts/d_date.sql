{{
    config(
        materialized='incremental',
        unique_key='d_date'
    )
}}

select
	to_char(dt.dt, 'yyyymmdd')::int as d_date,
	date(dt.dt) as dt,
	to_char(dt.dt, 'yyyy-mm-dd') as dt_str,
	extract(year from dt.dt) as dt_year,
	to_char(dt.dt, 'q')::int as dt_quarter,
	to_char(dt.dt, '"Q"q:yyyy') as dt_quarter_str,
	extract(month from dt.dt) as dt_month,
	to_char(dt.dt, 'Month') as dt_month_str,
	extract(DOY from dt.dt) as dt_day_year,
	date(dt.dt) - date_trunc('quarter', dt.dt)::date + 1 as dt_day_quarter,
	extract(day from dt.dt) as dt_day_month,
	case extract(DOW from dt.dt)
		when 0 then 7
		else extract(DOW from dt.dt)
	end as dt_day_week
from (
	select dt
	from generate_series(to_date('2010-01-01', 'yyyy-mm-dd'), to_date('2030-12-31', 'yyyy-mm-dd'), '1 day') as dt
	--from generate_series('2010-01-01'::date, '2030-12-31'::date, '1 day') as dt
) dt