-- PITs are implemented as incremental dbt models
-- this way, the latest open record (2099) is ended before a new state is inserted
-- Remember, for performance reasons, and with respect of deleted_ts these periods
-- are physically saved as a table rather than calculated as a view.
-- (deleted_ts is already applied in the BV-views)

-- Codes are be spared out, since this might be static data and onyl typos are
-- corrected over time. So we might want to apply the latest code descriptions
-- over the entity's entire history.

{{
    config(
        materialized='incremental',
        unique_key=['hk_customer', 'load_ts']
    )
}}

with dates as (
	-- collect all timestamps when changes happened (detected in data warehouse)
	select
		hub.hk_customer,
		hub.load_ts hub_load_ts,
		sat1.load_ts as sat1_load_ts,
		/*sat2.load_ts as sat2_load_ts,*/
		sat3.load_ts as sat3_load_ts,
		sat4.load_ts as sat4_load_ts
	from {{ ref('h_customer') }} hub
	-- use outer joins where necessary (eg. codes or users)
	join {{ ref('s_customer') }} sat1
		on sat1.hk_customer = hub.hk_customer
	/*join {{ ref('s_customerinterest') }} sat2
		on sat2.hk_customer = hub.hk_customer*/
	join {{ ref('s_customer_extended') }} sat3
		on sat3.hk_customer = hub.hk_customer
	join {{ ref('s_customer_meta') }} sat4
		on sat4.hk_customer = hub.hk_customer
),
dates_unpivot as (
	select distinct
		dts.hk_customer,
		t.load_ts
	from dates dts
	cross join lateral (
		values
			(dts.hub_load_ts),
			(dts.sat1_load_ts),
			/*(dts.sat2_load_ts),*/
			(dts.sat3_load_ts),
			(dts.sat4_load_ts)
		) as t(load_ts)
),
periods as (
	-- build periods from these timestamps
	select
		dts.*,
		coalesce(
			lead(dts.load_ts) over(
				partition by
					dts.hk_customer
				order by
					dts.load_ts) - interval '1 us',
			cast('2099-12-31 23:59:59' as timestamp)) as loadend_ts
	from dates_unpivot dts
),
periods_data as (
	-- get the latest load_ts for each period
	select
		dts.hk_customer,
		hub.cst_customer_no, -- denorm for usability
		dts.load_ts,
		dts.loadend_ts,
		max(sat1.load_ts) as sat1_load_ts,
		/*tmax(sat2.load_ts) as sat2_load_ts,*/
		max(sat3.load_ts) as sat3_load_ts,
		max(sat4.load_ts) as sat4_load_ts
	from periods dts
	join {{ ref('h_customer') }} hub
		on hub.hk_customer = dts.hk_customer
	join {{ ref('v_customer') }} sat1
		on  sat1.hk_customer = hub.hk_customer
		and dts.load_ts between sat1.load_ts and sat1.loadend_ts
	/*join {{ ref('v_customerinterest') }} sat2
		on  sat2.hk_customer = hub.hk_customer
		and dts.load_ts between sat2.load_ts and sat2.loadend_ts*/
	join {{ ref('v_customer_extended') }} sat3
		on  sat3.hk_customer = hub.hk_customer
		and dts.load_ts between sat3.load_ts and sat3.loadend_ts
	join {{ ref('v_customer_meta') }} sat4
		on  sat4.hk_customer = hub.hk_customer
		and dts.load_ts between sat4.load_ts and sat4.loadend_ts
group by
	dts.hk_customer,
	hub.cst_customer_no,
	dts.load_ts,
	dts.loadend_ts
),
periods_merged as (
	-- merge consecutive values into one period (compress)
	select
		grp.hk_customer,
		grp.cst_customer_no,
		min(grp.load_ts) as load_ts,
		max(grp.loadend_ts) as loadend_ts,
	grp.sat1_load_ts,
	/*grp.sat2_load_ts,*/
	grp.sat3_load_ts,
	grp.sat4_load_ts
	from (
		--count occurances of values and arrange them in groups
		select
			ts.*,
			--business key - business key/values
			row_number() over(partition by ts.hk_customer order by ts.load_ts)
				- row_number() over(
					partition by
						ts.hk_customer,
					ts.sat1_load_ts,
					/*ts.sat2_load_ts,*/
					ts.sat3_load_ts,
					ts.sat4_load_ts
				order by
					ts.load_ts) as grp
			from periods_data ts
	) grp
	group by
		grp.hk_customer,
		grp.cst_customer_no,
		grp.sat1_load_ts,
		/*grp.sat2_load_ts,*/
		grp.sat3_load_ts,
		grp.sat4_load_ts,
		grp.grp
)
select
	decode(md5(concat_ws('||'::text,
	coalesce(nullif(trim(both from t.cst_customer_no::character varying), ''::text), '^^'::text),
	coalesce(nullif(trim(both from t.load_ts::character varying), ''::text), '^^'::text)
	)), 'hex'::text) as hk_customer_snapshot,
	t.*
from periods_merged t
order by
	t.hk_customer,
	t.load_ts
