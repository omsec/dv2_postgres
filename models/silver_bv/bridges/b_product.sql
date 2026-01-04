-- Bridges are implemented as incremental dbt models
-- this way, the latest open record (2099) is ended before a new state is inserted
-- Remember, for performance reasons, and with respect of deleted_at these periods
-- are physically saved as a table rather than calculated as a view.
-- (deleted_ts is already applied in the BV-views)

-- Codes are be spared out, since this might be static data and onyl typos are
-- corrected over time. So we might want to apply the latest code descriptions
-- over the entity's entire history.

{{
    config(
        materialized='incremental',
        unique_key=['hk_product', 'load_ts']
    )
}}

-- build bridge for debug-view

with dates as (
	-- collect all timestamps when changes happened (detected in data warehouse)
	select
		hPrd.hk_product,
		hPrd.load_ts as hPrd_load_ts,
		sPrd.load_ts as sPrd_load_ts,
		lPrdPct.load_ts as lPrdPct_load_ts,
		sPrdPct.load_ts as sPrdPct_load_ts,
		hPct.load_ts as hPct_load_ts,
		sPct.load_ts as sPct_load_ts
	from {{ ref('h_product') }} hPrd
	join {{ ref('s_product') }} sPrd
		on sPrd.hk_product = hPrd.hk_product
	join {{ ref('l_product_productcategory') }} lPrdPct
		on lPrdPct.hk_product = hPrd.hk_product
	join {{ ref('s_product_productcategory') }} sPrdPct
		on sPrdPct.hk_product_productcategory = lPrdPct.hk_product_productcategory
	join {{ ref('h_productcategory') }} hPct
		on hPct.hk_productcategory = lPrdPct.hk_productcategory
	join {{ ref('s_productcategory') }} sPct
		on sPct.hk_productcategory = hPct.hk_productcategory
	--where hPrd.product_bk = '2'
),
dates_unpivot as (
-- https://blog.sql-workbench.eu/post/unpivot-with-postgres/
	select distinct
		dts.hk_product,
		t.load_ts
	from dates dts
	cross join lateral (
		values
			(dts.hPrd_load_ts),
			(dts.sPrd_load_ts),
			(lPrdPct_load_ts),
			(sPrdPct_load_ts),
			(hPct_load_ts),
			(sPct_load_ts)
	) as t(load_ts)
),
periods as (
-- build periods from these timestamps
	select
		dts.*
	from (
		select
			dts.*,
			coalesce(
				lead(dts.load_ts) over(
					partition by
						dts.hk_product
					order by
						dts.load_ts) - interval '1 us',
				to_timestamp('2099-12-31 23:59:59', 'yyyy-mm-dd hh24:mi:ss')) as loadend_ts
		from dates_unpivot dts
		--order by dts.hk_customer, dts.load_ts
	) dts
),
periods_data as (
	select
		dts.hk_product,
		hPrd.product_bk,
		dts.load_ts,
		dts.loadend_ts,
		sPrd.load_ts as sPrd_load_ts,
		lPrdPct.hk_product_productcategory,
		sPrdPct.load_ts as sPrdPct_load_ts,
		--sPrdPct.prd_pct_start,
		--sPrdPCt.prd_pct_end
		lPrdPct.hk_productcategory,
		hPct.productcategory_bk,
		sPct.load_ts as sPct_load_ts
	from periods dts
	-- this is the "driving hub"
	-- this hub and its satellites will be inner joined
	join {{ ref('h_product') }} hPrd
		on hPrd.hk_product = dts.hk_product
	-- pct may have existed before prd: inner join removes these records
	join {{ ref('vs_product') }} sPrd
		on  sPrd.hk_product = hPrd.hk_product
		and dts.load_ts between sPrd.load_ts and sPrd.loadend_ts
	join {{ ref('l_product_productcategory') }} lPrdPct
		on lPrdPct.hk_product = hPrd.hk_product
	join {{ ref('vs_product_productcategory') }} sPrdPct
		on  sPrdPct.hk_product_productcategory = lPrdPct.hk_product_productcategory
		and dts.load_ts between sPrdPct.load_ts and sPrdPct.loadend_ts
	join {{ ref('h_productcategory') }} hPct
		on hPct.hk_productcategory = lPrdPct.hk_productcategory
	-- optional hub's satellites must be outer-joined, since the entity may not have existed
	-- at a given point in time, eg. deleted
	-- (remember hub is timeless, ghost-keys will be used)
	left outer join {{ ref('vs_productcategory') }} sPct
		on  sPct.hk_productcategory = hPct.hk_productcategory
		and dts.load_ts between sPct.load_ts and sPct.loadend_ts
	--order by dts.load_ts
),
periods_merged as (
	-- merge consecutive values into one period (compress)
	select
		grp.hk_product,
		grp.product_bk,
		min(grp.load_ts) as load_ts,
		max(grp.loadend_ts) as loadend_ts,
		grp.sprd_load_ts,
		grp.hk_product_productcategory,
		grp.sprdpct_load_ts,
		grp.hk_productcategory,
		grp.productcategory_bk,
		grp.spct_load_ts
	from (
		-- count occurances of values and arrange them in groups
		select
			ts.*,
			-- business key - business key/values
			row_number() over(partition by ts.hk_product order by ts.load_ts)
				- row_number() over(
					partition by
						ts.hk_product,
						ts.product_bk,
						ts.sprd_load_ts,
						ts.hk_product_productcategory,
						ts.sprdpct_load_ts,
						ts.hk_productcategory,
						ts.productcategory_bk,
						ts.spct_load_ts
					order by
						ts.load_ts
				) as grp
		from periods_data ts
	) grp
	group by
		grp.hk_product,
		grp.product_bk,
		grp.sprd_load_ts,
		grp.hk_product_productcategory,
		grp.sprdpct_load_ts,
		grp.hk_productcategory,
		grp.productcategory_bk,
		grp.spct_load_ts,
		grp.grp
)
select
	decode(md5(concat_ws('||'::text,
		COALESCE(NULLIF(TRIM(BOTH FROM t.product_bk::character varying), ''::text), '^^'::text),
		COALESCE(NULLIF(TRIM(BOTH FROM t.load_ts::character varying), ''::text), '^^'::text)
	)), 'hex'::text) as hk_product_snapshot,
	t.*
from periods_merged t
order by
	t.hk_product,
	t.load_ts