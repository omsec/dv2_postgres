-- SCD1: Only apply the latest version (overwrite changes)
-- https://ijaniszewski.medium.com/data-engineering-understanding-slowly-changing-dimensions-scd-with-dbt-98b37f3ddf03

-- Since it's so easy, all dimensions are SCD2 (materialized=incremental) - except for the dim_dates

-- ToDo:
-- SCD1-Demo: read current version of b_product only, then apply incremental or table

{{
    config(
        materialized='incremental',
        unique_key='d_product'
    )
}}

select
	t.hk_product_snapshot as d_product,
	t.hk_product,
	t.product_bk,
	t.load_ts,
	t.loadend_ts,
	t.prd_name,
	t.prd_standard_cost,
	t.prd_list_price,
	t.prd_sold_until,
	--t.txt_complexity_en
	t.hk_productcategory,
	t.productcategory_bk,
	t.pct_name
from{{ ref('ch_product') }} t
order by
	t.load_ts,
	cast(t.product_bk as int)