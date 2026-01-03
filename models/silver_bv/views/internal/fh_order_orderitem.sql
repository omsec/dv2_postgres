{{
    config(
        materialized='incremental',
        unique_key=['hk_order_orderitem']
    )
}}


with ts as (
	select
        t.hk_order_orderitem,
        t.load_ts,
        t.loadend_ts,
        -- denorm/extension of the DV2-Standard
        t.hk_order,
        t.hk_product,
        t.hk_warehouse,
        t.rowhash,
        -- payload
        t.oit_rowid,
        t.oit_created_at,
        t.usr_created_by,
        t.usr_created_by_login_name,
        t.oit_modified_at,
        t.usr_modified_by,
        t.usr_modified_by_login_name,
        lead(t.oit_deleted_at) over(partition by t.hk_order_orderitem order by t.load_ts) as oit_deleted_at,
		lead(t.usr_deleted_by) over(partition by t.hk_order_orderitem order by t.load_ts) as usr_deleted_by,
		lead(t.usr_deleted_by_login_name) over(partition by t.hk_order_orderitem order by t.load_ts) as usr_deleted_by_login_name,
        t.ord_order,
        t.prd_product,
        t.wrh_warehouse,
        t.oit_quantity,
        t.oit_unit_price,
        t.oit_attr_int,
        t.oit_attr_str,
        t.oit_ord_start,
        t.oit_ord_end,
        t.oit_ord_effective_from
        --t.record_source
	from {{ ref('br_order_orderitem') }} t
)
select *
from ts
where
	-- remove deleted records (this intentionally creates a gap)
	ts.load_ts < ts.loadend_ts
