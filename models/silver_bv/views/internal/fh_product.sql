with ts as (
	select
		t.hk_product_snapshot,
		t.hk_product,
		t.product_bk,
		t.load_ts,
		coalesce(
			t.prd_deleted_at,
			lead(t.prd_deleted_at) over(partition by t.hk_product order by t.load_ts),
			lead(t.load_ts) over(partition by t.hk_product order by t.load_ts) - interval '1 ms',
			to_timestamp('2099-12-31 23:59:59.999', 'yyyy-mm-dd hh24:mi:ss.fff')
		) as loadend_ts,
		t.prd_name,
		t.prd_standard_cost,
		t.prd_list_price,
		t.prd_sold_until,
		t.txt_complexity_en,
		t.hk_productcategory,
		t.productcategory_bk,
		t.pct_name,
		t.prd_description,
		t.prd_created_at,
		t.usr_created_by,
		t.usr_created_by_login_name,
		t.prd_modified_at,
		t.usr_modified_by,
		t.usr_modified_by_login_name,
		lead(t.prd_deleted_at) over(partition by t.hk_product order by t.load_ts) as prd_deleted_at,
		lead(t.usr_deleted_by) over(partition by t.hk_product order by t.load_ts) as usr_deleted_by,
		lead(t.usr_deleted_by_login_name) over(partition by t.hk_product order by t.load_ts) as usr_deleted_by_login_name
	from {{ ref('br_product') }} t
)
select *
from ts
where
	-- remove deleted records (this intentionally creates a gap)
	ts.load_ts < ts.loadend_ts