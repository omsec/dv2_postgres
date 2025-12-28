with ts as (
	select
		t.hk_product,
		t.prd_created_at,
		t.usr_created_by,
		t.prd_modified_at,
		t.usr_modified_by,
		lead(t.prd_deleted_at) over(partition by t.hk_product order by t.load_ts) as prd_deleted_at,
		lead(t.usr_deleted_by) over(partition by t.hk_product order by t.load_ts) as usr_deleted_by,
		t.prd_name,
		t.prd_description,
		t.prd_standard_cost,
		t.prd_list_price,
		t.prd_sold_until,
		t.load_ts,
		coalesce(
			t.prd_deleted_at,
			lead(t.prd_deleted_at) over(partition by t.hk_product order by t.load_ts),
			lead(t.load_ts) over(partition by t.hk_product order by t.load_ts) - interval '1 ms',
			to_timestamp('2099-12-31 23:59:59.999', 'yyyy-mm-dd hh24:mi:ss.fff')
		) as loadend_ts
	from {{ ref('br_product') }} t
)
select *
from ts
where
	-- remove deleted records (this intentionally creates a gap)
	ts.load_ts < ts.loadend_ts
