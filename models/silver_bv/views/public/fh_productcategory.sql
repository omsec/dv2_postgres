with ts as (
	select
		t.hk_productcategory,
		t.productcategory_bk,
		t.pct_created_at,
		t.usr_created_by,
		t.usr_created_by_login_name,
		t.pct_modified_at,
		t.usr_modified_by,
		t.usr_modified_by_login_name,
		lead(t.pct_deleted_at) over(partition by t.hk_productcategory order by t.load_ts) as pct_deleted_at,
		lead(t.usr_deleted_by) over(partition by t.hk_productcategory order by t.load_ts) as usr_deleted_by,
		lead(t.usr_deleted_by_login_name) over(partition by t.hk_productcategory order by t.load_ts) as usr_deleted_by_login_name,
		t.pct_name,
		t.pct_import_filename,
		t.load_ts,
		coalesce(
			t.pct_deleted_at,
			lead(t.pct_deleted_at) over(partition by t.hk_productcategory order by t.load_ts),
			lead(t.load_ts) over(partition by t.hk_productcategory order by t.load_ts) - interval '1 ms',
			to_timestamp('2099-12-31 23:59:59.999', 'yyyy-mm-dd hh24:mi:ss.fff')
		) as loadend_ts
	from {{ ref('fd_productcategory') }} t
)
select *
from ts
where
	-- remove deleted records (this intentionally creates a gap)
	ts.load_ts < ts.loadend_ts
