with ts as (
	select
		t.hk_customer_snapshot,
		t.hk_customer,
		t.cst_customer_no,
		t.load_ts,
		coalesce(
			t.cst_deleted_at,
			lead(t.cst_deleted_at) over(partition by t.hk_customer order by t.load_ts),
			lead(t.load_ts) over(partition by t.hk_customer order by t.load_ts) - interval '1 ms',
			to_timestamp('2099-12-31 23:59:59.999', 'yyyy-mm-dd hh24:mi:ss.fff')
		) as loadend_ts,
		t.cst_created_at,
		t.usr_created_by,
		t.usr_created_by_login_name,
		t.cst_modified_at,
		t.usr_modified_by,
		t.usr_modified_by_login_name,
		lead(t.cst_deleted_at) over(partition by t.hk_customer order by t.load_ts) as cst_deleted_at,
		lead(t.usr_deleted_by) over(partition by t.hk_customer order by t.load_ts) as usr_deleted_by,
		lead(t.usr_deleted_by_login_name) over(partition by t.hk_customer order by t.load_ts) as usr_deleted_by_login_name,
		t.cod_gender,
		t.txt_gender_en,
		t.cst_first_name,
		t.cst_last_name,
		t.cst_birth_date,
		t.cod_language,
		t.txt_language_en,
		t.cst_culture,
		t.cst_credit_limit,
		t.cst_remark,
		t.cst_attr1,
		t.cst_attr2,
		t.cst_attr3,
		t.cod_source,
		t.txt_source_en,
		t.cst_attr_int,
		t.cst_attr_str,
		t.cod_level,
		t.txt_level_en,
		t.dwh_applied_issues,
		t.cst_age
	from {{ ref('ax_customer') }} t
)
select *
from ts
where
	-- remove deleted records (this intentionally creates a gap)
	ts.load_ts < ts.loadend_ts
