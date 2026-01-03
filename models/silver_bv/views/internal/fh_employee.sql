with ts as (
	select
		t.hk_employee,
		t.emp_employee_no,
		t.rowhash,
		t.emp_rowid,
		t.emp_created_at,
		t.usr_created_by,
		t.usr_created_by_login_name,
		t.emp_modified_at,
		t.usr_modified_by,
		t.usr_modified_by_login_name,
		lead(t.emp_deleted_at) over(partition by t.hk_employee order by t.load_ts) as emp_deleted_at,
		lead(t.usr_deleted_by) over(partition by t.hk_employee order by t.load_ts) as usr_deleted_by,
		lead(t.usr_deleted_by_login_name) over(partition by t.hk_employee order by t.load_ts) as usr_deleted_by_login_name,
		t.emp_first_name,
		t.emp_last_name,
		t.emp_email,
		t.emp_phone,
		t.emp_birth_date,
		t.emp_hire_date,
		t.emp_job_title,
		t.usr_account_id,
		t.load_ts,
		coalesce(
			t.emp_deleted_at,
			lead(t.emp_deleted_at) over(partition by t.hk_employee order by t.load_ts),
			lead(t.load_ts) over(partition by t.hk_employee order by t.load_ts) - interval '1 ms',
			to_timestamp('2099-12-31 23:59:59.999', 'yyyy-mm-dd hh24:mi:ss.fff')
		) as loadend_ts
	from {{ ref('fd_employee') }} t
)
select *
from ts
where
	-- remove deleted records (this intentionally creates a gap)
	ts.load_ts < ts.loadend_ts
