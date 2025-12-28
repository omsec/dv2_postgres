-- *** GENERATED CODE *** >> update COG_CodeGroups below if necessary
select
	hub.hk_employee,
	hub.emp_employee_no,
	sat.rowhash,
	sat.emp_rowid,
	sat.emp_created_at,
	sat.usr_created_by,
	coalesce(usrC.usr_login_name, sat.usr_created_by) as usr_created_by_login_name,
	sat.emp_modified_at,
	sat.usr_modified_by,
	coalesce(usrM.usr_login_name, sat.usr_modified_by) as usr_modified_by_login_name,
	sat.emp_deleted_at,
	sat.usr_deleted_by,
	coalesce(usrD.usr_login_name, sat.usr_deleted_by) as usr_deleted_by_login_name,
	sat.emp_first_name,
	sat.emp_last_name,
	sat.emp_email,
	sat.emp_phone,
	sat.emp_birth_date,
	sat.emp_hire_date,
	sat.emp_job_title,
	sat.usr_account_id,
	sat.load_ts,
	sat.loadend_ts
from {{ ref('h_employee') }} hub
join {{ ref('v_employee') }} sat
	on sat.hk_employee = hub.hk_employee
-- User Look-ups (latest; references are not historized)
left outer join {{ ref('r_user') }} usrC
	on usrC.usr_rowid = sat.usr_created_by
left outer join {{ ref('r_user') }} usrM
	on usrM.usr_rowid = sat.usr_modified_by
left outer join {{ ref('r_user') }} usrD
	on usrD.usr_rowid = sat.usr_deleted_by
