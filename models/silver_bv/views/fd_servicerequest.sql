-- *** GENERATED CODE *** >> update COG_CodeGroups below if necessary
select
	hub.hk_servicerequest,
	hub.srv_request_no,
	sat.srv_created_at,
	sat.usr_created_by,
	coalesce(usrC.usr_login_name, sat.usr_created_by) as usr_created_by_login_name,
	sat.srv_modified_at,
	sat.usr_modified_by,
	coalesce(usrM.usr_login_name, sat.usr_modified_by) as usr_modified_by_login_name,
	sat.srv_deleted_at,
	sat.usr_deleted_by,
	coalesce(usrD.usr_login_name, sat.usr_deleted_by) as usr_deleted_by_login_name,
	sat.srv_subject,
	sat.srv_message,
	sat.cod_priority,
	coalesce(cdPriorityEN.cod_text, sat.cod_priority) as txt_priority_en,
	sat.cod_status,
	coalesce(cdStatusEN.cod_text, sat.cod_status) as txt_status_en,
	sat.srv_attr_int,
	sat.srv_attr_str,
	sat.load_ts,
	sat.loadend_ts
from {{ ref('h_servicerequest') }} hub
join {{ ref('vs_servicerequest') }} sat
	on sat.hk_servicerequest = hub.hk_servicerequest
-- User Look-ups (latest; references are not historized)
left outer join {{ ref('r_user') }} usrC
	on usrC.usr_rowid = sat.usr_created_by
left outer join {{ ref('r_user') }} usrM
	on usrM.usr_rowid = sat.usr_modified_by
left outer join {{ ref('r_user') }} usrD
	on usrD.usr_rowid = sat.usr_deleted_by
-- Code Look-ups (latest)
left outer join {{ ref('vls_codedefinition')}} cdPriorityEN
	on  cdPriorityEN.cog_group = 7
	and cdPriorityEN.cod_value = sat.cod_priority
	and cdPriorityEN.cod_language = 10
left outer join {{ ref('vls_codedefinition')}} cdStatusEN
	on  cdStatusEN.cog_group = 8
	and cdStatusEN.cod_value = sat.cod_status
	and cdStatusEN.cod_language = 10
