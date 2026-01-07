-- *** GENERATED CODE *** >> update COG_CodeGroups below if necessary
select
	hub.hk_address,
	hub.adr_rowid,
	sat.rowhash,
	sat.adr_created_at,
	sat.usr_created_by,
	coalesce(usrC.usr_login_name, sat.usr_created_by) as usr_created_by_login_name,
	sat.adr_modified_at,
	sat.usr_modified_by,
	coalesce(usrM.usr_login_name, sat.usr_modified_by) as usr_modified_by_login_name,
	sat.adr_deleted_at,
	sat.usr_deleted_by,
	coalesce(usrD.usr_login_name, sat.usr_deleted_by) as usr_deleted_by_login_name,
	sat.cod_type,
	coalesce(cdTypeEN.cod_text, sat.cod_type) as txt_type_en,
	sat.adr_priority,
	sat.adr_address,
	sat.adr_primary,
	sat.load_ts,
	sat.loadend_ts
from {{ ref('h_address') }} hub
join {{ ref('vs_address') }} sat
	on sat.hk_address = hub.hk_address
-- User Look-ups (latest; references are not historized)
left outer join {{ ref('r_user') }} usrC
	on usrC.usr_rowid = sat.usr_created_by
left outer join {{ ref('r_user') }} usrM
	on usrM.usr_rowid = sat.usr_modified_by
left outer join {{ ref('r_user') }} usrD
	on usrD.usr_rowid = sat.usr_deleted_by
-- Code Look-ups (latest)
left outer join {{ ref('vls_codedefinition')}} cdTypeEN
	on  cdTypeEN.cog_group = 4
	and cdTypeEN.cod_value = sat.cod_type
	and cdTypeEN.cod_language = 10
