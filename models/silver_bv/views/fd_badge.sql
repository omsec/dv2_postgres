select
	hub.hk_badge,
	hub.bdg_badge_bk,
	sat.rowhash,
	sat.bdg_rowid,
	sat.bdg_created_at,
	sat.usr_created_by,
	coalesce(usrC.usr_login_name, sat.usr_created_by) as usr_created_by_login_name,
	sat.bdg_modified_at,
	sat.usr_modified_by,
	coalesce(usrM.usr_login_name, sat.usr_modified_by) as usr_modified_by_login_name,
	sat.bdg_deleted_at,
	sat.usr_deleted_by,
	coalesce(usrD.usr_login_name, sat.usr_deleted_by) as usr_deleted_by_login_name,
	sat.bdg_name,
	sat.bdg_type1,
	sat.bdg_type2,
	sat.bdg_abilities,
	sat.bdg_generation,
	sat.load_ts,
	sat.loadend_ts
from {{ ref('h_badge') }} hub
join {{ ref('vs_badge') }} sat
	on sat.hk_badge = hub.hk_badge
-- User Look-ups (latest; references are not historized)
left outer join {{ ref('r_user') }} usrC
	on usrC.usr_rowid = sat.usr_created_by
left outer join {{ ref('r_user') }} usrM
	on usrM.usr_rowid = sat.usr_modified_by
left outer join {{ ref('r_user') }} usrD
	on usrD.usr_rowid = sat.usr_deleted_by
