select
	hub.hk_order,
	hub.ord_order_no,
	sat.ord_created_at,
	sat.usr_created_by,
	coalesce(usrC.usr_login_name, sat.usr_created_by) as usr_created_by_login_name,
	sat.ord_modified_at,
	sat.usr_modified_by,
	coalesce(usrM.usr_login_name, sat.usr_modified_by) as usr_modified_by_login_name,
	sat.ord_deleted_at,
	sat.usr_deleted_by,
	coalesce(usrD.usr_login_name, sat.usr_deleted_by) as usr_deleted_by_login_name,
	sat.cod_status,
	coalesce(cdStatusEN.cod_text, sat.cod_status) as txt_status_en,
	sat.ord_sale_ts,
	sat.ord_attr_int,
	sat.ord_attr_str,
	sat.load_ts,
	sat.loadend_ts
from {{ ref('h_order') }} hub
join {{ ref('vs_order') }} sat
	on sat.hk_order = hub.hk_order
-- User Look-ups (latest; references are not historized)
left outer join {{ ref('r_user') }} usrC
	on usrC.usr_rowid = sat.usr_created_by
left outer join {{ ref('r_user') }} usrM
	on usrM.usr_rowid = sat.usr_modified_by
left outer join {{ ref('r_user') }} usrD
	on usrD.usr_rowid = sat.usr_deleted_by
-- Code Look-ups (latest)
left outer join {{ ref('vs_codedefinition')}} cdStatusEN
	on  cdStatusEN.cog_group = 5
	and cdStatusEN.cod_value = sat.cod_status
	and cdStatusEN.cod_language = 10
	and cdStatusEN.loadend_ts = to_timestamp('2099-12-31 23:59:59.999', 'yyyy-mm-dd hh24:mi:ss.fff')
