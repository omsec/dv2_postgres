select
    sat.hk_order_orderitem,
    sat.load_ts,
    sat.loadend_ts,
    -- denorm/extension of the DV2-Standard
    sat.hk_order,
    sat.hk_product,
    sat.hk_warehouse,
    sat.rowhash,
    -- payload
    sat.oit_rowid,
    sat.oit_created_at,
    sat.usr_created_by,
    coalesce(usrC.usr_login_name, sat.usr_created_by) as usr_created_by_login_name,
    sat.oit_modified_at,
    sat.usr_modified_by,
    coalesce(usrM.usr_login_name, sat.usr_modified_by) as usr_modified_by_login_name,
    sat.oit_deleted_at,
    sat.usr_deleted_by,
    coalesce(usrD.usr_login_name, sat.usr_deleted_by) as usr_deleted_by_login_name,
    sat.ord_order,
    sat.prd_product,
    sat.wrh_warehouse,
    sat.oit_quantity,
    sat.oit_unit_price,
    sat.oit_attr_int,
    sat.oit_attr_str,
    sat.oit_ord_start,
    sat.oit_ord_end,
    sat.oit_ord_effective_from
    --sat.record_source
-- link table omitted, as it's not needed
from {{ ref ('vs_order_orderitem')}} sat
-- User Look-ups (latest; references are not historized)
left outer join {{ ref('r_user') }} usrC
	on usrC.usr_rowid = sat.usr_created_by
left outer join {{ ref('r_user') }} usrM
	on usrM.usr_rowid = sat.usr_modified_by
left outer join {{ ref('r_user') }} usrD
	on usrD.usr_rowid = sat.usr_deleted_by
-- Code Look-ups (latest)
-- optional: look-up referenced BKs (hk_)