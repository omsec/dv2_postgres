select
	bPrd.hk_product_snapshot as d_product,
	bPrd.hk_product,
	bPrd.product_bk,
	bPrd.load_ts,
	bPrd.loadend_ts,
	sPrd.prd_name,
	sPrd.prd_standard_cost,
	sPrd.prd_list_price,
	sPrd.prd_sold_until,
	--sPrd.txt_complexity_en,
	bPrd.hk_productcategory,
	bPrd.productcategory_bk,
	sPct.pct_name,
	sPrd.prd_description,
	sPrd.prd_created_at,
	sPrd.usr_created_by,
    coalesce(usrC.usr_login_name, sPrd.usr_created_by) as usr_created_by_login_name,
	sPrd.prd_modified_at,
	sPrd.usr_modified_by,
	coalesce(usrM.usr_login_name, sPrd.usr_modified_by) as usr_modified_by_login_name,
	sPrd.prd_deleted_at,
	sPrd.usr_deleted_by,
	coalesce(usrD.usr_login_name, sPrd.usr_deleted_by) as usr_deleted_by_login_name
from {{ ref('b_product') }} bPrd
join {{ ref('v_product') }} sPrd
	on  sPrd.hk_product = bPrd.hk_product
	and sPrd.load_ts = bPrd.sprd_load_ts
left outer join {{ ref('v_productcategory') }} sPct
	on  sPct.hk_productcategory = bPrd.hk_productcategory
	and sPct.load_ts = bPrd.spct_load_ts
-- User Look-ups (latest; references are not historized)
left outer join {{ ref('r_user') }} usrC
	on usrC.usr_rowid = sPrd.usr_created_by
left outer join {{ ref('r_user') }} usrM
	on usrM.usr_rowid = sPrd.usr_modified_by
left outer join {{ ref('r_user') }} usrD
	on usrD.usr_rowid = sPrd.usr_deleted_by