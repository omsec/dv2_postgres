-- View containing Business Rules
select
	t.hk_product_snapshot,
	t.hk_product,
	t.product_bk,
	t.load_ts,
	t.loadend_ts,
	t.prd_name,
	-- apply business rules
	coalesce(t.prd_standard_cost, 0) as prd_standard_cost,
	coalesce(t.prd_list_price, 0) as prd_list_price,
	coalesce(t.prd_sold_until, to_date('2099-12-31', 'yyyy-mm-dd')) as prd_sold_until,
	t.txt_complexity_en,
	t.hk_productcategory,
	t.productcategory_bk,
	t.pct_name,
	t.prd_description,
	t.prd_created_at,
	t.usr_created_by,
	t.usr_created_by_login_name,
	t.prd_modified_at,
	t.usr_modified_by,
	t.usr_modified_by_login_name,
	t.prd_deleted_at,
	t.usr_deleted_by,
	t.usr_deleted_by_login_name
from {{ ref('fd_product') }} t