select
	t.*,
	-- 1:0..1 link
	-- every combination just once
	-- new combination means new period/validity
	-- hence the driving key is used for grouping / building technical periods
    coalesce(lead(t.load_ts) over(partition by t.hk_product order by t.load_ts) - interval '1 ms', to_timestamp('2099-12-31 23:59:59.999', 'yyyy-mm-dd hh24:mi:ss.fff')) as loadend_ts
from {{ ref('s_product_productcategory') }} t