select
	t.*
from {{ ref('xh_productcategory') }} t
where
	current_timestamp between t.load_ts and t.loadend_ts
