select
	t.*
from {{ ref('ah_productcategory') }} t
where
	current_timestamp between t.load_ts and t.loadend_ts
