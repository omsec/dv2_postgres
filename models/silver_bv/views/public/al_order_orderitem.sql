select
	t.*
from {{ ref('ah_order_orderitem') }} t
where
	current_timestamp between t.load_ts and t.loadend_ts
