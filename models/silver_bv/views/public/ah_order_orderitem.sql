select
	t.*
from {{ ref('fh_order_orderitem') }} t
where
	current_timestamp between t.load_ts and t.loadend_ts
