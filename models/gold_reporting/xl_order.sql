select
	t.*
from {{ ref('xh_order') }} t
where
	current_timestamp between t.load_ts and t.loadend_ts
