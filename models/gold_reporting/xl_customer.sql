select
	t.*
from {{ ref('xh_customer') }} t
where
	current_timestamp between t.load_ts and t.loadend_ts
