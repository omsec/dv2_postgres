select
	t.*
from {{ ref('fh_order') }} t
where
	current_timestamp between t.load_ts and t.loadend_ts
