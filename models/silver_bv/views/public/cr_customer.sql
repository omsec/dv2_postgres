select
	t.*
from {{ ref('fh_customer') }} t
where
	current_timestamp between t.load_ts and t.loadend_ts
