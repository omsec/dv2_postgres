select
	t.*
from {{ ref('xh_campaign') }} t
where
	current_timestamp between t.load_ts and t.loadend_ts
