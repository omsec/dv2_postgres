select
    t.*
    -- could read vls as well, if no access rules required
from {{ ref('ah_order_customer_salesman') }} t
where
	current_timestamp between t.load_ts and t.loadend_ts