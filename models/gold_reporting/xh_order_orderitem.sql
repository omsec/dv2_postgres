select
	t.*
from {{ ref('ch_order_orderitem') }} t
