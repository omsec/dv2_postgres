select
	t.*
from {{ ref('fh_order_orderitem') }} t
