select
	t.*
from {{ ref('ch_order') }} t
