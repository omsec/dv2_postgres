select
	t.*
from {{ ref('ch_product') }} t
